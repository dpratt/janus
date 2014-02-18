package janus

import javax.sql.DataSource
import org.springframework.jdbc.datasource.DataSourceUtils
import java.sql.{ResultSet, Connection}
import util.control.NonFatal
import org.springframework.transaction.{TransactionStatus, TransactionDefinition, PlatformTransactionManager}
import org.springframework.transaction.support.DefaultTransactionDefinition
import com.typesafe.scalalogging.slf4j.Logging

/**
 * A Session defines a concrete set of interactions with the database. On the highest level, they can be thought of
 */
trait Session {

  /**
   * Run the supplied function within a transaction. If the function throws an Exception
   * or the session's rollback() method is called, the transaction is rolled back,
   * otherwise it is commited when the function returns.
   *
   * If there is an already active transaction, a new transaction is not created. Functionally,
   * this method is a no-op in the presence of an existing transaction.
   */
  def withTransaction[T](f: Transaction => T): T

  def executeSql(query: String): Boolean = {
    withStatement { stmt =>
      stmt.execute(query)
    }
  }

  /**
   * A shortcut method for executing a query.
   */
  def executeQuery(query: String): Seq[Row]

  /**
   * Allocate (and automatically reclaim) a PreparedStatement. Note - you must fully use/consume any
   * resources produced by this Statement inside of the supplied handler block. For example, the various execute
   * methods on Statement produce lazy Streams of results. If you wish to return/use any of these values outside of the
   * block you *MUST* map them to non-lazy strict collections. If you do not, undefined behavior will occur.
   */
  def withPreparedStatement[T](query: String, returnGeneratedKeys: Boolean = false)(f: PreparedStatement => T): T

  /**
   * Allocate (and automatically reclaim) a Statement object. Note - you must fully use/consume any
   * resources produced by this Statement inside of the supplied handler block. For example, the various execute
   * methods on Statement produce lazy Streams of results. If you wish to return/use any of these values outside of the
   * block you *MUST* map them to non-lazy strict collections. If you do not, undefined behavior will occur.
   * @return
   */
  def withStatement[T](f: Statement => T): T

  /**
   * Allows, raw, low-level access to the JDBC connection. This is a way to 'break out' of the Janus API
   * in order to integrate to existing legacy code that requires a low-level connection.
   *
   * This Connection object is managed for you - do *NOT* call close on it. However, you are required to clean up
   * any other resources (Statements, PreparedStatements, ResultSets, etc) you allocate using this connection.
   */
  def withConnection[A](f: java.sql.Connection => A): A

  /**
   * Clean up
   */
  def close()
}

private[janus] abstract class JdbcSessionBase extends Session with Logging {


  /**
   * A shortcut method for executing a query.
   */
  def executeQuery(query: String): Seq[Row] = {
    withStatement { stmt =>
      stmt.executeQuery(query)
    }
  }

  def withPreparedStatement[T](query: String, returnGeneratedKeys: Boolean = false)(f: (PreparedStatement) => T): T = {
    withConnection { c =>
      val ps = createPreparedStatement(c, query, returnGeneratedKeys)
      try {
        f(new JdbcPreparedStatement(ps))
      } finally {
        ps.close()
      }
    }
  }

  def withStatement[T](f: Statement => T): T ={
    withConnection { c =>
      val stmt = createStatment(c)
      try {
        f(new JdbcStatement(stmt))
      } finally {
        stmt.close()
      }
    }
  }

  protected def applyStatementSettings(stmt: java.sql.Statement): java.sql.Statement

  private def createPreparedStatement(c: Connection, query: String, returnGeneratedKeys: Boolean = false): java.sql.PreparedStatement = {
    logger.debug("Creating new PreparedStatement - {}", query)

    val stmt = if (returnGeneratedKeys) {
      c.prepareStatement(query, java.sql.Statement.RETURN_GENERATED_KEYS)
    } else {
      c.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    }
    applyStatementSettings(stmt)
    stmt
  }

  private def createStatment(c: Connection): java.sql.Statement = {
    applyStatementSettings(c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY))
  }

}

//used when we do not have access to a PlatformTransactionManager
//this class manages it's own transactions
//NOTE - This class will completely and utterly mess up an existing
//Spring managed transaction. DO NOT USE THIS CLASS IF YOUR DATASOURCE IS SYNCHRONIZED
//WITH SPRING. You have been warned.
private[janus] class SimpleSession(conn: Connection) extends JdbcSessionBase with Logging {

  private var currentTransaction: JdbcTransaction = null
  private var resetAutoCommit = false

  /**
   * Clean up
   */
  def close() {
    conn.close()
  }

  /**
   * Run the supplied function within a transaction. If the function throws an Exception
   * or the session's rollback() method is called, the transaction is rolled back,
   * otherwise it is commited when the function returns.
   *
   * If there is an already active transaction, a new transaction is not created. Functionally,
   * this method is a no-op in the presence of an existing transaction.
   *
   */
  def withTransaction[A](f: Transaction => A): A = {

    if (currentTransaction != null) {
      logger.debug("No need to start new transaction - already in one.")
      //if we're already in a transaction, don't need to do anything
      f(new NestedTransaction(currentTransaction))
    } else {
      logger.debug("Starting new transaction.")
      //turn on transactional behavior on the connection
      if (conn.getAutoCommit) {
        //only flip autocommit if we have to
        //it has the potential to be expensive
        resetAutoCommit = true
      }
      conn.setAutoCommit(false)
      currentTransaction = new JdbcTransaction
      try {
        f(currentTransaction)
      } catch {
        case NonFatal(e) =>
          logger.debug("Error in transaction. Setting rollback.")
          currentTransaction.setRollback()
          throw e
      } finally {
        if (currentTransaction.shouldRollback) {
          conn.rollback()
        } else {
          conn.commit()
        }
        if (resetAutoCommit) conn.setAutoCommit(true)
        currentTransaction = null
      }
    }
  }

  //this is a no-op for simple sessions
  def withConnection[A](f: Connection => A): A = f(conn)

  protected def applyStatementSettings(stmt: java.sql.Statement): java.sql.Statement = stmt

}

private[janus] class SpringSession(ds: DataSource, txManager: PlatformTransactionManager) extends JdbcSessionBase with Logging {

  /**
   * Clean up
   */
  def close() {
    //a no-op for spring sessions
  }

  /**
   * Use Spring to manage our transactions.
   */
  def withTransaction[T](f: (Transaction) => T): T = {

    //TODO - figure out a way to parametrize propagation and isolation
    val transactionDef = new DefaultTransactionDefinition()
    transactionDef.setName("janusTransaction")
    transactionDef.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED)

    val transaction = txManager.getTransaction(transactionDef)
    val returnVal = try {
      f(new SpringTransaction(transaction))
    } catch {
      case NonFatal(e) => doRollback(transaction, e); throw e
    }
    txManager.commit(transaction)
    returnVal
  }

  private def doRollback(tx: TransactionStatus, t: Throwable) {
    try {
      txManager.rollback(tx)
    } catch {
      case NonFatal(e) =>
        logger.error("Exception during rollback masked real exception.", t)
        throw e
    }
  }

  def withConnection[A](f: Connection => A): A = {
    val c = DataSourceUtils.getConnection(ds)
    try {
      f(c)
    } finally {
      DataSourceUtils.releaseConnection(c, ds)
    }
  }

  protected def applyStatementSettings(stmt: java.sql.Statement): java.sql.Statement = {
    //for now, just set transaction timeout (if available)
    DataSourceUtils.applyTransactionTimeout(stmt, ds)
    stmt
  }

}
