package janus

import akka.dispatch.{ExecutionContext, Future}
import java.sql.Connection
import javax.sql.DataSource
import org.slf4j.LoggerFactory

import CloseableResource.withResource

object Database {
  def apply(ds: DataSource) = new JdbcDatabase(ds)

  //TODO: add other creation methods here - perhaps from URL or DriverManager
  val logger = LoggerFactory.getLogger(classOf[Database])
}

trait Database {

  import Database._

  def createSession: Session

  def withSession[T](f: Session => T) : T = {
    logger.debug("Creating new session.")
    withResource(createSession)(f)
  }

  /**
   * Shorthand for wrapping a Session in a Future.
   */
  def withDetachedSession[T](f: Session => T)(implicit ec: ExecutionContext): Future[T] = {
    logger.debug("Creating new detached session.")
    Future {
      withSession(f)
    }
  }

}

sealed class JdbcDatabase(ds: DataSource) extends Database {

  def createConnection(): Connection = ds.getConnection

  /**
   * Create a new Session. Note - this Session *must* be closed by the caller when completed.
   * @return
   */
  def createSession: Session = {
    new JdbcSession(this)
  }
}

trait Session extends CloseableResource {

  protected def database: Database

  /**
   * Close this session
   */
  def close()

  //TODO - figure out a way to handle transaction propagation
  /**
   * Run the supplied function within a transaction. If the function throws an Exception
   * or the session's rollback() method is called, the transaction is rolled back,
   * otherwise it is commited when the function returns.
   *
   * If there is an already active transaction, a new transaction is not created. Functionally,
   * this method is a no-op in the presence of an existing transaction.
   */
  def withTransaction[T](f: Transaction => T): T

  def executeSql(query: String) = {
    withStatement { stmt =>
      stmt.execute(query)
    }
  }

  def executeQuery[T](query: String)(f: ResultRow => T): Traversable[T] = {
    withStatement { stmt =>
      stmt.executeQuery(query)(_.map(f))
    }
  }

  def withPreparedStatement[T](query: String)(f: PreparedStatement => T): T = {
    withResource(prepareStatement(query))(f)
  }

  def withStatement[T](f: Statement => T): T = {
    withResource(statement())(f)
  }

  /**
   * Generate a PreparedStatement. NOTE - the caller of this method is responsible for closing the returned
   * PreparedStatement (when available).
   */
  def prepareStatement(query: String): PreparedStatement

  def statement(): Statement
}

class JdbcSession(val database: JdbcDatabase) extends Session {

  import JdbcSession._

  lazy val conn = database.createConnection()

  protected var inTransaction = false

  /**
   * Close this session
   */
  def close() {
    log.debug("Closing session/connection.")
    conn.close()
  }
  def failureHandler(e: Throwable) {
    log.error("Closing JdbcSession due to exception.", e)
  }

  /**
   * Run the supplied function within a transaction. If the function throws an Exception
   * or the session's rollback() method is called, the transaction is rolled back,
   * otherwise it is commited when the function returns.
   *
   * If there is an already active transaction, a new transaction is not created. Functionally,
   * this method is a no-op in the presence of an existing transaction.
   */
  def withTransaction[A](f: Transaction => A): A = {
    if(inTransaction) {
      log.debug("No need to start new transaction - already in one.")
      //if we're already in a transaction, don't need to do anything
      withResource(new NestedTransaction)(f)
    } else {
      log.debug("Starting new transaction.")
      //turn on transactional behavior on the connection
      conn.setAutoCommit(false)
      inTransaction = true
      withResource(new JdbcTransaction(conn))(f)
    }
  }

  /**
   * Generate a PreparedStatement. NOTE - the caller of this method is responsible for closing the returned
   * PreparedStatement (when available).
   */
  def prepareStatement(query: String): PreparedStatement  = {
    log.debug("Creating new PreparedStatement - {}", query)
    new JdbcPreparedStatement(conn.prepareStatement(query, java.sql.Statement.RETURN_GENERATED_KEYS))
  }

  def statement(): Statement = {
    log.debug("Creating new Statement")
    new JdbcStatement(conn.createStatement())
  }
}

object JdbcSession {
  val log = LoggerFactory.getLogger(classOf[JdbcSession])
}
