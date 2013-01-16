package janus

import java.sql.Connection
import javax.sql.DataSource
import org.slf4j.LoggerFactory

import CloseableResource.withResource
import scala.concurrent.{ExecutionContext, Future}

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

private[janus] class JdbcDatabase(ds: DataSource) extends Database {

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

  /**
   * A shortcut method for executing a query. Note that this method differs from the alternate
   * ways of getting a Stream[Row], in that it explicitly forces the stream to load all of the
   * results in the underlying query. If your query returns a lot of rows, this can potentially use
   * quite a bit of memory. If your usecase requires a large result set, use the withStatement method
   * to create a statement that will return a lazy stream.
   */
  def executeQuery(query: String): Stream[Row]

  /**
   * Allocate (and automatically reclaim) a PreparedStatement. Note - you must fully use/consume any
   * resources produced by this Statement inside of the supplied handler block. For example, the various execute
   * methods on Statement produce lazy Streams of results. If you wish to return/use any of these values outside of the
   * block you *MUST* map them to non-lazy strict collections. If you do not, undefined behavior will occur.
   */
  def withPreparedStatement[T](query: String, returnGeneratedKeys: Boolean = false)(f: PreparedStatement => T): T = {
    withResource(prepareStatement(query, returnGeneratedKeys))(f)
  }

  /**
   * Allocate (and automatically reclaim) a Statement object. Note - you must fully use/consume any
   * resources produced by this Statement inside of the supplied handler block. For example, the various execute
   * methods on Statement produce lazy Streams of results. If you wish to return/use any of these values outside of the
   * block you *MUST* map them to non-lazy strict collections. If you do not, undefined behavior will occur.
   * @return
   */
  def withStatement[T](f: Statement => T): T = {
    withResource(statement())(f)
  }

  /**
   * Generate a PreparedStatement. NOTE - the caller of this method is responsible for closing the returned
   * PreparedStatement.
   */
  def prepareStatement(query: String, returnGeneratedKeys: Boolean = false): PreparedStatement

  def statement(): Statement
}

private[janus] class JdbcSession(val database: JdbcDatabase) extends Session {

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
    log.debug("Closing JdbcSession due to exception.")
    close()
  }


  def executeQuery(query: String): Stream[Row] = {
    //a shortcut - we don't create a janus statement here, but just directly
    //create the JDBC statement, and assume that the resulting Stream will close it
    val stmt = conn.createStatement()
    //explicitly call 'force' on the stream so that the Statement is closed.
    //TODO - this potentially uses a lot of memory, but it's the only way to ensure that the
    //statement is closed.
    JdbcRow.convertResultSet(stmt.executeQuery(query), closeStatementWhenComplete = true).force
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
  def prepareStatement(query: String, returnGeneratedKeys: Boolean = false): PreparedStatement  = {
    log.debug("Creating new PreparedStatement - {}", query)
    if (returnGeneratedKeys) {
      new JdbcPreparedStatement(conn.prepareStatement(query, java.sql.Statement.RETURN_GENERATED_KEYS))
    } else {
      new JdbcPreparedStatement(conn.prepareStatement(query))
    }
  }

  def statement(): Statement = {
    log.debug("Creating new Statement")
    new JdbcStatement(conn.createStatement())
  }
}

object JdbcSession {
  val log = LoggerFactory.getLogger(classOf[JdbcSession])
}
