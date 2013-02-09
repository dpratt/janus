package janus

import javax.sql.DataSource
import org.slf4j.LoggerFactory

import scala.concurrent._
import org.springframework.transaction.PlatformTransactionManager

object Database {
  /**
   * Create a new Database. Sessions created by this database will manage their own transaction semantics.
   *
   * Note - if this datasource is synchronized to a Spring PlatformTransactionManager, usage of a Database returned from
   * this method will interfere with and likely cause undefined behavior with Spring managed transactions.
   */
  def apply(ds: DataSource) = new JdbcDatabase(ds)

  /**
   * Create a new Database that will create and participate in Spring managed transactions.
   *
   * Note - the supplied DataSource *MUST* be synchronized by the supplied PlatformTransactionManager,
   * or undefined (and likely destructive) behavior will occur.
   */
  def apply(ds: DataSource, txManager: PlatformTransactionManager) = new SpringDatabase(ds, txManager)

  private val logger = LoggerFactory.getLogger(classOf[Database])
}

trait Database {

  import Database._

  def withSession[T](f: Session => T) : T = {
    logger.debug("Creating new session.")
    val session = createSession
    try {
      f(session)
    } finally {
      session.close()
    }
  }

  /**
   * Shorthand for wrapping a Session in a Future. This will isolate database operations to their own, private
   * threadpool. Suitable for using when access is required from a context which would not normally handle blocking calls well.
   *
   * Note - this will break any active transaction semantics you may have currently, so ensure that this method
   * 1) Only gets executed from a context in which blocking would degrade performance *AND*
   * 2) You do not have the requirement to participate in a possibly already present transaction.
   */
  def withDetachedSession[T](f: Session => T): Future[T] = {
    logger.debug("Creating new detached session.")
    future {
      blocking {
        withSession(f)
      }
    }
  }

  protected def createSession: Session

}

private[janus] class JdbcDatabase(ds: DataSource) extends Database {
  def createSession: Session = new SimpleSession(ds)
}

private[janus] class SpringDatabase(ds: DataSource, txManager: PlatformTransactionManager) extends Database {
  def createSession: Session = new SpringSession(ds, txManager)
}
