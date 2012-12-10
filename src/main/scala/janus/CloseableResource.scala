package janus

import akka.dispatch.Future
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Connection

/**
 * Indicates that this class implements a disposable resource.
 *
 * @author David Pratt (dpratt@vast.com)
 */
trait CloseableResource {
  def close()
  def failureHandler(e: Throwable)
}

object CloseableResource {

  private val logger = LoggerFactory.getLogger(classOf[CloseableResource])

  def withResource[A <: CloseableResource, B](resource: A)(f: A => B) : B = {
    val result = try {
      f(resource)
    } catch {
      case e: Throwable => {
        logger.error("Error wrapping resource. Cleaning up.", e)
        //something went wrong constructing the value
        //clean up and re-throw
        resource.failureHandler(e)
        throw e
      }
    }

    //okay, here's where it gets interesting.
    //If the return type T is a Future, we can't clean up yet. We have to wait until
    //the Future itself completes. If it isn't a Future, we can clean up right now
    //Note - this could be done with Manifests, but we don't need deep type inspection here
    val futureClass = classOf[Future[_]]
    if (!futureClass.isInstance(result)) {
      logger.debug("Cleaning up for result type {}", result.getClass.toString)
      //we have a regular value
      //the assumption here is that we can just close the transaction
      //otherwise, we would have rolled back and thrown the cause above
      resource.close()
    } else {
      logger.debug("Cleaning up async result type {}", result.getClass.toString)
      val resultFuture = result.asInstanceOf[Future[_]]
      resultFuture.onComplete {
        case Right(_) => resource.close()
        case Left(e) => resource.failureHandler(e)
      }
    }
    result
  }

}

/**
 * Represents a transaction's state.
 */
trait Transaction extends CloseableResource {

  /**
   * Call this method to roll back the current transaction after it's resource is closed.
   */
  def rollback()
}


sealed class JdbcTransaction(c: Connection) extends Transaction {

  import JdbcTransaction._

  protected var shouldRollback = false

  def close() {
    log.debug("Finishing transaction")
    if(!shouldRollback) {
      doCommit()
    } else {
      doRollback()
    }
  }

  def rollback() {
    shouldRollback = true
  }

  def failureHandler(e: Throwable) {
    log.debug("Failing transaction due to exception.", e)
    doRollback()
  }

  protected def doCommit() {
    log.debug("Comitting transaction.")
    c.commit()
    c.setAutoCommit(true)
    shouldRollback = false
  }

  protected def doRollback() {
    log.debug("Rolling back transaction.")
    shouldRollback = false
    c.rollback()
    c.setAutoCommit(true)
  }
}

object JdbcTransaction {
  val log = LoggerFactory.getLogger(classOf[JdbcTransaction])
}

//No support for nested transactions right now - they do nothing.
sealed class NestedTransaction extends Transaction {

  import NestedTransaction._

  def close() {
    //no-op (for now
    log.debug("Closing dummy nested transaction.")
  }

  def failureHandler(e: Throwable) {
    //no-op (for now
    log.debug("Failing dummy nested transaction.")
  }

  def rollback() {
    //no-op (for now
    log.debug("Rolling back dummy nested transaction.")
  }
}

object NestedTransaction {
  def apply() = new NestedTransaction

  val log = LoggerFactory.getLogger(classOf[NestedTransaction])
}

