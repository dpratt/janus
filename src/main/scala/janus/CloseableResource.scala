package janus

import akka.dispatch.Future
import org.slf4j.LoggerFactory
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

  private val logger = LoggerFactory.getLogger(CloseableResource.getClass)

  def withResource[A <: CloseableResource, B](resource: A)(f: A => B) : B = {
    val resourceClass = resource.getClass

    val result = try {
      f(resource)
    } catch {
      case e: Throwable => {
        logger.debug("Error wrapping resource of type {}. Cleaning up.", resourceClass.toString)
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
      logger.debug("Cleaning up for plain resource of type {}", resourceClass.toString)
      //we have a regular value
      //the assumption here is that we can just close the transaction
      //otherwise, we would have rolled back and thrown the cause above
      resource.close()
      result
    } else {
      val resultFuture = result.asInstanceOf[Future[_]]
      logger.debug("Adding cleanup handler for {} - {}", Array(resourceClass.toString, resultFuture.toString))
      //transform the result Future into a new Future that will have a defined order of completion handlers
      //andThen will cause a defined ordering of handlers - we need to do this to ensure that handlers wrapped around the
      //future at a higher level execute in the proper order - for example, after the Future completes, we need to clean up
      //a PreparedStatement, *then* a Transaction, *then* the Session or connection.
      val transformedFuture = resultFuture andThen {
        case Right(_) => {
          logger.debug("Cleaning up Future resource of type {}", resourceClass.toString)
          resource.close()
        }
        case Left(e) => {
          logger.debug("Rolling back Future resource of type {}", resourceClass.toString)
          resource.failureHandler(e)
        }
      }
      transformedFuture.asInstanceOf[B]
    }
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


private[janus] class JdbcTransaction(c: Connection) extends Transaction {

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
    log.debug("Failing transaction due to exception.")
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

private[janus] object JdbcTransaction {
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

