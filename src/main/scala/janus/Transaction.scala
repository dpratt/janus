package janus

import java.sql.Connection
import org.slf4j.LoggerFactory
import collection.mutable.ArrayBuffer
import util.control.NonFatal
import org.springframework.transaction.TransactionStatus

/**
 * Represents a transaction's state.
 */
trait Transaction {

  /**
   * Call this method to roll back the current transaction after it's resource is closed.
   */
  def setRollback()

}

private[janus] class JdbcTransaction extends Transaction {

  var shouldRollback = false

  def setRollback() {
    shouldRollback = true
  }
}

//No support for nested transactions right now - they do nothing.
private[janus] class NestedTransaction(parent: JdbcTransaction) extends Transaction {

  def setRollback() {
    parent.setRollback()
  }
}

private[janus] class SpringTransaction(tx: TransactionStatus) extends Transaction {
  /**
   * Call this method to roll back the current transaction after it's resource is closed.
   */
  def setRollback() {
    tx.setRollbackOnly()
  }
}



