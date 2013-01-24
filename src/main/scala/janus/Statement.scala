package janus

import org.slf4j.LoggerFactory
import java.sql
import java.util.Date
import org.joda.time.DateTime

trait Statement extends CloseableResource {

  /**
   * Execute the SQL statement and return a Boolean specifying success.
   */
  def execute(sql: String): Boolean

  /**
   * Execute the specified query and process the results with the supplied handler
   */
  def executeQuery(sql: String): Stream[Row]

  /**
   * Execute the specified update query and return the number of updated rows.
   */
  def executeUpdate(sql: String, returnAutoGeneratedKeys: Boolean = false): Int

  /**
   * Get the generated keys from the most recently executed statement. NOTE - the caller is responsible
   * for closing this ResultSet when done with it.
   */
  def generatedKeys: Stream[Row]

  /**
   * Get the update count from the most recently executed statement.
   */
  def updateCount: Int
}

trait PreparedStatement extends CloseableResource {

  /**
   * Execute the SQL statement contained in this statement and return a Boolean specifying success.
   * @return
   */
  def execute(): Boolean

  /**
   * Execute the query contained in this statement and process the rows with the supplied handler.
   * Each row in the resultset will be mapped using the supplied function.
   * @return
   */
  def executeQuery: Stream[Row]

  /**
   * Execute the update query represented by this statement and return the number of updated rows.
   */
  def executeUpdate(): Int

  /**
   * Add the current parameter state to this PreparedStatement's batch list.
   */
  def addBatch()

  /**
   * Remove all batched statements from this PreparedStatement.
   */
  def clearBatch()

  /**
   * Execute this PreparedStatement once for each batch of parameters that have been added to it.
   */
  def executeBatch(): Array[Int]

  /**
   * Set the value of a parameter in this statement to the specified value
   * @param index The index of the parameter. NOTE - Unlike JDBC, parameters here are *zero based*.
   * @param value the value to set
   */
  def setParam(index: Int, value: Any)

  /**
   * Clear out all the currently set parameters on this prepared statement.
   */
  def clearParams()

  /**
   * Get the generated keys from the most recently executed statement.
   */
  def generatedKeys: Stream[Row]

  /**
   * Get the update count from the most recently executed statement.
   */
  def updateCount: Int

}

private[janus] class JdbcStatement(stmt: java.sql.Statement) extends Statement {

  import JdbcStatement._

  def close() {
    log.debug("Closing statement")
    stmt.close()
  }

  def failureHandler(e: Throwable) {
    log.debug("Cleaning up statement due to exception - {}", stmt.toString)
    close()
  }

  def execute(sql: String): Boolean = stmt.execute(sql)
  def executeQuery(sql: String): Stream[Row] = {
    JdbcRow.convertResultSet(stmt.executeQuery(sql))
  }
  def executeUpdate(sql: String, returnAutoGeneratedKeys: Boolean): Int = {
    val generatedParams = if (returnAutoGeneratedKeys) {
      java.sql.Statement.RETURN_GENERATED_KEYS
    } else {
      java.sql.Statement.NO_GENERATED_KEYS
    }
    stmt.executeUpdate(sql, generatedParams)
  }

  def generatedKeys = JdbcRow.convertResultSet(stmt.getGeneratedKeys)

  def updateCount = stmt.getUpdateCount
}

private[janus] object JdbcStatement {
  val log = LoggerFactory.getLogger(classOf[JdbcStatement])
}

private[janus] class JdbcPreparedStatement(ps: java.sql.PreparedStatement) extends PreparedStatement {

  import JdbcPreparedStatement._

  def execute(): Boolean = ps.execute()
  def executeQuery: Stream[Row] = {
    JdbcRow.convertResultSet(ps.executeQuery())
  }
  def executeUpdate(): Int = ps.executeUpdate()

  def addBatch() {
    ps.addBatch()
  }

  def clearBatch() {
    ps.clearBatch()
  }

  def executeBatch(): Array[Int] = {
    ps.executeBatch()
  }

  def clearParams() {
    ps.clearParameters()
  }

  def setParam(index: Int, value: Any) {
    //stupid JDBC is 1-based
    val realIndex = index + 1
    value match {
      case o: Option[_] => setParam(index, o.orNull)
      case s: String => ps.setString(realIndex, s)
      case i: Int => ps.setInt(realIndex, i)
      case l: Long => ps.setLong(realIndex, l)
      case d: Double => ps.setDouble(realIndex, d)
      case b: Boolean => ps.setBoolean(realIndex, b)
      case d: Date => ps.setDate(realIndex, new sql.Date(d.getTime))
      case d: DateTime => ps.setDate(realIndex, new sql.Date(d.toDate.getTime))
      case value: Any => {
        log.warn("Unknown type.")
        ps.setObject(realIndex, value)
      }
      case null => ps.setObject(realIndex, null)
    }
  }

  def generatedKeys: Stream[Row] = JdbcRow.convertResultSet(ps.getGeneratedKeys)

  def updateCount: Int = ps.getUpdateCount

  def close() {
    log.debug("Closing prepared statement - {}", ps.toString)
    ps.close()
  }

  def failureHandler(e: Throwable) {
    log.debug("Closing prepared statement due to exception.")
    close()
  }
}

private[janus] object JdbcPreparedStatement {
  val log = LoggerFactory.getLogger(classOf[JdbcPreparedStatement])
}

