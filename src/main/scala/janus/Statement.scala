package janus

import org.slf4j.LoggerFactory
import java.sql
import java.util.Date
import org.joda.time.DateTime
import com.typesafe.scalalogging.slf4j.Logging

trait Statement {

  /**
   * Execute the SQL statement and return a Boolean specifying success.
   */
  def execute(sql: String): Boolean

  /**
   * Execute the specified query and process the results with the supplied handler
   */
  def executeQuery[A](sql: String): Seq[Row]

  /**
   * Execute the specified update query and return the number of updated rows, along with any generated keys.
   */
  def executeUpdate(sql: String, returnAutoGeneratedKeys: Boolean = false): (Int, Seq[Row])

}

trait PreparedStatement {

  /**
   * Execute the SQL statement contained in this statement and return a Boolean specifying success.
   */
  def execute(): Boolean

  /**
   * Execute the query contained in this statement and process the rows with the supplied handler.
   */
  def executeQuery(): Seq[Row]

  /**
   * Execute the update query represented by this statement and return the number of updated rows and any generated keys.
   */
  def executeUpdate(): (Int, Seq[Row])

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
  def executeBatch(): Seq[Int]

  /**
   * Set the value of a parameter in this statement to the specified value
   * @param index The index of the parameter. NOTE - Unlike JDBC, parameters here are *zero based*.
   * @param value the value to set
   */
  def setParam(index: Int, value: DbValue)

  /**
   * Clear out all the currently set parameters on this prepared statement.
   */
  def clearParams()

}

private[janus] class JdbcStatement(stmt: java.sql.Statement) extends Statement {

  def execute(sql: String): Boolean = stmt.execute(sql)

  def executeQuery[A](sql: String): Seq[Row] = copyResultSet(stmt.executeQuery(sql))

  def executeUpdate(sql: String, returnAutoGeneratedKeys: Boolean = false): (Int, Seq[Row]) = {
    val generatedParams = if (returnAutoGeneratedKeys) {
      java.sql.Statement.RETURN_GENERATED_KEYS
    } else {
      java.sql.Statement.NO_GENERATED_KEYS
    }
    val count = stmt.executeUpdate(sql, generatedParams)
    val keys = if(returnAutoGeneratedKeys) {
      copyResultSet(stmt.getGeneratedKeys)
    } else {
      Seq.empty[Row]
    }
    (count, keys)
  }
}

private[janus] class JdbcPreparedStatement(ps: java.sql.PreparedStatement) extends PreparedStatement with Logging {

  def execute(): Boolean = ps.execute()

  def executeQuery(): Seq[Row] = copyResultSet(ps.executeQuery())

  def executeUpdate(): (Int, Seq[Row]) = {
    val count = ps.executeUpdate()
    val keys = copyResultSet(ps.getGeneratedKeys)
    (count, keys)
  }

  def addBatch() {
    ps.addBatch()
  }

  def clearBatch() {
    ps.clearBatch()
  }

  def executeBatch(): Seq[Int] = {
    ps.executeBatch().toSeq
  }

  def clearParams() {
    ps.clearParameters()
  }


  /**
   * Set the value of a parameter in this statement to the specified value
   * @param index The index of the parameter. NOTE - Unlike JDBC, parameters here are *zero based*.
   * @param value the value to set
   */
  override def setParam(index: Int, value: DbValue): Unit = {
    //stupid JDBC is 1-based
    val realIndex = index + 1
    value match {
      case DbString(x) => ps.setString(realIndex, x)
      case DbInt(x) => ps.setInt(realIndex, x)
      case DbLong(x) => ps.setLong(realIndex, x)
      case DbDouble(x) => ps.setDouble(realIndex, x)
      case DbDate(x) => ps.setDate(realIndex, new sql.Date(x.getMillis))
      case DbBoolean(x) => ps.setBoolean(realIndex, x)
      case DbNull => ps.setObject(realIndex, null)
      case DbUnknown(x) =>
        logger.warn("Warning - cannot directly convert value {} - using setObject.", x.toString)
        ps.setObject(realIndex, x)
      case x: Row => throw new IllegalArgumentException("Cannot set a Row value into a column.")
    }
  }

  def generatedKeys(): Traversable[Row] = copyResultSet(ps.getGeneratedKeys)

  def updateCount(): Int = ps.getUpdateCount

}

