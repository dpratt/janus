package janus

import org.slf4j.LoggerFactory
import CloseableResource.withResource

trait Statement extends CloseableResource {

  /**
   * Execute the SQL statement and return a Boolean specifying success.
   */
  def execute(sql: String): Boolean

  /**
   * Execute the specified query and process the results with the supplied handler
   */
  def executeQuery[A : Manifest](sql: String)(resultHandler: ResultSet => A): A

  /**
   * Execute the specified update query and return the number of updated rows.
   */
  def executeUpdate(sql: String, returnAutoGeneratedKeys: Boolean = false): Int

  /**
   * Get the generated keys from the most recently executed statement. NOTE - the caller is responsible
   * for closing this ResultSet when done with it.
   */
  def generatedKeys(): ResultSet
}

trait PreparedStatement extends CloseableResource {

  /**
   * Execute the SQL statement contained in this statement and return a Boolean specifying success.
   * @return
   */
  def execute(): Boolean

  /**
   * Execute the query contained in this statement and process the rows with the supplied handler.
   * @return
   */
  def executeQuery[A : Manifest](resultHandler: ResultSet => A): A

  /**
   * Execute the update query represented by this statement and return the number of updated rows.
   */
  def executeUpdate(): Int

  /**
   * Set the value of a parameter in this statement to the specified value
   * @param value the value to set
   * @tparam T the type of the value
   */
  def setParam[T : Manifest](index: Int, value: T)

  /**
   * Get the generated keys from the most recently executed statement. NOTE - the caller is responsible
   * for closing this ResultSet when done with it.
   */
  def generatedKeys(): ResultSet

}

/**
 * A thin wrapper on top of the JDBC result set
 */
trait ResultSet extends CloseableResource {

  //TODO: Perhaps make this iterable?
  def next(): Boolean

  def getValue[A : Manifest](index: Int): A

}

sealed class JdbcStatement(stmt: java.sql.Statement) extends Statement {

  import JdbcStatement._

  def close() {
    log.debug("Closing statement - {}", stmt.toString)
    stmt.close()
  }

  def failureHandler(e: Throwable) {
    log.debug("Cleaning up statement due to exception - {}", Array(stmt.toString, e))
    close()
  }

  def execute(sql: String): Boolean = stmt.execute(sql)
  def executeQuery[A : Manifest](sql: String)(resultHandler: ResultSet => A): A = {
    withResource(JdbcResultSet(stmt.executeQuery(sql)))(resultHandler)
  }
  def executeUpdate(sql: String, returnAutoGeneratedKeys: Boolean): Int = {
    val generatedParams = if (returnAutoGeneratedKeys) {
      java.sql.Statement.RETURN_GENERATED_KEYS
    } else {
      java.sql.Statement.NO_GENERATED_KEYS
    }
    stmt.executeUpdate(sql, generatedParams)
  }

  def generatedKeys(): ResultSet = JdbcResultSet(stmt.getGeneratedKeys)
}

object JdbcStatement {
  val log = LoggerFactory.getLogger(classOf[JdbcStatement])
}

sealed class JdbcPreparedStatement(ps: java.sql.PreparedStatement) extends PreparedStatement {

  import JdbcPreparedStatement._

  def execute(): Boolean = ps.execute()

  def executeQuery[A : Manifest](resultHandler: ResultSet => A): A = {
    withResource(JdbcResultSet(ps.executeQuery()))(resultHandler)
  }
  def executeUpdate(): Int = ps.executeUpdate()

  def setParam[T : Manifest](index: Int, value: T) {
    import ClassConstants._
    val m = manifest[T]
    m.erasure match {
      case StringClass => ps.setString(index, value.toString)
      case IntClass => ps.setInt(index, value.asInstanceOf[Int])
      case DoubleClass => ps.setDouble(index, value.asInstanceOf[Double])
      case DateSQLClass => ps.setDate(index, value.asInstanceOf[java.sql.Date])
      case _ => throw new RuntimeException("Unknown type " + m.toString)
    }
  }

  def generatedKeys(): ResultSet = JdbcResultSet(ps.getGeneratedKeys)

  def close() {
    ps.close()
  }

  def failureHandler(e: Throwable) {
    log.error("Closing prepared statement due to exception.", e)
    close()
  }
}

object JdbcPreparedStatement {
  val log = LoggerFactory.getLogger(classOf[JdbcPreparedStatement])
}

sealed class JdbcResultSet(rs: java.sql.ResultSet) extends ResultSet {
  //TODO: Perhaps make this iterable?

  import JdbcResultSet._

  def next(): Boolean = rs.next()

  def getValue[A: Manifest](index: Int): A = {
    import ClassConstants._
    val m = manifest[A]
    (m.erasure match {
      case StringClass => rs.getString(index)
      case IntClass => rs.getInt(index)
      case DoubleClass => rs.getDouble(index)
      case DateSQLClass => rs.getDate(index)
      case _ => throw new RuntimeException("Unknown type " + m.toString)
    }).asInstanceOf[A]
  }

  def close() {
    rs.close()
  }

  def failureHandler(e: Throwable) {
    log.error("Closing ResultSet due to exception.", e)
  }
}

object JdbcResultSet {

  def apply(rs: java.sql.ResultSet) = new JdbcResultSet(rs)

  val log = LoggerFactory.getLogger(classOf[JdbcResultSet])
}


object ClassConstants {
  //useful constants - we use these for pattern matching
  val StringClass = classOf[String]
  val IntClass = classOf[Int]
  val DoubleClass = classOf[Double]
  val DateSQLClass = classOf[java.sql.Date]
}