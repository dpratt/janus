package janus

import java.sql.ResultSetMetaData
import org.slf4j.LoggerFactory
import java.util.Date
import util.Try
import org.joda.time.DateTime

case class ColumnInfo(name: String, alias: String, nullable: Boolean, valueClassName: String) {
  val shortName = name.split(".").lastOption.getOrElse(name)
}

class UnknownColumnException(message: String)
  extends Exception(message) {

  def this(columnName: String, candidates: Seq[String]) {
    this(columnName + " not found, possible columns : " + candidates.mkString(","))
  }
  def this(badIndex: Int, columnCount: Int) {
    this("Column index " + badIndex + " out of bounds. Number of columns - " + columnCount)
  }
}

class Metadata(val columns: IndexedSeq[ColumnInfo]) {

  private lazy val columnIndexMap: Map[String, Int] = columns.zipWithIndex.flatMap {
    case (column, index) => List((column.shortName.toUpperCase, index), (column.name.toUpperCase, index), (column.alias.toUpperCase, index))
  }.toMap

  lazy val availableColumns: Seq[String] = columnIndexMap.keys.toSeq

  def get(name: String): Try[ColumnInfo] = {
    indexForColumn(name.toUpperCase) map {
      index =>
        columns(index)
    }
  }

  def get(index: Int): Try[ColumnInfo] = {
    Try {
      if (index < 0 || index >= columns.size) {
        throw new UnknownColumnException(index, columns.size)
      }
      columns(index)
    }
  }

  def indexForColumn(columnName: String): Try[Int] = {
    Try {
      columnIndexMap.getOrElse(columnName.toUpperCase, throw new UnknownColumnException(columnName, availableColumns))
    }
  }
}

private object Metadata {
  def apply(rs: java.sql.ResultSet): Metadata = {
    val metadata = rs.getMetaData
    val columnCount = metadata.getColumnCount
    val items: IndexedSeq[ColumnInfo] = for (i <- 1 to columnCount) yield {
      val isNullable = metadata.isNullable(i) != ResultSetMetaData.columnNoNulls
      ColumnInfo(
        name = metadata.getTableName(i) + "." + metadata.getColumnName(i),
        alias = metadata.getColumnLabel(i),
        nullable = isNullable,
        valueClassName = metadata.getColumnClassName(i)
      )
    }
    new Metadata(items)
  }
}

trait Row {

  val metadata: Metadata
  protected val data: IndexedSeq[Any]

  //construct a list of (Any, Boolean) consisting of the nullable flag on the metadata and the data itself
  //if the column is nullable, return an Option wrapping the data, otherwise just return the data
  lazy val asList = data.zip(metadata.columns.map(_.nullable)).map {
    case (value, nullable) => if (nullable) {
      Option(value)
    } else {
      value
    }
  }

  //construct a list of (columnName, columnValue) and then convert it to a map
  lazy val asMap = metadata.columns.map(_.name).zip(asList).toMap

  private def get[A](index: Int)(implicit extractor: Column[A]): Try[A] = {
    metadata.get(index).flatMap {
      columnInfo =>
        val rawValue = data(index)
        extractor(rawValue, columnInfo)
    }
  }

  /**
   * Retrieve a value from this row.
   */
  def apply[A: Column](columnName: String): A = {
    metadata.indexForColumn(columnName).flatMap {
      index =>
        get(index)
    }.get
  }

  /**
   * Retrieve a value from this row.
   * @param index The index of the column in the result set. NOTE - unlike JDBC, this is *zero based*.
   * @return
   */
  def apply[A: Column](index: Int): A = {
    get(index).get
  }

}

trait ValueExtractor[A] extends (Any => Option[A])

object ValueExtractor {

  def apply[A](pf: PartialFunction[Any, A]): ValueExtractor[A] = {
    //transform the supplied PF to Any => Option[A]
    val lifted = pf.lift
    new ValueExtractor[A] {
      def apply(value: Any): Option[A] = {
        if (value == null) {
          //can't extract a null value - we pass it through, though, since a
          //ValueExtractor really just does type coercion for us.
          Some(null).asInstanceOf[Option[A]]
        } else {
          lifted(value)
        }
      }
    }
  }

  implicit val stringExtractor = ValueExtractor {
    case s: String => s
    case otherValue: Any => otherValue.toString
  }

  implicit val intExtractor = ValueExtractor {
    case i: Int => i
  }

  implicit val longExtractor = ValueExtractor {
    case l: Long => l
    case i: Int => i.toLong
  }

  implicit val doubleExtractor = ValueExtractor {
    case d: Double => d
  }

  implicit val booleanExtractor = ValueExtractor {
    //could also match strings here, but would rather bind to what the DB returns
    case b: Boolean => b
  }

  implicit val dateExtractor = ValueExtractor {
    case d: Date => d
  }

  implicit val dateTimeExtractor = ValueExtractor {
    case d: Date => new DateTime(d)
  }

}


/**
 * A trait that defines a typeclass for extracting values of a given type from an instance of Any,
 * while applying the context and constraints of the column being mapped.
 * @tparam A the type to be extracted
 */
trait Column[A] extends ((Any, ColumnInfo) => Try[A])


class IncompatibleColumnTypeException(value: Any, columnName: String, targetType: Class[_])
  extends Exception("Cannot convert '" + value + "' to " + targetType.toString + " for column " + columnName)

object Column {

  val log = LoggerFactory.getLogger(classOf[Column[_]])

  def notNull[A](implicit ve: ValueExtractor[A], m: Manifest[A]): Column[A] = new Column[A] {
    def apply(data: Any, columnDef: ColumnInfo): Try[A] = Try {
      //throw an exception if the data is null
      //      if(data == null) {
      //        throw new NullableColumnException(columnDef.name)
      //      }
      //alternatively, we can look at the resultset metadata and see if the column is nullable
      //This is a bit more strict, but makes the API a bit more unwieldy - for example, a
      //"SELECT count(*) from foo" query returns the first column as nullable, even though it isn't
      //I actually prefer this method, since it can catch db -> type mapping errors immediately. For example,
      //if the database has a nullable column with no null values in it, the above will pass and work fine
      //until a null value is (probably accidentally) inserted into the DB. Plus, this catches things at
      //unit test time rather than production. There is a tradeoff with verbosity in the API (e.g. aggregates
      //MUST be mapped to Option) but that's a small price to pay.
      if (columnDef.nullable) {
        throw new NullableColumnException(columnDef.name)
      }
      ve.apply(data) getOrElse {
        throw new IncompatibleColumnTypeException(data, columnDef.name, m.runtimeClass)
      }
    }
  }

  implicit val stringColumn = Column.notNull[String]
  implicit val intColumn = Column.notNull[Int]
  implicit val longColumn = Column.notNull[Long]
  implicit val doubleColumn = Column.notNull[Double]
  implicit val booleanColumn = Column.notNull[Boolean]
  implicit val dateColumn = Column.notNull[Date]
  implicit val dateTimeColumn = Column.notNull[DateTime]

  //This handles mapping columns to Option[_]
  implicit def nullableColumn[A](implicit ve: ValueExtractor[A], m: Manifest[A]): Column[Option[A]] = new Column[Option[A]] {
    def apply(data: Any, columnDef: ColumnInfo): Try[Option[A]] = Try {
      val extracted = ve(data) getOrElse {
        throw new IncompatibleColumnTypeException(data, columnDef.name, m.runtimeClass)
      }
      Option(extracted)
    }
  }

}

class NullableColumnException(columnName: String) extends RuntimeException("A nullable column named '" + columnName + "' was mapped to a scalar type. You must map this to an Option type.")

private[janus] case class JdbcRow(protected val data: IndexedSeq[Any], metadata: Metadata) extends Row

private[janus] object JdbcRow {

  def convertResultSet(resultSet: java.sql.ResultSet, closeStatementWhenComplete: Boolean = false): Stream[Row] = {

    val meta = Metadata(resultSet)
    val columnRange = 1 to meta.columns.size
    def data(rs: java.sql.ResultSet) = {
      for (i <- columnRange) yield rs.getObject(i)
    }

    def toStream(rs: java.sql.ResultSet): Stream[JdbcRow] = {
      if (!rs.next()) {
        if (closeStatementWhenComplete) {
          val stmt = rs.getStatement
          if (stmt != null) {
            //should automatically close the rs as well
            stmt.close()
          }
        } else {
          rs.close()
        }
        Stream.empty
      } else {
        JdbcRow(data(rs), meta) #:: toStream(rs)
      }
    }

    toStream(resultSet)
  }


}