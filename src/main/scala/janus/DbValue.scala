package janus

import org.joda.time.DateTime
import java.util.Date
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.{Success, Failure, Try}


sealed trait DbValue {

  def as[T](implicit fjs: DbReads[T]): T = fjs.reads(this).fold(
    valid = identity,
    invalid = e => throw new ConversionException(e)
  )

  def validate[T](implicit rds: DbReads[T]): DbResult[T] = rds.reads(this)
}

class DbUndefined(message: => String) extends DbValue{
  override def toString: String = s"DbUndefined($message)"
}

case class DbString(value: String) extends DbValue
case class DbBoolean(value: Boolean) extends DbValue
case class DbInt(value: Int) extends DbValue
case class DbLong(value: Long) extends DbValue
case class DbDouble(value: Double) extends DbValue
case class DbDate(value: DateTime) extends DbValue

case class DbUnknown(value: Any) extends DbValue

case object DbNull extends DbValue

class Row private (data: IndexedSeq[DbValue], metadata: Metadata) extends DbValue {

  /**
   * Retrieve a value from this row.
   */
  def apply(columnName: String): DbValue = {
    byName(columnName).getOrElse(new DbUndefined(s"Unknown column $columnName"))
  }

  /**
   * Retrieve a value from this row.
   * @param index The index of the column in the result set. NOTE - unlike JDBC, this is *zero based*.
   */
  def apply(index: Int): DbValue = {
    byIndex(index).getOrElse(new DbUndefined(s"Invalid column index $index"))
  }

  /**
   * Retrieve a value from this row.
   * @throws UnknownColumnException if the column name is not known
   */
  def value(columnName: String): DbValue = {
    byName(columnName).get
  }

  /**
   * Retrieve a value from this row.
   * @param index The index of the column in the result set. NOTE - unlike JDBC, this is *zero based*.
   * @throws UnknownColumnException if the column index is invalid
   */
  def value(index: Int): DbValue = {
    byIndex(index).get
  }

  private def byName(name: String): Try[DbValue] = {
    metadata.indexForColumn(name).map(data(_))
  }

  private def byIndex(idx: Int): Try[DbValue] = {
    if(idx > data.length) {
      Failure(new UnknownColumnException(idx, data.length))
    } else {
      Success(data(idx))
    }
  }

}

object Row {
  def apply(meta: Metadata, rawData: IndexedSeq[Any]): Row =  {
    val converted = rawData.map(DbValue.fromAny)
    new Row(converted, meta)
  }
}

object DbValue extends Logging {

  def fromAny(value: Any): DbValue = {
    if(value == null) {
      DbNull
    } else {
      value match {
        case s: String => DbString(s)
        case i: Int => DbInt(i)
        case l: Long => DbLong(l)
        case d: Double => DbDouble(d)
        case b: Boolean => DbBoolean(b)
        case d: Date => DbDate(new DateTime(d))
        case d: DateTime => DbDate(d)
        case x =>
          logger.warn("Unknown type {}, using DbUnknown", x.getClass.toString)
          DbUnknown(x)
      }
    }
  }

}




