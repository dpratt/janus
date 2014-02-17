package janus

import org.joda.time.DateTime
import java.util.Date
import com.typesafe.scalalogging.slf4j.Logging


sealed trait DbValue {

  def as[T](implicit fjs: DbReads[T]): T = fjs.reads(this).fold(
    valid = identity,
    invalid = e => throw new ConversionException(e)
  )

  def validate[T](implicit rds: DbReads[T]): DbResult[T] = rds.reads(this)
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

  private def get(index: Int): DbValue = {
    if(index > data.length) {
      throw new IllegalArgumentException(s"Invalid column index - $index")
    }
    data(index)
  }

  /**
   * Retrieve a value from this row.
   */
  def apply(columnName: String): DbValue = {
    value(columnName)
  }

  /**
   * Retrieve a value from this row.
   */
  def value(columnName: String): DbValue = {
    get(metadata.indexForColumn(columnName))
  }

  /**
   * Retrieve a value from this row.
   * @param index The index of the column in the result set. NOTE - unlike JDBC, this is *zero based*.
   * @return
   */
  def apply(index: Int): DbValue = {
    value(index)
  }

  /**
   * Retrieve a value from this row.
   * @param index The index of the column in the result set. NOTE - unlike JDBC, this is *zero based*.
   */
  def value(index: Int): DbValue = {
    get(index)
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




