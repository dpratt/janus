package janus

import java.sql.ResultSetMetaData
import scala.util.Try

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
    case (column, index) => IndexedSeq((column.shortName.toUpperCase, index), (column.name.toUpperCase, index), (column.alias.toUpperCase, index))
  }.toMap

  lazy val availableColumns: Seq[String] = columnIndexMap.keys.toSeq

  def get(name: String): ColumnInfo = {
    columns(indexForColumn(name.toUpperCase).get)
  }

  def get(index: Int): ColumnInfo = {
    if (index < 0 || index >= columns.size) {
      throw new UnknownColumnException(index, columns.size)
    }
    columns(index)
  }

  def indexForColumn(columnName: String): Try[Int] = {
    Try {
      columnIndexMap.getOrElse(columnName.toUpperCase, throw new UnknownColumnException(columnName, availableColumns))
    }
  }
}

private[janus] object Metadata {
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
