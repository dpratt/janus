package janus

import scala.annotation.implicitNotFound
import org.joda.time.DateTime

@implicitNotFound(
"No SQL deserializer found for type ${A}. Try to implement an implicit DbReads for this type."
)
trait DbReads[A] {
  self =>

  def reads(dbValue: DbValue): DbResult[A]

  def map[B](f: A => B): DbReads[B] =
    DbReads[B] { dbValue => self.reads(dbValue).map(f) }

  def flatMap[B](f: A => DbReads[B]): DbReads[B] = DbReads[B] { dbValue =>
    self.reads(dbValue).flatMap(t => f(t).reads(dbValue))
  }

  def filter(f: A => Boolean): DbReads[A] =
    DbReads[A] { dbValue => self.reads(dbValue).filter(f) }

  def filter(error: String)(f: A => Boolean): DbReads[A] =
    DbReads[A] { dbValue => self.reads(dbValue).filter(error)(f) }

  def filterNot(f: A => Boolean): DbReads[A] =
    DbReads[A] { dbValue => self.reads(dbValue).filterNot(f) }

  def filterNot(error: String)(f: A => Boolean): DbReads[A] =
    DbReads[A] { dbValue => self.reads(dbValue).filterNot(error)(f) }

  def collect[B](error: String)(f: PartialFunction[A, B]) =
    DbReads[B] { dbValue => self.reads(dbValue).collect(error)(f) }

  def orElse(v: DbReads[A]): DbReads[A] =
    DbReads[A] { dbValue => self.reads(dbValue).orElse(v.reads(dbValue)) }

}

sealed abstract class RowPath {
  self =>

  def eval(dbVal: DbValue): DbResult[DbValue]

  def read[T](implicit rt: DbReads[T]): DbReads[T] = DbReads[T] { dbVal =>
    self.eval(dbVal).flatMap(x => rt.reads(x))
  }

  def readNullable[T](implicit rt: DbReads[T]): DbReads[Option[T]] = DbReads[Option[T]] { dbVal =>
    self.eval(dbVal).fold(
      _ => DbSuccess(None),
      {
        case DbNull => DbSuccess(None)
        case x => rt.reads(x).map(Some(_))
      }
    )
  }
}

object RowPath {
  def apply(columnName: String): RowPath = new NamePath(columnName)
  def apply(idx: Int): RowPath = new IndexPath(idx)

  import scala.language.implicitConversions

  implicit def string2RowPath(name: String) = RowPath(name)
  implicit def int2RowPath(idx: Int) = RowPath(idx)
}

class NamePath(columnName: String) extends RowPath {
  override def eval(dbVal: DbValue): DbResult[DbValue] = dbVal match {
    case row: Row =>
      row.apply(columnName) match {
        case undef: DbUndefined => DbError(s"Unknown column: $columnName")
        case x => DbSuccess(x)
      }
    case x => DbError(s"Expected row, got $x")
  }
}

class IndexPath(idx: Int) extends RowPath {
  override def eval(dbVal: DbValue): DbResult[DbValue] = dbVal match {
    case row: Row =>
      row.apply(idx) match {
        case undef: DbUndefined => DbError(s"Invalid column index: $idx")
        case x => DbSuccess(x)
      }
    case x => DbError(s"Expected row, got $x")
  }
}



object DbReads extends DefaultReads {
  def apply[A](f: DbValue => DbResult[A]): DbReads[A] = new DbReads[A] {
    def reads(value: DbValue) = f(value)
  }

  def of[A](implicit reads: DbReads[A]): DbReads[A] = reads

  import play.api.libs.functional._

  //This lets us use the Builder combinator from the play functional library to construct
  //complex DbReads instances declaratively
  implicit def applicative(implicit applicativeDbResult: Applicative[DbResult]): Applicative[DbReads] = new Applicative[DbReads] {
    def pure[A](a: A): DbReads[A] = DbReads[A] { _ => DbSuccess(a) }
    def map[A, B](m: DbReads[A], f: A => B): DbReads[B] = m.map(f)
    def apply[A, B](mf: DbReads[A => B], ma: DbReads[A]): DbReads[B] = new DbReads[B] {
      def reads(js: DbValue) = applicativeDbResult(mf.reads(js), ma.reads(js))
    }
  }

  implicit def functorReads(implicit a: Applicative[DbReads]) = new Functor[DbReads] {
    def fmap[A, B](reads: DbReads[A], f: A => B): DbReads[B] = a.map(reads, f)
  }

  //  implicit def alternative(implicit a: Applicative[DbReads]): Alternative[DbReads] = new Alternative[DbReads] {
//    val app = a
//    def |[A, B >: A](alt1: DbReads[A], alt2: DbReads[B]): DbReads[B] = new DbReads[B] {
//      def reads(js: DbValue) = alt1.reads(js) match {
//        case r @ DbSuccess(_) => r
//        case r @ DbError(es1) => alt2.reads(js) match {
//          case r2 @ DbSuccess(_) => r2
//          case r2 @ DbError(es2) => DbError(es1 ++ es2)
//        }
//      }
//    }
//    def empty: DbReads[Nothing] = new DbReads[Nothing] { def reads(js: DbValue) = DbError(Seq()) }
//  }

}

trait DefaultReads {

  implicit object StringReads extends DbReads[String] {
    override def reads(dbValue: DbValue): DbResult[String] = dbValue match {
      case DbString(x) => DbSuccess(x)
      case _ => DbError("error.expected.string")
    }
  }

  implicit object IntReads extends DbReads[Int] {
    override def reads(dbValue: DbValue): DbResult[Int] = dbValue match {
      case DbInt(x) => DbSuccess(x)
      case _ => DbError("error.expected.int")
    }
  }

  implicit object LongReads extends DbReads[Long] {
    override def reads(dbValue: DbValue): DbResult[Long] = dbValue match {
      case DbLong(x) => DbSuccess(x)
      case DbInt(x) => DbSuccess(x)
      case _ => DbError("error.expected.long")
    }
  }

  implicit object DoubleReads extends DbReads[Double] {
    override def reads(dbValue: DbValue): DbResult[Double] = dbValue match {
      case DbDouble(x) => DbSuccess(x)
      case _ => DbError("error.expected.double")
    }
  }

  implicit object BooleanReads extends DbReads[Boolean] {
    override def reads(dbValue: DbValue): DbResult[Boolean] = dbValue match {
      case DbBoolean(x) => DbSuccess(x)
      case _ => DbError("error.expected.boolean")
    }
  }

  implicit object DateReads extends DbReads[DateTime] {
    override def reads(dbValue: DbValue): DbResult[DateTime] = dbValue match {
      case DbDate(x) => DbSuccess(x)
      case _ => DbError("error.expected.date")
    }
  }

  implicit def optionReads[T](implicit rt: DbReads[T]): DbReads[Option[T]] = new DbReads[Option[T]] {
    def reads(dbValue: DbValue): DbResult[Option[T]] = dbValue match {
      case DbNull => DbSuccess(None)
      case x => rt.reads(x).map(Some(_))
    }
  }

}
