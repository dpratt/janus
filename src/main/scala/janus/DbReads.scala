package janus

import scala.annotation.implicitNotFound

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

object DbReads extends DefaultReads {
  def apply[A](f: DbValue => DbResult[A]): DbReads[A] = new DbReads[A] {
    def reads(value: DbValue) = f(value)
  }

  import play.api.libs.functional._

  //these are here if we want to use the Builder combinators at some point to create more complex
  //reads implementations - not really used now, but could be if we make Row a first-class DbValue
  implicit def applicative(implicit applicativeDbResult: Applicative[DbResult]): Applicative[DbReads] = new Applicative[DbReads] {
    def pure[A](a: A): DbReads[A] = DbReads[A] { _ => DbSuccess(a) }
    def map[A, B](m: DbReads[A], f: A => B): DbReads[B] = m.map(f)
    def apply[A, B](mf: DbReads[A => B], ma: DbReads[A]): DbReads[B] = new DbReads[B] {
      def reads(js: DbValue) = applicativeDbResult(mf.reads(js), ma.reads(js))
    }
  }

  implicit def alternative(implicit a: Applicative[DbReads]): Alternative[DbReads] = new Alternative[DbReads] {
    val app = a
    def |[A, B >: A](alt1: DbReads[A], alt2: DbReads[B]): DbReads[B] = new DbReads[B] {
      def reads(js: DbValue) = alt1.reads(js) match {
        case r @ DbSuccess(_) => r
        case r @ DbError(es1) => alt2.reads(js) match {
          case r2 @ DbSuccess(_) => r2
          case r2 @ DbError(es2) => DbError(es1 ++ es2)
        }
      }
    }
    def empty: DbReads[Nothing] = new DbReads[Nothing] { def reads(js: DbValue) = DbError(Seq()) }
  }

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
      case _ => DbError("error.expected.long")
    }
  }


  implicit def optionReads[T](implicit rt: DbReads[T]): DbReads[Option[T]] = new DbReads[Option[T]] {
    def reads(dbValue: DbValue): DbResult[Option[T]] = dbValue match {
      case DbNull => DbSuccess(None)
      case x => rt.reads(x).map(Some(_))
    }
  }




}
