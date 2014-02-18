package janus

case class ConversionException(errors: Seq[String]) extends RuntimeException(s"Problem converting value - ${errors.mkString(", ")}")

case class DbSuccess[T](value: T) extends DbResult[T] {
  def get: T = value
}

case class DbError(errors: Seq[String]) extends DbResult[Nothing] {
  def get: Nothing = throw new ConversionException(errors)
}

object DbError {
  def apply(): DbError = DbError(Seq())
  def apply(err: String): DbError = DbError(Seq(err))
}

sealed trait DbResult[+A] {

  def fold[X](invalid: Seq[String] => X, valid: A => X): X = this match {
    case DbSuccess(v) => valid(v)
    case DbError(e) => invalid(e)
  }

  def map[X](f: A => X): DbResult[X] = this match {
    case DbSuccess(v) => DbSuccess(f(v))
    case e: DbError => e
  }

  def combine[B, X](that: DbResult[B])(combiner: (A, B) => X): DbResult[X] = (this, that) match {
    case (DbError(leftError), DbError(rightError)) => DbError(leftError ++ rightError)
    case (left: DbError, right: DbSuccess[B]) => left
    case (left: DbSuccess[A], right: DbError) => right
    case (DbSuccess(a), DbSuccess(b)) => DbSuccess(combiner(a, b))
  }

  def filterNot(error: String)(p: A => Boolean): DbResult[A] =
    this.flatMap { a => if (p(a)) DbError(error) else DbSuccess(a) }

  def filterNot(p: A => Boolean): DbResult[A] =
    this.flatMap { a => if (p(a)) DbError() else DbSuccess(a) }

  def filter(p: A => Boolean): DbResult[A] =
    this.flatMap { a => if (p(a)) DbSuccess(a) else DbError() }

  def filter(otherwise: String)(p: A => Boolean): DbResult[A] =
    this.flatMap { a => if (p(a)) DbSuccess(a) else DbError(otherwise) }

  def collect[B](otherwise: String)(p: PartialFunction[A, B]): DbResult[B] = flatMap {
    case t if p.isDefinedAt(t) => DbSuccess(p(t))
    case _ => DbError(otherwise)
  }

  def flatMap[X](f: A => DbResult[X]): DbResult[X] = this match {
    case DbSuccess(v) => f(v)
    case e: DbError => e
  }

  def zip[X](that: DbResult[X]): DbResult[(A, X)] = combine(that) { (left, right) => (left, right) }

  def foreach(f: A => Unit): Unit = this match {
    case DbSuccess(a) => f(a)
    case _ => ()
  }

  def get: A

  def getOrElse[AA >: A](t: => AA): AA = this match {
    case DbSuccess(a) => a
    case DbError(_) => t
  }

  def orElse[AA >: A](t: => DbResult[AA]): DbResult[AA] = this match {
    case s @ DbSuccess(_) => s
    case DbError(_) => t
  }

  def asOpt = this match {
    case DbSuccess(v) => Some(v)
    case DbError(_) => None
  }

  def asEither: Either[Seq[String], A] = this match {
    case DbSuccess(v) => Right(v)
    case DbError(e) => Left(e)
  }

  def recover[AA >: A](errManager: PartialFunction[DbError, AA]): DbResult[AA] = this match {
    case DbSuccess(v) => DbSuccess(v)
    case e: DbError => if (errManager isDefinedAt e) DbSuccess(errManager(e)) else this
  }
}

object DbResult {
  import play.api.libs.functional._

//  implicit def alternativeDbResult(implicit a: Applicative[DbResult]): Alternative[DbResult] = new Alternative[DbResult] {
//    val app = a
//    def |[A, B >: A](alt1: DbResult[A], alt2: DbResult[B]): DbResult[B] = (alt1, alt2) match {
//      case (DbError(e), DbSuccess(t)) => DbSuccess(t)
//      case (DbSuccess(t), _) => DbSuccess(t)
//      case (DbError(e1), DbError(e2)) => DbError(e1 ++ e2)
//    }
//    def empty: DbResult[Nothing] = DbError(Seq())
//  }

  implicit val applicativeDbResult: Applicative[DbResult] = new Applicative[DbResult] {
    def pure[A](a: A): DbResult[A] = DbSuccess(a)
    def map[A, B](m: DbResult[A], f: A => B): DbResult[B] = m.map(f)
    def apply[A, B](mf: DbResult[A => B], ma: DbResult[A]): DbResult[B] = (mf, ma) match {
      case (DbSuccess(f), DbSuccess(a)) => DbSuccess(f(a))
      case (DbError(e1), DbError(e2)) => DbError(e1 ++ e2)
      case (DbError(e), _) => DbError(e)
      case (_, DbError(e)) => DbError(e)
    }
  }

}