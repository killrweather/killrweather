package com.datastax.killrweather.syntax

package object future {
  import scala.concurrent._
  import scalaz._
  import scalaz.contrib.std.scalaFuture._

  type FutureT[+A] = EitherT[Future, Throwable, A]

  /** Avoid the need to handle Future error/timeout via callbacks by transforming the value into an EitherT, i.e.,
    * EitherT[Future, Throwable, A] === Future[Throwable \/ A]. */
  implicit class ScalaFutureOps[A](future: Future[A])(implicit context: ExecutionContext) {
    def eitherT: EitherT[Future, Throwable, A] =
      EitherT.eitherT(
        future
          .map(\/.right)
          .recover { case e: Throwable => \/.left(e) })

    def valueOrThrow[B](implicit ev: A <:< \/[Throwable, B]): Future[B] =
      future map (_ valueOr (throw _))
  }
}

