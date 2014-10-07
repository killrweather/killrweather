package com.datastax

import scalaz.{\/, EitherT}

package object killrweather {

  import scala.concurrent._
  import scalaz._

  implicit class ScalaFutureOps[A](future: Future[A])(implicit context: ExecutionContext) {
    def eitherT: EitherT[Future, Throwable, A] =
      EitherT.eitherT(
        future
          .map(\/.right)
          .recover { case e: Throwable => \/.left(e) })

    def valueOrThrow[B](implicit ev: A <:< \/[Throwable, B]): Future[B] =
      future map (_ valueOr (throw _))
  }

  type FutureT[+A] = EitherT[Future, Throwable, A]

  type Id = String

}
