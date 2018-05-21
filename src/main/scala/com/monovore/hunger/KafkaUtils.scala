package com.monovore.hunger

import cats.effect.IO
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse

import scala.collection.JavaConverters._

object KafkaUtils {

  def retryForever[A](action: IO[A]): IO[A] =
    action.attempt
      .flatMap {
        case Left(retriable: RetriableException) =>
          retryForever(action)
        case other =>
          IO.fromEither(other)
      }

  def checkErrors(response: AbstractResponse): IO[Unit] =
    response.errorCounts.keySet.asScala
      .filterNot { _ == Errors.NONE }
      .headOption match {
        case Some(error) => IO.raiseError(error.exception)
        case None => IO.pure(())
      }
}
