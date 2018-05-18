package com.monovore.hunger

import java.util

import cats.effect.IO
import fs2._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, MetadataRequest}

import scala.collection.JavaConverters._

object Streaming {

  def consume(client: ApiClient[IO]): Stream[IO, Record] = {

    val partition = new TopicPartition("test-topic", 0)

    // Fetch a single batch, starting at the given offset, from the given partition.
    def fetch(offset: Long): IO[FetchResponse.PartitionData] = {
      val fetchHash = new util.LinkedHashMap[TopicPartition, PartitionData]()
      fetchHash.put(partition, new FetchRequest.PartitionData(offset, FetchRequest.INVALID_LOG_START_OFFSET, 100000))
      client.fetch(FetchRequest.Builder.forConsumer(0, 1, fetchHash))
        .flatMap { response =>
          val partitionResponse = response.responseData.get(partition)
          Option(partitionResponse.error.exception)
            .map(IO.raiseError)
            .getOrElse(IO.pure(partitionResponse))
        }
    }

    def streamFrom(offset: Long): Stream[IO, Record] = {
      for {
        response <- Stream.eval(fetch(offset))
        record <- Stream.emits(response.records.records.asScala.toVector) ++ streamFrom(offset)
      } yield record
    }

    streamFrom(0L)
  }
}
