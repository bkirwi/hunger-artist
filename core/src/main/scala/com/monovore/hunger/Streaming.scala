package com.monovore.hunger

import java.nio.ByteBuffer
import java.util

import cats.effect.IO
import fs2._
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests._

import scala.collection.JavaConverters._

object Streaming {

  def consume(
    client: AsyncClient,
    partition: TopicPartition
  ): Stream[IO, Record] = {

    // Fetch a single batch, starting at the given offset, from the given partition.
    def fetch(node: Node, offset: Long): IO[FetchResponse.PartitionData] = {
      val fetchHash = new util.LinkedHashMap[TopicPartition, PartitionData]()
      fetchHash.put(partition, new FetchRequest.PartitionData(offset, FetchRequest.INVALID_LOG_START_OFFSET, 100000))
      client.sendUnchecked(node, FetchRequest.Builder.forConsumer(0, 1, fetchHash))
        .flatMap { response =>
          val partitionResponse = response.responseData.get(partition)
          Option(partitionResponse.error.exception)
            .map(IO.raiseError)
            .getOrElse(IO.pure(partitionResponse))
        }
    }

    def streamFrom(node: Node, offset: Long): Stream[IO, Record] = {
      for {
        response <- Stream.eval(fetch(node, offset))
        record <- Stream.emits(response.records.records.asScala.toVector) ++ streamFrom(node, offset)
      } yield record
    }

    for {
      response <- Stream.eval(client.send(new MetadataRequest.Builder(List(partition.topic).asJava, false)))
      leader = response.cluster.leaderFor(partition)
      message <- streamFrom(leader, 0L)
    } yield message
  }
}
