package com.monovore.hunger

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Collections

import com.monovore.hunger.GroupClient.Meta
import com.monovore.hunger._
import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.clients.consumer.internals.PartitionAssignor
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.PlaintextChannelBuilder
import org.apache.kafka.common.record.{CompressionType, MemoryRecords}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.SystemTime

import scala.collection.JavaConverters._

object TestScript {

  def main(args: Array[String]): Unit = {

    val bootstrap = List(new InetSocketAddress(9092)).asJava
    val time = new SystemTime
    val metrics = new Metrics
    val kafka = Client.fromWhatever(bootstrap, new PlaintextChannelBuilder, time, metrics)
    val communicator = new Communicator(kafka, time)
    val client = new Client(communicator)

    val records = MemoryRecords.emptyRecords(ByteBuffer.allocate(1000000), CompressionType.NONE)
    records.append(0L, Array.empty[Byte], Array.empty[Byte])
    records.close()

    val partition = new TopicPartition("foo", 0)

    val buffer = records.buffer()
    val result = client.metadata(new MetadataRequest(Collections.singletonList("foo"))).get()
    println(result)
    val produced = client.produce(new ProduceRequest(-1, 1000, Map(partition -> buffer).asJava)).get()
    println(produced)
    val fetched = client.fetch(new FetchRequest(100, 0, Map(partition -> new PartitionData(0, 1000000)).asJava)).get()
    println(fetched)
    val offsets = client.listOffsets(new ListOffsetRequest(
      Map(partition -> new ListOffsetRequest.PartitionData(-1, 1)).asJava
    )).get()
    println(offsets)
    val commit = client.offsetCommit(new OffsetCommitRequest("bug",
      OffsetCommitRequest.DEFAULT_GENERATION_ID,
      OffsetCommitRequest.DEFAULT_MEMBER_ID,
      OffsetCommitRequest.DEFAULT_RETENTION_TIME,
      Map(partition -> new OffsetCommitRequest.PartitionData(1L, "hello")).asJava
    )).get()
    println(commit)
    val fetch = client.offsetFetch(new OffsetFetchRequest("bug", List(partition).asJava)).get()
    println(fetch)

    val meta = new Meta("other-group", 6000)
    val groupClient =
      communicator.group(meta).get()
        .join(Set("test").asJava, List[PartitionAssignor](new RangeAssignor).asJava).get()

    println(groupClient.assignment().partitions())

    groupClient.heartbeat().get()
    groupClient.leave().get()
  }
}
