package com.monovore.hunger

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Collections

import com.monovore.hunger.Client.Communicator
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.PlaintextChannelBuilder
import org.apache.kafka.common.record.{CompressionType, MemoryRecords}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.{ListOffsetRequest, FetchRequest, MetadataRequest, ProduceRequest}
import org.apache.kafka.common.utils.SystemTime

import scala.collection.JavaConverters._

object TestScript {

  def main(args: Array[String]): Unit = {

    val bootstrap = List(new InetSocketAddress(9092)).asJava
    val kafka = Client.fromWhatever(bootstrap, new PlaintextChannelBuilder, new SystemTime, new Metrics)
    val communicator = new Communicator(kafka, new SystemTime)
    val thread = new Thread(communicator)
    thread.setDaemon(true)
    thread.start()
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
  }
}
