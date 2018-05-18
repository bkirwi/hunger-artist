package com.monovore.hunger

import java.io.Closeable
import java.net.InetSocketAddress
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import cats.effect.{Async, IO}
import com.monovore.hunger.AsyncClient.Message
import org.apache.kafka.clients._
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilder, PlaintextChannelBuilder, Selector}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{Cluster, Node}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object AsyncClient {

  case class Message(node: Node, builder: AbstractRequest.Builder[_ <: AbstractRequest], callback: RequestCompletionHandler)

  case class Config(
    clientId: String,
    requestTimeoutMs: Int = 1000,
    metadataRetryBackoffMs: Long = 100L,
    connectionsMaxIdleMs: Long = 100L,
    metadataExpireMs: Long = 100L,
    reconnectBackoffMs: Long = 100L,
    reconnectBackoffMaxMs: Long = 100L,
    sendBuffer: Int = 100 * 1000,
    receiveBuffer: Int = 100 * 1000
  )

  def networkClient(
    brokers: List[InetSocketAddress],
    config: Config
  ): KafkaClient = {

    import config._

    val logContext = new LogContext("[Communicator clientId=" + clientId + "] ")

    val time = Time.SYSTEM

    val metrics = new Metrics()

    val metadata = new Metadata(metadataRetryBackoffMs, metadataExpireMs, false, false, new ClusterResourceListeners)
    val addresses = brokers.asJava
    metadata.update(Cluster.bootstrap(addresses), Collections.emptySet[String], 0)
    val metricGrpPrefix = "consumer"
    val channelBuilder: ChannelBuilder = new PlaintextChannelBuilder
    channelBuilder.configure(Map.empty[String, Any].asJava)

    val throttleTimeSensor = metrics.sensor("throttle-time-ms")

    new NetworkClient(
      new Selector(
        connectionsMaxIdleMs,
        metrics,
        time,
        metricGrpPrefix,
        channelBuilder,
        logContext
      ),
      metadata,
      clientId,
      100, // Hardcoded max-in-flight!
      reconnectBackoffMs,
      reconnectBackoffMaxMs,
      sendBuffer,
      receiveBuffer,
      requestTimeoutMs,
      time,
      true, // Discover broker versions.
      new ApiVersions,
      throttleTimeSensor,
      logContext
    )
  }
}

/**
 * An event loop wrapped around the standard Kafka network client.
 *
 */
class AsyncClient(val kafka: KafkaClient, val time: Time) extends Runnable with Closeable {

  @volatile
  private[this] var closed: Boolean = false
  private[this] val awaitClosed: CountDownLatch = new CountDownLatch(1)

  private[this] val buffers: ConcurrentHashMap[Node, ArrayBuffer[Message]] = new ConcurrentHashMap()

  def send[A <: AbstractRequest](node: Node, request: AbstractRequest.Builder[A])(implicit ApiMethod: ApiMethod[A]): IO[ApiMethod.Response] = {
    IO.async { callback =>
      doSend(Node.noNode, request, (response) => {
        val result =
          if (response.wasDisconnected) Left(DisconnectException.INSTANCE)
          else if (response.hasResponse) response.responseBody match {
            case ApiMethod.Response(expected) => Right(expected)
            case unexpected => Left(new MatchError(s"Got unexpected response type: ${unexpected.getClass}"))
          }
          else if (response.versionMismatch != null) Left(response.versionMismatch)
          else Left(new IllegalArgumentException("HUH"))

        callback(result)
      })
    }
  }

  private[this] def doSend(node: Node, builder: AbstractRequest.Builder[_ <: AbstractRequest], callback: RequestCompletionHandler): Unit = synchronized {
    val buffer = buffers.computeIfAbsent(node, (_: Node) => new ArrayBuffer[Message]())
    buffer.append(AsyncClient.Message(node, builder, callback))
    kafka.wakeup()
  }

  override def run(): Unit = try {

    while (!closed) {

      val currentMs = time.milliseconds()

      synchronized {
        for ((nodeOption, buffer) <- buffers.asScala) {

          val node = if (nodeOption.isEmpty) kafka.leastLoadedNode(currentMs) else nodeOption

          if (node != null && kafka.ready(node, currentMs)) {
            for (message <- buffer) {
              val request = kafka.newClientRequest(node.idString, message.builder, currentMs, true, message.callback)
              kafka.send(request, currentMs)
            }
            buffer.clear()
          }
        }
      }

      kafka.poll(1000L, currentMs)
    }
  } finally {
    kafka.close()
    awaitClosed.countDown()
  }

  override def close(): Unit = {
    closed = true
    awaitClosed.await()
  }
}
