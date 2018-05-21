package com.monovore.hunger

import java.io.Closeable
import java.net.InetSocketAddress
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import cats.effect.IO
import com.monovore.hunger.AsyncClient.Message
import org.apache.kafka.clients._
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilder, PlaintextChannelBuilder, Selector}
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{Cluster, Node}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object AsyncClient {

  case class Message(node: Node, builder: AbstractRequest.Builder[_ <: AbstractRequest], callback: RequestCompletionHandler)

  case class Config(
    clientId: String,
    requestTimeoutMs: Int = 30000,
    metadataRetryBackoffMs: Long = 100L,
    connectionsMaxIdleMs: Long = 60 * 1000L,
    metadataExpireMs: Long = 5 * 60 * 1000L,
    reconnectBackoffMs: Long = 500L,
    reconnectBackoffMaxMs: Long = 1000L,
    sendBuffer: Int = 100 * 1000,
    receiveBuffer: Int = 100 * 1000
  )

  def networkClient(
    brokers: List[InetSocketAddress],
    config: Config
  ): KafkaClient = {

    import config._

    val logContext = new LogContext("[Async clientId=" + clientId + "] ")

    val time = Time.SYSTEM

    val metrics = new Metrics()

    val metadata = new Metadata(metadataRetryBackoffMs, metadataExpireMs, false, false, new ClusterResourceListeners)
    val addresses = brokers.asJava
    metadata.update(Cluster.bootstrap(addresses), Collections.emptySet[String], 0)
    val metricGrpPrefix = "async"
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
class AsyncClient(kafka: KafkaClient, time: Time = Time.SYSTEM) extends Runnable with Closeable {

  private[this] val log = LoggerFactory.getLogger(getClass)

  @volatile
  private[this] var closed: Boolean = false
  private[this] val awaitClosed: CountDownLatch = new CountDownLatch(1)

  private[this] val buffers: ConcurrentHashMap[Node, ArrayBuffer[Message]] = new ConcurrentHashMap()

  def send[A <: AbstractRequest](request: AbstractRequest.Builder[A])(implicit method: ApiMethod[A]): IO[method.Response] =
    send(Node.noNode, request)

  def sendUnchecked[A <: AbstractRequest](request: AbstractRequest.Builder[A])(implicit method: ApiMethod[A]): IO[method.Response] =
    sendUnchecked(Node.noNode, request)

  def send[A <: AbstractRequest](node: Node, request: AbstractRequest.Builder[A])(implicit method: ApiMethod[A]): IO[method.Response] =
    for {
      response <- sendUnchecked(node, request)
      _ <- KafkaUtils.checkErrors(response)
    } yield response

  def sendUnchecked[A <: AbstractRequest](node: Node, request: AbstractRequest.Builder[A])(implicit method: ApiMethod[A]): IO[method.Response] =
    IO.async { callback =>
      doSend(Node.noNode, request, (response) => {
        val result =
          if (response.wasDisconnected) {
            Left(DisconnectException.INSTANCE)
          }
          else if (response.hasResponse) response.responseBody match {
            case method.Response(expected) => Right(expected)
            case unexpected => Left(new MatchError(s"Got unexpected response type: ${unexpected.getClass}"))
          }
          else if (response.versionMismatch != null) Left(response.versionMismatch)
          else Left(new IllegalArgumentException("HUH"))

        callback(result)
      })
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

      // Why is this sensitive to ~anything?
      kafka.poll(2000L, currentMs)
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
