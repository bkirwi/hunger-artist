package com.monovore.hunger

import java.io.Closeable
import java.util
import java.util.concurrent.LinkedBlockingQueue

import cats.effect.Async
import org.apache.kafka.clients.{ClientRequest, ClientResponse, KafkaClient}
import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{BrokerNotAvailableException, DisconnectException}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.requests.RequestSend
import org.apache.kafka.common.utils.Time

import scala.annotation.tailrec

import scala.collection.JavaConverters._

object Communicator {

  private[Communicator] val PollTimeoutMs = 1000L

  case class Outbound(destination: Option[Node], key: ApiKeys, request: Struct, callback: Either[Throwable, Struct] => Unit)
}

/**
  * An event loop wrapped around the standard Kafka network client.
  * 
  */
class Communicator(val kafka: KafkaClient, val time: Time) extends Runnable with Closeable {

  private[this] val outbound: LinkedBlockingQueue[Communicator.Outbound] = new LinkedBlockingQueue[Communicator.Outbound]

  private[this] var running: Boolean = true

  def anyone[F[_]: Async] = new ApiClient[F]((key: ApiKeys, request: Struct) => send(None, key, request))

  def node[F[_]: Async](node: Node) = new ApiClient[F]((key: ApiKeys, request: Struct) => send(Some(node), key, request))

  private def send[F[_]](
    destination: Option[Node],
    key: ApiKeys,
    request: Struct
  )(implicit
    A: Async[F]
  ) = A.async[Struct] { callback =>
    if (!running) {
      callback(Left(new IllegalStateException("Client has been closed!")))
    }
    try
      this.outbound.put(Communicator.Outbound(destination, key, request, callback))
    catch {
      case e: InterruptedException =>
        callback(Left(e))
        Thread.currentThread.interrupt()
    }
    this.kafka.wakeup()
  }

  override def close(): Unit = {
    running = false
    kafka.close()
  }

  def run() = {

    @tailrec
    def drain(now: Long): Unit = {
      val arrayList = new util.ArrayList[Communicator.Outbound]()
      val numberToSend = outbound.drainTo(arrayList)

      for (message <- arrayList.iterator.asScala) {
        val destination = message.destination.getOrElse(kafka.leastLoadedNode(now))
        if (destination == null) Left(new BrokerNotAvailableException("No brokers available!"))
        if (!kafka.isReady(destination, now)) {
          message.callback(Left(new BrokerNotAvailableException("Connection not ready to node: " + destination.idString)))
            continue //todo: continue is not supported
        }
        val header = kafka.nextRequestHeader(message.key)
        val send = new RequestSend(destination.idString, header, message.request)
        val request = new ClientRequest(now, true, send, (response: ClientResponse) => {
          def foo(response: ClientResponse) = if (response.wasDisconnected) message.callback(Left((new DisconnectException("Disconnected from node: " + destination.idString))
          else {
            val body = response.responseBody
            message.responseFuture.complete(body)
          }

          foo(response)
        })
        kafka.send(request, now)
      }

      Option(outbound.poll()) match {
        case None => ()
        case Some(message) =>
          message.destination.orElse(Option(kafka.leastLoadedNode(now))) match {
            case None =>
              message.callback(Left(new NoAvailableBrokersException()))
            case Some(destination) =>
          }
      }
    }

    while (running) {
      val now = time.milliseconds
      var next: Communicator.Outbound = null
      while ((next = outbound.poll()) != null) {
      val message = next
        val destination = message.destination.getOrElse(kafka.leastLoadedNode(now))
        if (destination == null) {
          message.callback(Left((new BrokerNotAvailableException("No brokers available!"))
          continue //todo: continue is not supported
        }
        if (!kafka.ready(destination, now)) kafka.poll(Communicator.PollTimeoutMs, now)
        if (!kafka.isReady(destination, now)) {
          message.callback(Left((new BrokerNotAvailableException("Connection not ready to node: " + destination.idString))
          continue //todo: continue is not supported
        }
        val header = kafka.nextRequestHeader(message.key)
        val send = new RequestSend(destination.idString, header, message.request)
        val request = new ClientRequest(now, true, send, (response: ClientResponse) => {
          def foo(response: ClientResponse) = if (response.wasDisconnected) message.callback(Left((new DisconnectException("Disconnected from node: " + destination.idString))
          else {
            val body = response.responseBody
            message.responseFuture.complete(body)
          }

          foo(response)
        })
        kafka.send(request, now)
      }
      // TODO: deal with exceptions here
      kafka.poll(Communicator.PollTimeoutMs, now)
    }

    ???
  }
}
