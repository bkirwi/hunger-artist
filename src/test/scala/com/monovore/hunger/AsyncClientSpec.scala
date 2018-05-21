package com.monovore.hunger

import java.net.InetSocketAddress

import cats.effect.IO
import com.monovore.hunger
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.RoundRobinAssignor
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails
import org.apache.kafka.common.requests.{CreateTopicsRequest, MetadataRequest}
import org.apache.kafka.common.utils.Time
import org.scalatest.WordSpec

import scala.collection.JavaConverters._

class AsyncClientSpec extends WordSpec with EmbeddedKafka {

  "A communicator" should {

    val autoKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

    "do a thing" in {

      withRunningKafkaOnFoundPort(autoKafkaConfig) { implicit realConfig =>

        val brokers = List(new InetSocketAddress("localhost", realConfig.kafkaPort))
        val networkClient = AsyncClient.networkClient(brokers, AsyncClient.Config("client.id"))

        val client = new AsyncClient(networkClient, Time.SYSTEM)

        def printIO(a: Any) = IO { println(a.toString)}
        new Thread(client).start()
        val allTopicsRequest = MetadataRequest.Builder.allTopics()

        val done = for {
          meta <- client.sendUnchecked(Node.noNode, allTopicsRequest)
          _ <- printIO(meta.topicMetadata())
          createTopicsRequest: CreateTopicsRequest.Builder = new CreateTopicsRequest.Builder(Map("test-topic" -> new TopicDetails(2, 1.toShort)).asJava, 10000)
          created <- client.sendUnchecked(Node.noNode, createTopicsRequest)
          _ <- printIO(created.errors())
          meta <- client.sendUnchecked(Node.noNode, allTopicsRequest)
          _ <- printIO(meta.topicMetadata.asScala.map { _.topic })
          groupClient = GroupClient(client, "foosball")
          protocol = new GroupClient.Partitioned(client, new RoundRobinAssignor, Set("test-topic"), printIO)
          done <- groupClient.runGroup(protocol)
        } yield done

        try {
          done.unsafeRunSync()
        } finally {
          client.close()
        }
      }
    }
  }

}
