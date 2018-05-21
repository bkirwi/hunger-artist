package com.monovore.hunger

import java.nio.ByteBuffer

import cats.effect.IO
import com.monovore.hunger.GroupClient.Generation
import org.apache.kafka.clients.consumer.internals.{ConsumerProtocol, PartitionAssignor}
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import org.apache.kafka.common.requests._

object GroupClient {

  case class Generation(coordinator: Node, memberId: String, generationId: Int)

  trait Assignor {
    def name: String
    def subscription: ByteBuffer
    def membership(members: Map[String, ByteBuffer]): IO[Map[String, ByteBuffer]]
    def action(generation: Generation, assignment: ByteBuffer): IO[Unit]
  }

  class Partitioned(
    client: AsyncClient,
    assignor: PartitionAssignor,
    topics: Set[String],
    assignmentAction: PartitionAssignor.Assignment => IO[Unit]
  ) extends Assignor {

    def name: String = assignor.name

    def subscription: ByteBuffer = ConsumerProtocol.serializeSubscription(assignor.subscription(topics.asJava))

    def membership(members: Map[String, ByteBuffer]): IO[Map[String, ByteBuffer]] = {
      for {
        meta <- client.sendUnchecked(new MetadataRequest.Builder(topics.toList.asJava, false))
      } yield {
        val deserialized = members.mapValues(ConsumerProtocol.deserializeSubscription).toList.toMap
        val assignment = assignor.assign(meta.cluster(), deserialized.asJava).asScala
        assignment.mapValues(ConsumerProtocol.serializeAssignment).toList.toMap
      }
    }

    override def action(generation: Generation, assignment: ByteBuffer): IO[Unit] = {
      assignmentAction(ConsumerProtocol.deserializeAssignment(assignment))
    }
  }
}

case class GroupClient(client: AsyncClient, groupId: String) {

  def heartbeat(generation: Generation): IO[Unit] =
    client.send(generation.coordinator, new HeartbeatRequest.Builder(groupId, generation.generationId, generation.memberId))
      .map { _ => () }

  def commitOffsets(generation: Generation, offsets: Map[TopicPartition, OffsetCommitRequest.PartitionData]): IO[Unit] = {
    val request = new OffsetCommitRequest.Builder(groupId, offsets.asJava)
    request.setGenerationId(generation.generationId)
    request.setMemberId(generation.memberId)
    client.send(generation.coordinator, request).map { _ => () }
  }

  def runGroup(assignor: GroupClient.Assignor): IO[Unit] = {

    for {
      response <- KafkaUtils.retryForever {
        client.send(new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, groupId))
      }
      coordinator = response.node
      response <-
        client.send(coordinator,
          new JoinGroupRequest.Builder(
            groupId, 10000, JoinGroupRequest.UNKNOWN_MEMBER_ID, "consumer", List(new JoinGroupRequest.ProtocolMetadata(assignor.name, assignor.subscription)).asJava
          )
        )
      generationId = response.generationId
      memberId = response.memberId
      membership <-
        if (!response.members.isEmpty) {
          // I'm the leader!
          assignor.membership(response.members.asScala.toMap)
        }
        else IO.pure(Map.empty[String, ByteBuffer])
      response <- client.send(coordinator,
        new SyncGroupRequest.Builder(groupId, generationId, memberId, membership.asJava)
      )
      result <- assignor.action(GroupClient.Generation(coordinator, memberId, generationId), response.memberAssignment)
      leave <- client.sendUnchecked(coordinator,
        new LeaveGroupRequest.Builder(groupId, memberId)
      )
    } yield result
  }
}
