package com.monovore.hunger

import java.nio.ByteBuffer

import cats.effect.IO
import org.apache.kafka.clients.consumer.internals.{ConsumerProtocol, PartitionAssignor}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import org.apache.kafka.common.requests._

case class GroupClient(client: AsyncClient) {

  def runGroup[A](groupId: String, assignor: PartitionAssignor, topics: Set[String], action: PartitionAssignor.Assignment => IO[A]): IO[A] = {

    def maybeThrow(error: Errors) = IO { error.maybeThrow() }

    for {
      response <- client.send(
        new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, groupId)
      )
      _ = Thread.sleep(1000)
      response <- client.send(
        new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, groupId)
      )
      _ <- maybeThrow(response.error)
      coordinator = response.node
      subscription = ConsumerProtocol.serializeSubscription(assignor.subscription(topics.asJava))
      response <- client.send(coordinator,
        new JoinGroupRequest.Builder(
          groupId, 10000, JoinGroupRequest.UNKNOWN_MEMBER_ID, "consumer", List(new JoinGroupRequest.ProtocolMetadata(assignor.name, subscription)).asJava
        )
      )
      _ <- maybeThrow(response.error)
      generationId = response.generationId
      memberId = response.memberId
      membership <-
        if (!response.members.isEmpty) {
          for {
            meta <- client.send(coordinator, new MetadataRequest.Builder(topics.toList.asJava, false))
          } yield {
            val members =
              response.members.asScala
                .mapValues { value =>
                  ConsumerProtocol.deserializeSubscription(value)
                }
                .toMap

            val assignment = assignor.assign(meta.cluster(), members.asJava).asScala
            assignment.mapValues(ConsumerProtocol.serializeAssignment)
          }
        }
        else IO.pure(Map.empty[String, ByteBuffer])
      response <- client.send(coordinator,
        new SyncGroupRequest.Builder(groupId, generationId, memberId, membership.asJava)
      )
      _ <- maybeThrow(response.error)
      assignment = ConsumerProtocol.deserializeAssignment(response.memberAssignment)
      result <- action(assignment)
      leave <- client.send(coordinator,
        new LeaveGroupRequest.Builder(groupId, memberId)
      )
    } yield result
  }
}
