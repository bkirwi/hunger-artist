package com.monovore.hunger

import cats.Functor
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.requests._
import cats.implicits._

class ApiClient[F[_]: Functor](send: (ApiKeys, Struct) => F[Struct]) {

  def produce(request: ProduceRequest): F[ProduceResponse] =
    send(ApiKeys.PRODUCE, request.toStruct).map(new ProduceResponse(_))

  def fetch(request: FetchRequest): F[FetchResponse] =
    send(ApiKeys.FETCH, request.toStruct).map(new FetchResponse(_))

  def listOffsets(request: ListOffsetRequest): F[ListOffsetResponse] =
    send(ApiKeys.LIST_OFFSETS, request.toStruct).map(new ListOffsetResponse(_))

  def metadata(request: MetadataRequest): F[MetadataResponse] =
    send(ApiKeys.METADATA, request.toStruct).map(new MetadataResponse(_))

  def offsetCommit(request: OffsetCommitRequest): F[OffsetCommitResponse] =
    send(ApiKeys.OFFSET_COMMIT, request.toStruct).map(new OffsetCommitResponse(_))

  def offsetFetch(request: OffsetFetchRequest): F[OffsetFetchResponse] =
    send(ApiKeys.OFFSET_FETCH, request.toStruct).map(new OffsetFetchResponse(_))

  def groupCoordinator(request: GroupCoordinatorRequest): F[GroupCoordinatorResponse] =
    send(ApiKeys.GROUP_COORDINATOR, request.toStruct).map(new GroupCoordinatorResponse(_))

  def joinGroup(request: JoinGroupRequest): F[JoinGroupResponse] =
    send(ApiKeys.JOIN_GROUP, request.toStruct).map(new JoinGroupResponse(_))

  def heartbeat(request: HeartbeatRequest): F[HeartbeatResponse] =
    send(ApiKeys.HEARTBEAT, request.toStruct).map(new HeartbeatResponse(_))

  def leaveGroup(request: LeaveGroupRequest): F[LeaveGroupResponse] =
    send(ApiKeys.LEAVE_GROUP, request.toStruct).map(new LeaveGroupResponse(_))

  def syncGroup(request: SyncGroupRequest): F[SyncGroupResponse] =
    send(ApiKeys.SYNC_GROUP, request.toStruct).map(new SyncGroupResponse(_))

  def describeGroups(request: DescribeGroupsRequest): F[DescribeGroupsResponse] =
    send(ApiKeys.DESCRIBE_GROUPS, request.toStruct).map(new DescribeGroupsResponse(_))

  def listGroups(request: ListGroupsRequest): F[ListGroupsResponse] =
    send(ApiKeys.LIST_GROUPS, request.toStruct).map(new ListGroupsResponse(_))

  def createTopics(request: CreateTopicsRequest): F[CreateTopicsResponse] =
    send(ApiKeys.CREATE_TOPICS, request.toStruct).map(new CreateTopicsResponse(_))

  def deleteTopics(request: DeleteTopicsRequest): F[DeleteTopicsResponse] =
    send(ApiKeys.DELETE_TOPICS, request.toStruct).map(new DeleteTopicsResponse(_))
}
