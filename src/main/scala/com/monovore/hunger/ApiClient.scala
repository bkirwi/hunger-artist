package com.monovore.hunger

import cats.Functor
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.requests._
import cats.implicits._

class ApiClient[F[_]: Functor](send: AbstractRequest.Builder[_ <: AbstractRequest] => F[AbstractResponse]) {

  def produce(request: ProduceRequest.Builder): F[ProduceResponse] =
    send(request).asInstanceOf[F[ProduceResponse]]

  def fetch(request: FetchRequest.Builder): F[FetchResponse] =
    send(request).asInstanceOf[F[FetchResponse]]

  def listOffsets(request: ListOffsetRequest.Builder): F[ListOffsetResponse] =
    send(request).asInstanceOf[F[ListOffsetResponse]]

  def metadata(request: MetadataRequest.Builder): F[MetadataResponse] =
    send(request).asInstanceOf[F[MetadataResponse]]

  def offsetCommit(request: OffsetCommitRequest.Builder): F[OffsetCommitResponse] =
    send(request).asInstanceOf[F[OffsetCommitResponse]]

  def offsetFetch(request: OffsetFetchRequest.Builder): F[OffsetFetchResponse] =
    send(request).asInstanceOf[F[OffsetFetchResponse]]

  def findCoordinator(request: FindCoordinatorRequest.Builder): F[FindCoordinatorResponse] =
    send(request).asInstanceOf[F[FindCoordinatorResponse]]

  def joinGroup(request: JoinGroupRequest.Builder): F[JoinGroupResponse] =
    send(request).asInstanceOf[F[JoinGroupResponse]]

  def heartbeat(request: HeartbeatRequest.Builder): F[HeartbeatResponse] =
    send(request).asInstanceOf[F[HeartbeatResponse]]

  def leaveGroup(request: LeaveGroupRequest.Builder): F[LeaveGroupResponse] =
    send(request).asInstanceOf[F[LeaveGroupResponse]]

  def syncGroup(request: SyncGroupRequest.Builder): F[SyncGroupResponse] =
    send(request).asInstanceOf[F[SyncGroupResponse]]

  def describeGroups(request: DescribeGroupsRequest.Builder): F[DescribeGroupsResponse] =
    send(request).asInstanceOf[F[DescribeGroupsResponse]]

  def listGroups(request: ListGroupsRequest.Builder): F[ListGroupsResponse] =
    send(request).asInstanceOf[F[ListGroupsResponse]]

  def createTopics(request: CreateTopicsRequest.Builder): F[CreateTopicsResponse] =
    send(request).asInstanceOf[F[CreateTopicsResponse]]

  def deleteTopics(request: DeleteTopicsRequest.Builder): F[DeleteTopicsResponse] =
    send(request).asInstanceOf[F[DeleteTopicsResponse]]

  def deleteRecords(request: DeleteRecordsRequest.Builder): F[DeleteRecordsResponse] =
    send(request).asInstanceOf[F[DeleteRecordsResponse]]

  def initProducerId(request: InitProducerIdRequest.Builder): F[InitProducerIdResponse] =
    send(request).asInstanceOf[F[InitProducerIdResponse]]

//  def addPartitonsToTxn(request: AddPartitionsToTxnRequest.Builder): F[AddPartitionsToTxnResponse] =
//
//  def addOffsetsToTxn(request: AddOffsetsToTxnRequest.Builder): F[AddOffsetsToTxnResponse] =
//    send(request).asInstanceOf[F[AddOffsetsToTxnResponse]]

//  ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn", false, RecordBatch.MAGIC_VALUE_V2, AddOffsetsToTxnRequest.schemaVersions(),
//    AddOffsetsToTxnResponse.schemaVersions()),
//  END_TXN(26, "EndTxn", false, RecordBatch.MAGIC_VALUE_V2, EndTxnRequest.schemaVersions(),
//    EndTxnResponse.schemaVersions()),
  /*
  ADD_PARTITIONS_TO_TXN(24, "AddPartitionsToTxn", false, RecordBatch.MAGIC_VALUE_V2,
          AddPartitionsToTxnRequest.schemaVersions(), AddPartitionsToTxnResponse.schemaVersions()),
  ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn", false, RecordBatch.MAGIC_VALUE_V2, AddOffsetsToTxnRequest.schemaVersions(),
          AddOffsetsToTxnResponse.schemaVersions()),
  END_TXN(26, "EndTxn", false, RecordBatch.MAGIC_VALUE_V2, EndTxnRequest.schemaVersions(),
          EndTxnResponse.schemaVersions()),
  WRITE_TXN_MARKERS(27, "WriteTxnMarkers", true, RecordBatch.MAGIC_VALUE_V2, WriteTxnMarkersRequest.schemaVersions(),
          WriteTxnMarkersResponse.schemaVersions()),
  TXN_OFFSET_COMMIT(28, "TxnOffsetCommit", false, RecordBatch.MAGIC_VALUE_V2, TxnOffsetCommitRequest.schemaVersions(),
          TxnOffsetCommitResponse.schemaVersions()),
  DESCRIBE_ACLS(29, "DescribeAcls", DescribeAclsRequest.schemaVersions(), DescribeAclsResponse.schemaVersions()),
  CREATE_ACLS(30, "CreateAcls", CreateAclsRequest.schemaVersions(), CreateAclsResponse.schemaVersions()),
  DELETE_ACLS(31, "DeleteAcls", DeleteAclsRequest.schemaVersions(), DeleteAclsResponse.schemaVersions()),
  DESCRIBE_CONFIGS(32, "DescribeConfigs", DescribeConfigsRequest.schemaVersions(),
          DescribeConfigsResponse.schemaVersions()),
  ALTER_CONFIGS(33, "AlterConfigs", AlterConfigsRequest.schemaVersions(),
          AlterConfigsResponse.schemaVersions()),
  ALTER_REPLICA_LOG_DIRS(34, "AlterReplicaLogDirs", AlterReplicaLogDirsRequest.schemaVersions(),
          AlterReplicaLogDirsResponse.schemaVersions()),
  DESCRIBE_LOG_DIRS(35, "DescribeLogDirs", DescribeLogDirsRequest.schemaVersions(),
          DescribeLogDirsResponse.schemaVersions()),
  SASL_AUTHENTICATE(36, "SaslAuthenticate", SaslAuthenticateRequest.schemaVersions(),
          SaslAuthenticateResponse.schemaVersions()),
  CREATE_PARTITIONS(37, "CreatePartitions", CreatePartitionsRequest.schemaVersions(),
          CreatePartitionsResponse.schemaVersions());
 */

}