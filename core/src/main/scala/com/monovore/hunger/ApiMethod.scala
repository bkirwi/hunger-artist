package com.monovore.hunger

import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests._

sealed abstract class ApiMethod[Request <: AbstractRequest] { method =>
  type Response <: AbstractResponse

  def castResponse(someResponse: AbstractResponse): Option[Response]

  final object Response {
    def unapply(someResponse: AbstractResponse): Option[Response] = method.castResponse(someResponse)
  }
}

object ApiMethod {

  // Start generated code

  implicit object Produce extends ApiMethod[ProduceRequest] {
    type Response = ProduceResponse

    override def castResponse(someResponse: AbstractResponse): Option[ProduceResponse] = someResponse match {
      case expected: ProduceResponse => Some(expected)
      case _ => None
    }
  }

  implicit object Fetch extends ApiMethod[FetchRequest] {
    type Response = FetchResponse

    override def castResponse(someResponse: AbstractResponse): Option[FetchResponse] = someResponse match {
      case expected: FetchResponse => Some(expected)
      case _ => None
    }
  }

  implicit object ListOffset extends ApiMethod[ListOffsetRequest] {
    type Response = ListOffsetResponse

    override def castResponse(someResponse: AbstractResponse): Option[ListOffsetResponse] = someResponse match {
      case expected: ListOffsetResponse => Some(expected)
      case _ => None
    }
  }

  implicit object Metadata extends ApiMethod[MetadataRequest] {
    type Response = MetadataResponse

    override def castResponse(someResponse: AbstractResponse): Option[MetadataResponse] = someResponse match {
      case expected: MetadataResponse => Some(expected)
      case _ => None
    }
  }

  implicit object OffsetCommit extends ApiMethod[OffsetCommitRequest] {
    type Response = OffsetCommitResponse

    override def castResponse(someResponse: AbstractResponse): Option[OffsetCommitResponse] = someResponse match {
      case expected: OffsetCommitResponse => Some(expected)
      case _ => None
    }
  }

  implicit object OffsetFetch extends ApiMethod[OffsetFetchRequest] {
    type Response = OffsetFetchResponse

    override def castResponse(someResponse: AbstractResponse): Option[OffsetFetchResponse] = someResponse match {
      case expected: OffsetFetchResponse => Some(expected)
      case _ => None
    }
  }

  implicit object FindCoordinator extends ApiMethod[FindCoordinatorRequest] {
    type Response = FindCoordinatorResponse

    override def castResponse(someResponse: AbstractResponse): Option[FindCoordinatorResponse] = someResponse match {
      case expected: FindCoordinatorResponse => Some(expected)
      case _ => None
    }
  }

  implicit object JoinGroup extends ApiMethod[JoinGroupRequest] {
    type Response = JoinGroupResponse

    override def castResponse(someResponse: AbstractResponse): Option[JoinGroupResponse] = someResponse match {
      case expected: JoinGroupResponse => Some(expected)
      case _ => None
    }
  }

  implicit object Heartbeat extends ApiMethod[HeartbeatRequest] {
    type Response = HeartbeatResponse

    override def castResponse(someResponse: AbstractResponse): Option[HeartbeatResponse] = someResponse match {
      case expected: HeartbeatResponse => Some(expected)
      case _ => None
    }
  }

  implicit object LeaveGroup extends ApiMethod[LeaveGroupRequest] {
    type Response = LeaveGroupResponse

    override def castResponse(someResponse: AbstractResponse): Option[LeaveGroupResponse] = someResponse match {
      case expected: LeaveGroupResponse => Some(expected)
      case _ => None
    }
  }

  implicit object SyncGroup extends ApiMethod[SyncGroupRequest] {
    type Response = SyncGroupResponse

    override def castResponse(someResponse: AbstractResponse): Option[SyncGroupResponse] = someResponse match {
      case expected: SyncGroupResponse => Some(expected)
      case _ => None
    }
  }

  implicit object DescribeGroups extends ApiMethod[DescribeGroupsRequest] {
    type Response = DescribeGroupsResponse

    override def castResponse(someResponse: AbstractResponse): Option[DescribeGroupsResponse] = someResponse match {
      case expected: DescribeGroupsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object ListGroups extends ApiMethod[ListGroupsRequest] {
    type Response = ListGroupsResponse

    override def castResponse(someResponse: AbstractResponse): Option[ListGroupsResponse] = someResponse match {
      case expected: ListGroupsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object SaslHandshake extends ApiMethod[SaslHandshakeRequest] {
    type Response = SaslHandshakeResponse

    override def castResponse(someResponse: AbstractResponse): Option[SaslHandshakeResponse] = someResponse match {
      case expected: SaslHandshakeResponse => Some(expected)
      case _ => None
    }
  }

  implicit object ApiVersions extends ApiMethod[ApiVersionsRequest] {
    type Response = ApiVersionsResponse

    override def castResponse(someResponse: AbstractResponse): Option[ApiVersionsResponse] = someResponse match {
      case expected: ApiVersionsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object CreateTopics extends ApiMethod[CreateTopicsRequest] {
    type Response = CreateTopicsResponse

    override def castResponse(someResponse: AbstractResponse): Option[CreateTopicsResponse] = someResponse match {
      case expected: CreateTopicsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object DeleteTopics extends ApiMethod[DeleteTopicsRequest] {
    type Response = DeleteTopicsResponse

    override def castResponse(someResponse: AbstractResponse): Option[DeleteTopicsResponse] = someResponse match {
      case expected: DeleteTopicsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object DeleteRecords extends ApiMethod[DeleteRecordsRequest] {
    type Response = DeleteRecordsResponse

    override def castResponse(someResponse: AbstractResponse): Option[DeleteRecordsResponse] = someResponse match {
      case expected: DeleteRecordsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object InitProducerId extends ApiMethod[InitProducerIdRequest] {
    type Response = InitProducerIdResponse

    override def castResponse(someResponse: AbstractResponse): Option[InitProducerIdResponse] = someResponse match {
      case expected: InitProducerIdResponse => Some(expected)
      case _ => None
    }
  }

  implicit object AddPartitionsToTxn extends ApiMethod[AddPartitionsToTxnRequest] {
    type Response = AddPartitionsToTxnResponse

    override def castResponse(someResponse: AbstractResponse): Option[AddPartitionsToTxnResponse] = someResponse match {
      case expected: AddPartitionsToTxnResponse => Some(expected)
      case _ => None
    }
  }

  implicit object AddOffsetsToTxn extends ApiMethod[AddOffsetsToTxnRequest] {
    type Response = AddOffsetsToTxnResponse

    override def castResponse(someResponse: AbstractResponse): Option[AddOffsetsToTxnResponse] = someResponse match {
      case expected: AddOffsetsToTxnResponse => Some(expected)
      case _ => None
    }
  }

  implicit object EndTxn extends ApiMethod[EndTxnRequest] {
    type Response = EndTxnResponse

    override def castResponse(someResponse: AbstractResponse): Option[EndTxnResponse] = someResponse match {
      case expected: EndTxnResponse => Some(expected)
      case _ => None
    }
  }

  implicit object TxnOffsetCommit extends ApiMethod[TxnOffsetCommitRequest] {
    type Response = TxnOffsetCommitResponse

    override def castResponse(someResponse: AbstractResponse): Option[TxnOffsetCommitResponse] = someResponse match {
      case expected: TxnOffsetCommitResponse => Some(expected)
      case _ => None
    }
  }

  implicit object DescribeAcls extends ApiMethod[DescribeAclsRequest] {
    type Response = DescribeAclsResponse

    override def castResponse(someResponse: AbstractResponse): Option[DescribeAclsResponse] = someResponse match {
      case expected: DescribeAclsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object CreateAcls extends ApiMethod[CreateAclsRequest] {
    type Response = CreateAclsResponse

    override def castResponse(someResponse: AbstractResponse): Option[CreateAclsResponse] = someResponse match {
      case expected: CreateAclsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object DeleteAcls extends ApiMethod[DeleteAclsRequest] {
    type Response = DeleteAclsResponse

    override def castResponse(someResponse: AbstractResponse): Option[DeleteAclsResponse] = someResponse match {
      case expected: DeleteAclsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object DescribeConfigs extends ApiMethod[DescribeConfigsRequest] {
    type Response = DescribeConfigsResponse

    override def castResponse(someResponse: AbstractResponse): Option[DescribeConfigsResponse] = someResponse match {
      case expected: DescribeConfigsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object AlterConfigs extends ApiMethod[AlterConfigsRequest] {
    type Response = AlterConfigsResponse

    override def castResponse(someResponse: AbstractResponse): Option[AlterConfigsResponse] = someResponse match {
      case expected: AlterConfigsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object AlterReplicaLogDirs extends ApiMethod[AlterReplicaLogDirsRequest] {
    type Response = AlterReplicaLogDirsResponse

    override def castResponse(someResponse: AbstractResponse): Option[AlterReplicaLogDirsResponse] = someResponse match {
      case expected: AlterReplicaLogDirsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object DescribeLogDirs extends ApiMethod[DescribeLogDirsRequest] {
    type Response = DescribeLogDirsResponse

    override def castResponse(someResponse: AbstractResponse): Option[DescribeLogDirsResponse] = someResponse match {
      case expected: DescribeLogDirsResponse => Some(expected)
      case _ => None
    }
  }

  implicit object SaslAuthenticate extends ApiMethod[SaslAuthenticateRequest] {
    type Response = SaslAuthenticateResponse

    override def castResponse(someResponse: AbstractResponse): Option[SaslAuthenticateResponse] = someResponse match {
      case expected: SaslAuthenticateResponse => Some(expected)
      case _ => None
    }
  }

  implicit object CreatePartitions extends ApiMethod[CreatePartitionsRequest] {
    type Response = CreatePartitionsResponse

    override def castResponse(someResponse: AbstractResponse): Option[CreatePartitionsResponse] = someResponse match {
      case expected: CreatePartitionsResponse => Some(expected)
      case _ => None
    }
  }

  // End generated code

  def main(args: Array[String]): Unit = {

    for (value <- ApiKeys.values if !value.clusterAction) {

      val name = value.name match {
        case "ListOffsets" => "ListOffset"
        case "OffsetForLeaderEpoch" => "OffsetsForLeaderEpoch"
        case other => other
      }

      println(
        s"""  implicit object $name extends ApiMethod[${name}Request] {
          |    type Response = ${name}Response
          |
          |    override def castResponse(someResponse: AbstractResponse): Option[${name}Response] = someResponse match {
          |      case expected: ${name}Response => Some(expected)
          |      case _ => None
          |    }
          |  }
        """.stripMargin
      )
    }
  }
}
