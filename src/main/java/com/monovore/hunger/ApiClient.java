package com.monovore.hunger;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.*;

import java.util.concurrent.CompletableFuture;

class ApiClient {

    public interface Send {
        CompletableFuture<Struct> send(ApiKeys key, Struct request);
    }

    private final Send send;

    public ApiClient(Send send) {
        this.send = send;
    }

    CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        return send.send(ApiKeys.METADATA, request.toStruct()).thenApply(MetadataResponse::new);
    }

    CompletableFuture<ProduceResponse> produce(ProduceRequest request) {
        return send.send(ApiKeys.PRODUCE, request.toStruct()).thenApply(ProduceResponse::new);
    }

    CompletableFuture<FetchResponse> fetch(FetchRequest request) {
        return send.send(ApiKeys.FETCH, request.toStruct()).thenApply(FetchResponse::new);
    }

    CompletableFuture<ListOffsetResponse> listOffsets(ListOffsetRequest request) {
        return send.send(ApiKeys.LIST_OFFSETS, request.toStruct()).thenApply(ListOffsetResponse::new);
    }

    CompletableFuture<GroupCoordinatorResponse> groupCoordinator(GroupCoordinatorRequest request) {
        return send.send(ApiKeys.GROUP_COORDINATOR, request.toStruct()).thenApply(GroupCoordinatorResponse::new);
    }

    CompletableFuture<OffsetCommitResponse> offsetCommit(OffsetCommitRequest request) {
        return send.send(ApiKeys.OFFSET_COMMIT, request.toStruct()).thenApply(OffsetCommitResponse::new);
    }

    CompletableFuture<OffsetFetchResponse> offsetFetch(OffsetFetchRequest request) {
        return send.send(ApiKeys.OFFSET_FETCH, request.toStruct()).thenApply(OffsetFetchResponse::new);
    }

    CompletableFuture<JoinGroupResponse> joinGroup(JoinGroupRequest request) {
        return send.send(ApiKeys.JOIN_GROUP, request.toStruct()).thenApply(JoinGroupResponse::new);
    }

    CompletableFuture<SyncGroupResponse> syncGroup(SyncGroupRequest request) {
        return send.send(ApiKeys.SYNC_GROUP, request.toStruct()).thenApply(SyncGroupResponse::new);
    }

    CompletableFuture<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
        return send.send(ApiKeys.HEARTBEAT, request.toStruct()).thenApply(HeartbeatResponse::new);
    }

    CompletableFuture<LeaveGroupResponse> leaveGroup(LeaveGroupRequest request) {
        return send.send(ApiKeys.LEAVE_GROUP, request.toStruct()).thenApply(LeaveGroupResponse::new);
    }
}
