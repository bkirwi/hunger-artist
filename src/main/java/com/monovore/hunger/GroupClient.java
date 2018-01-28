//package com.monovore.hunger;
//
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
//import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
//import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.protocol.Errors;
//import org.apache.kafka.common.requests.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.nio.ByteBuffer;
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//import java.util.stream.Collectors;
//
//public class GroupClient<A> {
//
//    private static final Logger log = LoggerFactory.getLogger(GroupClient.class);
//
//    public A assignment() {
//        return assignment;
//    }
//
//    static class Meta {
//        public final String groupId;
//        public final String protocolType;
//        public final int sessionTimeout;
//
//        public Meta(String groupId, String protocolType, int sessionTimeout) {
//            this.groupId = groupId;
//            this.protocolType = protocolType;
//            this.sessionTimeout = sessionTimeout;
//        }
//
//        public Meta(String groupId, int sessionTimeout) {
//            this(groupId, ConsumerProtocol.PROTOCOL_TYPE, sessionTimeout);
//        }
//    }
//
//    private final ApiClient api;
//    private final Meta meta;
//    private final int generationId;
//    private final String memberId;
//    private A assignment;
//
//    public GroupClient(ApiClient api, Meta meta, A assignment) {
//        this(api, meta, OffsetCommitRequest.DEFAULT_GENERATION_ID, OffsetCommitRequest.DEFAULT_MEMBER_ID, assignment);
//    }
//
//    public GroupClient(ApiClient api, Meta meta, int generationId, String memberId, A assignment) {
//        this.api = api;
//        this.meta = meta;
//        this.generationId = generationId;
//        this.memberId = memberId;
//        this.assignment = assignment;
//    }
//
//    public CompletableFuture<Void> offsetCommit(
//            Map<TopicPartition, OffsetAndMetadata> offsets
//    ) {
//        OffsetCommitRequest request = new OffsetCommitRequest(
//                meta.groupId,
//                generationId,
//                memberId,
//                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
//                offsets.entrySet().stream()
//                        .collect(Collectors.toMap(
//                                Map.Entry::getKey,
//                                entry -> new OffsetCommitRequest.PartitionData(entry.getValue().offset(), entry.getValue().metadata())
//                        ))
//        );
//
//        return api.offsetCommit(request)
//                .thenCompose(results -> {
//
//                    for (short code : results.responseData().values()) {
//                        Errors error = Errors.forCode(code);
//                        if (error != Errors.NONE) return Futures.exceptionalFuture(error.exception());
//                    }
//
//                    return CompletableFuture.completedFuture(null);
//                });
//    }
//
//    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> offsetFetch(
//            Set<TopicPartition> partitions
//    ) {
//        OffsetFetchRequest request = new OffsetFetchRequest(meta.groupId, new ArrayList<TopicPartition>(partitions));
//
//        return api.offsetFetch(request).thenCompose(results -> {
//
//            for (OffsetFetchResponse.PartitionData data : results.responseData().values()) {
//                Errors error = Errors.forCode(data.errorCode);
//                if (error != Errors.NONE) return Futures.exceptionalFuture(error.exception());
//            }
//
//            Map<TopicPartition, OffsetAndMetadata> result = results.responseData().entrySet().stream()
//                    .collect(Collectors.toMap(
//                            Map.Entry::getKey,
//                            entry -> new OffsetAndMetadata(entry.getValue().offset, entry.getValue().metadata)
//                    ));
//
//            return CompletableFuture.completedFuture(result);
//        });
//    }
//
//    public CompletableFuture<Void> heartbeat() {
//        HeartbeatRequest request = new HeartbeatRequest(this.meta.groupId, this.generationId, this.memberId);
//        return Futures.lifting(HeartbeatResponse::errorCode, api.heartbeat(request))
//                .thenApply(response -> null);
//    }
//
//    public CompletableFuture<GroupClient<Void>> leave() {
//        log.info("Member '{}' leaving group {}", memberId, meta.groupId);
//        LeaveGroupRequest request = new LeaveGroupRequest(this.meta.groupId, this.memberId);
//        return Futures.lifting(LeaveGroupResponse::errorCode, api.leaveGroup(request))
//                .thenApply(response -> new GroupClient<Void>(api, meta, null));
//    }
//
//    /**
//     * Join or rejoin a group.
//     *
//     * Once this method is called, the current client becomes invalid and should be discarded.
//     *
//     * @param topics The set of topics we want to subscribe to.
//     * @param assignors A list of possible partition-assignment strategies, in order of preference.
//     * @return a new client that has successfully joined the group, or an error.
//     */
//    public CompletableFuture<GroupClient<Assignment>> join(Set<String> topics, List<PartitionAssignor> assignors) {
//
//        List<JoinGroupRequest.ProtocolMetadata> protocols =
//                assignors.stream()
//                        .map(assignor ->
//                                new JoinGroupRequest.ProtocolMetadata(
//                                        assignor.name(),
//                                        ConsumerProtocol.serializeSubscription(assignor.subscription(topics))
//                                )
//                        )
//                        .collect(Collectors.toList());
//
//        JoinGroupRequest joinRequest =
//                new JoinGroupRequest(meta.groupId, meta.sessionTimeout, this.memberId, meta.protocolType, protocols);
//
//        log.info("Joining group '{}' with member id '{}'...", meta.groupId, memberId);
//
//        return Futures.lifting(JoinGroupResponse::errorCode, api.joinGroup(joinRequest))
//                .thenCompose(joinResponse -> {
//
//                    final CompletableFuture<Map<String, ByteBuffer>> groupAssignment;
//                    if (joinResponse.isLeader()) {
//
//                        log.info("Joined group '{}' as leader; generating assignments and syncing...", meta.groupId);
//
//                        String protocol = joinResponse.groupProtocol();
//                        PartitionAssignor partitionAssignor =
//                                assignors.stream()
//                                        .filter(assignor -> assignor.name().equals(protocol))
//                                        .findFirst()
//                                        .get(); // Guaranteed to return one of the specified protocols here
//
//                        Map<String, PartitionAssignor.Subscription> subscriptions =
//                                joinResponse.members().entrySet().stream()
//                                        .collect(Collectors.toMap(Map.Entry::getKey,
//                                                entry -> ConsumerProtocol.deserializeSubscription(entry.getValue())
//                                        ));
//
//                        List<String> allTopics = subscriptions.values().stream()
//                                .flatMap(subscription -> subscription.topics().stream())
//                                .collect(Collectors.toList());
//
//                        groupAssignment =
//                                api.metadata(new MetadataRequest(allTopics))
//                                        .thenCompose(metadataResponse -> {
//
//                                            for (Errors error : metadataResponse.errors().values()) {
//                                                if (error != Errors.NONE)
//                                                    return Futures.exceptionalFuture(error.exception());
//                                            }
//
//                                            Map<String, ByteBuffer> result = partitionAssignor.assign(metadataResponse.cluster(), subscriptions)
//                                                    .entrySet().stream()
//                                                    .collect(Collectors.toMap(Map.Entry::getKey,
//                                                            entry -> ConsumerProtocol.serializeAssignment(entry.getValue())
//                                                    ));
//
//                                            return CompletableFuture.completedFuture(result);
//                                        });
//                    } else {
//                        log.info("Joined group '{}' as follower; syncing...", meta.groupId);
//                        groupAssignment = CompletableFuture.completedFuture(Collections.emptyMap());
//                    }
//
//                    return groupAssignment.thenCompose(assignments -> {
//                        SyncGroupRequest syncRequest =
//                                new SyncGroupRequest(meta.groupId, joinResponse.generationId(), joinResponse.memberId(), assignments);
//
//                        return Futures.lifting(SyncGroupResponse::errorCode, api.syncGroup(syncRequest))
//                                .thenApply(syncResponse -> {
//                                    log.info("Synced with group '{}'; done.", meta.groupId);
//                                    Assignment assignment =
//                                            ConsumerProtocol.deserializeAssignment(syncResponse.memberAssignment());
//                                    return new GroupClient<>(this.api, this.meta, joinResponse.generationId(), joinResponse.memberId(), assignment);
//                                });
//                    });
//                });
//    }
//}
