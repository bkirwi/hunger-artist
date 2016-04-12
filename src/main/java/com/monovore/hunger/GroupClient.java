package com.monovore.hunger;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GroupClient {

    static class Meta {
        private final String groupId;
        private final String protocolType;
        private final int sessionTimeout;

        public Meta(String groupId, String protocolType, int sessionTimeout) {

            this.groupId = groupId;
            this.protocolType = protocolType;
            this.sessionTimeout = sessionTimeout;
        }
    }

    private final ApiClient api;
    private final Meta meta;

    public GroupClient(ApiClient api, Meta meta) {
        this.api = api;
        this.meta = meta;
    }

    public CompletableFuture<Map<TopicPartition, Errors>> offsetCommit(
            Map<TopicPartition, OffsetCommitRequest.PartitionData> offsets
    ) {
        OffsetCommitRequest request = new OffsetCommitRequest(
                meta.groupId,
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                OffsetCommitRequest.DEFAULT_MEMBER_ID,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsets
        );

        return api.offsetCommit(request)
                .thenApply(results ->
                        results.responseData().entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> Errors.forCode(entry.getValue())
                                ))
                );
    }

    public CompletableFuture<PartitionAssignor.Assignment> rejoin(Set<String> topics, List<PartitionAssignor> assignors) {

        List<JoinGroupRequest.ProtocolMetadata> protocols =
                assignors.stream()
                        .map(assignor ->
                                new JoinGroupRequest.ProtocolMetadata(
                                        assignor.name(),
                                        ConsumerProtocol.serializeSubscription(assignor.subscription(topics))
                                )
                        )
                        .collect(Collectors.toList());

        JoinGroupRequest joinRequest =
                new JoinGroupRequest(meta.groupId, meta.sessionTimeout, "", meta.protocolType, protocols);

        return Futures.lifting(JoinGroupResponse::errorCode, api.joinGroup(joinRequest))
                .thenCompose(joinResponse -> {

                    final CompletableFuture<Map<String, ByteBuffer>> groupAssignment;
                    if (joinResponse.isLeader()) {

                        String protocol = joinResponse.groupProtocol();
                        PartitionAssignor partitionAssignor =
                                assignors.stream()
                                        .filter(assignor -> assignor.name().equals(protocol))
                                        .findFirst()
                                        .get(); // Guaranteed to return one of the specified protocols here

                        Map<String, PartitionAssignor.Subscription> subscriptions =
                                joinResponse.members().entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                            entry -> ConsumerProtocol.deserializeSubscription(entry.getValue())
                                    ));

                        List<String> allTopics = subscriptions.values().stream()
                                .flatMap(subscription -> subscription.topics().stream())
                                .collect(Collectors.toList());

                        groupAssignment =
                            api.metadata(new MetadataRequest(allTopics))
                                    .thenApply(metadataResponse ->
                                            partitionAssignor.assign(metadataResponse.cluster(), subscriptions)
                                                    .entrySet().stream()
                                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                                            entry -> ConsumerProtocol.serializeAssignment(entry.getValue())
                                                    ))
                                    );
                    }
                    else {
                        groupAssignment = CompletableFuture.completedFuture(Collections.emptyMap());
                    }

                    return groupAssignment.thenCompose(assignment -> {
                        SyncGroupRequest syncRequest =
                                new SyncGroupRequest(meta.groupId, joinResponse.generationId(), joinResponse.memberId(), assignment);

                        return Futures.lifting(SyncGroupResponse::errorCode, api.syncGroup(syncRequest))
                                .thenApply(syncResponse ->
                                        ConsumerProtocol.deserializeAssignment(syncResponse.memberAssignment()));
                    });
                });
    }
}
