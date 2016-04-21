package com.monovore.hunger;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Client {

    private final Communicator communicator;

    public Client(Communicator communicator) {
        this.communicator = communicator;
    }

    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {

        return communicator.anyone().metadata(request);
    }

    private <A> CompletableFuture<Map<Node, Map<TopicPartition, A>>> groupByNode(Map<TopicPartition, A> byPartition) {
        List<String> topics = byPartition.keySet().stream()
                .map(TopicPartition::topic)
                .distinct()
                .collect(Collectors.toList());

        return metadata(new MetadataRequest(topics))
                .thenApply(meta -> {
                    Cluster cluster = meta.cluster();

                    return byPartition.entrySet().stream()
                            .collect(Collectors.groupingBy(
                                    entry -> cluster.leaderFor(entry.getKey()),
                                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                            ));
                });
    }

    public CompletableFuture<ProduceResponse> produce(ProduceRequest request) {

        return groupByNode(request.partitionRecords())
                .thenCompose(dataByNode -> {

                    List<CompletableFuture<ProduceResponse>> futures =
                            dataByNode.entrySet().stream()
                                    .map(entry -> {
                                        ProduceRequest nodeRequest = new ProduceRequest(request.acks(), request.timeout(), entry.getValue());
                                        return communicator.node(entry.getKey()).produce(nodeRequest);
                                    })
                                    .collect(Collectors.toList());

                    return Futures.collect(futures)
                            .thenApply(results -> {
                                Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap =
                                    results.stream()
                                        .flatMap(result -> result.responses().entrySet().stream())
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                return new ProduceResponse(responseMap);
                            });
                });
    }

    public CompletableFuture<FetchResponse> fetch(FetchRequest request) {

        return groupByNode(request.fetchData())
                .thenCompose(fetchesByNode -> {

                    List<CompletableFuture<FetchResponse>> futures =
                            fetchesByNode.entrySet().stream()
                                    .map(entry -> {
                                        FetchRequest fetch = new FetchRequest(request.maxWait(), request.minBytes(), entry.getValue());
                                        return communicator.node(entry.getKey()).fetch(fetch);
                                    })
                                    .collect(Collectors.toList());

                    return Futures.collect(futures)
                            .thenApply(results -> {
                                Map<TopicPartition, FetchResponse.PartitionData> responseMap = results.stream()
                                        .flatMap(result -> result.responseData().entrySet().stream())
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                return new FetchResponse(responseMap);
                            });
                });
    }

    public CompletableFuture<ListOffsetResponse> listOffsets(ListOffsetRequest request) {

        return groupByNode(request.offsetData())
                .thenCompose(listByNode -> {

                    List<CompletableFuture<ListOffsetResponse>> futures =
                            listByNode.entrySet().stream()
                                    .map(entry -> {
                                        ListOffsetRequest list = new ListOffsetRequest(entry.getValue());
                                        return communicator.node(entry.getKey()).listOffsets(list);
                                    })
                                    .collect(Collectors.toList());

                    return Futures.collect(futures)
                            .thenApply(results -> {
                                Map<TopicPartition, ListOffsetResponse.PartitionData> responseMap = results.stream()
                                        .flatMap(result -> result.responseData().entrySet().stream())
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                return new ListOffsetResponse(responseMap);
                            });
                });
    }

    public CompletableFuture<GroupCoordinatorResponse> groupCoordinator(GroupCoordinatorRequest request) {
        return communicator.anyone().groupCoordinator(request);
    }

    public CompletableFuture<OffsetCommitResponse> offsetCommit(OffsetCommitRequest request) {
        return communicator.coordinator(request.groupId()).thenCompose((client) -> client.offsetCommit(request));
    }

    public CompletableFuture<OffsetFetchResponse> offsetFetch(OffsetFetchRequest request) {
        return communicator.coordinator(request.groupId()).thenCompose((client) -> client.offsetFetch(request));
    }

    public CompletableFuture<JoinGroupResponse> joinGroup(JoinGroupRequest request) {
        return communicator.coordinator(request.groupId()).thenCompose((client) -> client.joinGroup(request));
    }

    public CompletableFuture<SyncGroupResponse> syncGroup(SyncGroupRequest request) {
        return communicator.coordinator(request.groupId()).thenCompose((client) -> client.syncGroup(request));
    }

    public CompletableFuture<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
        return communicator.coordinator(request.groupId()).thenCompose((client) -> client.heartbeat(request));
    }

    public CompletableFuture<LeaveGroupResponse> leaveGroup(LeaveGroupRequest request) {
        return communicator.coordinator(request.groupId()).thenCompose((client) -> client.leaveGroup(request));
    }

    public static KafkaClient fromWhatever(List<InetSocketAddress> bootstrapHosts, ChannelBuilder channelBuilder, Time time, Metrics metrics) {
        Selectable selector = new Selector(10000L, metrics, time, "test-client", new HashMap<>(), channelBuilder);
        Metadata metadata = new Metadata();
        metadata.update(Cluster.bootstrap(bootstrapHosts), time.milliseconds());
        String clientId = "test-client";
        NetworkClient kafka = new NetworkClient(selector, metadata, clientId, 100, 100, 100 * 1024, 100 * 1024, 30000, time);
        return kafka;
    }
}
