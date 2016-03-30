package com.monovore.hunger;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class Client {

    private final Communicator communicator;

    public Client(Communicator communicator) {
        this.communicator = communicator;
    }

    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {

        return communicator.send(Optional.empty(), ApiKeys.METADATA, request.toStruct())
                .thenApply(MetadataResponse::new);
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
                                        return communicator.send(Optional.of(entry.getKey()), ApiKeys.PRODUCE, nodeRequest.toStruct())
                                                .thenApply(ProduceResponse::new);
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
                                        return communicator.send(Optional.of(entry.getKey()), ApiKeys.FETCH, fetch.toStruct())
                                                .thenApply(FetchResponse::new);
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
                                        return communicator.send(Optional.of(entry.getKey()), ApiKeys.LIST_OFFSETS, list.toStruct())
                                                .thenApply(ListOffsetResponse::new);
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
        return communicator.send(Optional.empty(), ApiKeys.GROUP_COORDINATOR, request.toStruct())
                .thenApply(GroupCoordinatorResponse::new);
    }

    private CompletableFuture<Node> coordinatorNode(String groupId) {
        return groupCoordinator(new GroupCoordinatorRequest(groupId))
                .thenCompose(response -> {
                    Errors error = Errors.forCode(response.errorCode());
                    if (error == Errors.NONE) {
                        Node coordinator = response.node();
                        return CompletableFuture.completedFuture(coordinator);
                    }
                    else {
                        CompletableFuture<Node> failure = new CompletableFuture<>();
                        failure.completeExceptionally(error.exception());
                        return failure;
                    }
                });
    }

    public CompletableFuture<OffsetCommitResponse> offsetCommit(OffsetCommitRequest request) {
        return coordinatorNode(request.groupId())
                .thenCompose(coordinator ->
                        communicator.send(Optional.of(coordinator), ApiKeys.OFFSET_COMMIT, request.toStruct())
                                .thenApply(OffsetCommitResponse::new)
                );
    }

    public CompletableFuture<OffsetFetchResponse> offsetFetch(OffsetFetchRequest request) {
        return coordinatorNode(request.groupId())
                .thenCompose(coordinator ->
                        communicator.send(Optional.of(coordinator), ApiKeys.OFFSET_FETCH, request.toStruct())
                            .thenApply(OffsetFetchResponse::new)
                );
    }

    public static class Outbound {
        public final Optional<Node> destination;
        public final ApiKeys key;
        public final Struct request;
        public final CompletableFuture<Struct> responseFuture;

        public Outbound(Optional<Node> destination, ApiKeys key, Struct request, CompletableFuture<Struct> responseFuture) {
            this.destination = destination;
            this.key = key;
            this.request = request;
            this.responseFuture = responseFuture;
        }
    }

    public static class Communicator implements Runnable, Closeable {

        private final KafkaClient kafka;
        private final BlockingQueue<Outbound> outbound;
        private final Time time;

        private volatile boolean running = true;

        private static final long POLL_TIMEOUT_MS = 1000L;

        public Communicator(KafkaClient kafka, Time time) {
            this.kafka = kafka;
            this.outbound = new LinkedBlockingQueue<>();
            this.time = time;
        }

        public CompletableFuture<? extends Struct> send(Optional<Node> destination, ApiKeys key, Struct request) {
            CompletableFuture<Struct> responseFuture = new CompletableFuture<>();
            try {
                this.outbound.put(new Outbound(destination, key, request, responseFuture));
            } catch (InterruptedException e) {
                responseFuture.completeExceptionally(e);
                Thread.currentThread().interrupt();
            }
            this.kafka.wakeup();
            return responseFuture;
        }

        @Override
        public void run() {
            Outbound next;
            while(running && !Thread.interrupted()) {
                long now = time.milliseconds();
                while ((next = outbound.poll()) != null) {
                    final Outbound message = next;
                    Node destination =
                            message.destination.orElseGet(() -> kafka.leastLoadedNode(now));

                    if (destination == null) {
                        message.responseFuture.completeExceptionally(new BrokerNotAvailableException("No brokers available!"));
                        continue;
                    }

                    if (!kafka.ready(destination, now)) kafka.poll(POLL_TIMEOUT_MS, now);

                    if (!kafka.isReady(destination, now)) {
                        message.responseFuture.completeExceptionally(new BrokerNotAvailableException("Connection not ready!"));
                        continue;
                    }

                    RequestHeader header = kafka.nextRequestHeader(message.key);
                    RequestSend send = new RequestSend(destination.idString(), header, message.request);
                    ClientRequest request = new ClientRequest(now, true, send, response -> {
                        if (response.wasDisconnected()) {
                            message.responseFuture.completeExceptionally(new DisconnectException("Disconnect!"));
                        } else {
                            Struct body = response.responseBody();
                            message.responseFuture.complete(body);
                        }
                    });
                    kafka.send(request, now);
                }
                kafka.poll(POLL_TIMEOUT_MS, now);
            }
        }

        @Override
        public void close() throws IOException {
            running = false;
            kafka.close();
        }
    }

    public static KafkaClient fromWhatever(List<InetSocketAddress> bootstrapHosts, ChannelBuilder channelBuilder, Time time, Metrics metrics) {
        Selectable selector = new Selector(10000L, metrics, time, "test-client", new HashMap<>(), channelBuilder);
        Metadata metadata = new Metadata();
        metadata.update(Cluster.bootstrap(bootstrapHosts), time.milliseconds());
        String clientId = "test-client";
        NetworkClient kafka = new NetworkClient(selector, metadata, clientId, 100, 100, 100 * 1024, 100 * 1024, 100, time);
        return kafka;
    }
}