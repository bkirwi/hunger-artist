package com.monovore.hunger;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class Communicator implements Closeable {

    private final KafkaClient kafka;
    private final BlockingQueue<Outbound> outbound;
    private final Time time;
    private final Thread thread;

    private volatile boolean running = true;

    private static final long POLL_TIMEOUT_MS = 1000L;

    public Communicator(KafkaClient kafka, Time time) {
        this.kafka = kafka;
        this.outbound = new LinkedBlockingQueue<>();
        this.time = time;
        this.thread = new Thread(this::run);
        this.thread.setDaemon(true);
        this.thread.start();
    }

    public ApiClient anyone() {
        return new ApiClient((key, request) -> send(Optional.empty(), key, request));
    }

    public ApiClient node(Node node) {
        return new ApiClient(((key, request) -> send(Optional.of(node), key, request)));
    }

    public CompletableFuture<GroupClient<Void>> group(GroupClient.Meta meta) {
        return Futures.lifting(GroupCoordinatorResponse::errorCode, this.anyone().groupCoordinator(new GroupCoordinatorRequest(meta.groupId)))
                .thenApply(groupCoordinatorResponse -> new GroupClient<Void>(this.node(groupCoordinatorResponse.node()), meta, null));
    }

    public CompletableFuture<ApiClient> coordinator(String groupID) {
        return anyone().groupCoordinator(new GroupCoordinatorRequest(groupID))
                .thenCompose(Futures.liftError(GroupCoordinatorResponse::errorCode))
                .thenApply(response -> node(response.node()));
    }

    private CompletableFuture<Struct> send(Optional<Node> destination, ApiKeys key, Struct request) {
        CompletableFuture<Struct> responseFuture = new CompletableFuture<>();

        if (!running) {
          responseFuture.completeExceptionally(new IllegalStateException("Client has been closed!"));
          return responseFuture;
        }

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
    public void close() throws IOException {
        running = false;
        kafka.close();
    }

    private void run() {
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
                    message.responseFuture.completeExceptionally(new BrokerNotAvailableException("Connection not ready to node: " + destination.idString()));
                    continue;
                }

                RequestHeader header = kafka.nextRequestHeader(message.key);
                RequestSend send = new RequestSend(destination.idString(), header, message.request);
                ClientRequest request = new ClientRequest(now, true, send, response -> {
                    if (response.wasDisconnected()) {
                        message.responseFuture.completeExceptionally(new DisconnectException("Disconnected from node: " + destination.idString()));
                    } else {
                        Struct body = response.responseBody();
                        message.responseFuture.complete(body);
                    }
                });
                kafka.send(request, now);
            }
            // TODO: deal with exceptions here
            kafka.poll(POLL_TIMEOUT_MS, now);
        }
    }

    private static class Outbound {
        final Optional<Node> destination;
        final ApiKeys key;
        final Struct request;
        final CompletableFuture<Struct> responseFuture;

        Outbound(Optional<Node> destination, ApiKeys key, Struct request, CompletableFuture<Struct> responseFuture) {
            this.destination = destination;
            this.key = key;
            this.request = request;
            this.responseFuture = responseFuture;
        }
    }
}
