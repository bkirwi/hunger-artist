package com.monovore.hunger;

import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Futures {

    public static <A> CompletableFuture<List<A>> collect(List<CompletableFuture<A>> futures) {
        return CompletableFuture.allOf(futures.stream().toArray(CompletableFuture[]::new))
                .thenApply(done -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    public static <T> Function<T, CompletableFuture<T>> liftError(Function<T, Short> getErrorCode) {

        return value -> {
            short code = getErrorCode.apply(value);

            Errors error = Errors.forCode(code);
            if (error == Errors.NONE) {
                return CompletableFuture.completedFuture(value);
            } else {
                return exceptionalFuture(error.exception());
            }
        };
    }

    public static <T> CompletableFuture<T> exceptionalFuture(Throwable throwable) {
        CompletableFuture<T> failure = new CompletableFuture<>();
        failure.completeExceptionally(throwable);
        return failure;
    }

    public static <T> CompletableFuture<T> lifting(Function<T, Short> getErrorCode, CompletableFuture<T> future) {
        return future.thenCompose(liftError(getErrorCode));
    }
}
