package com.monovore.hunger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Futures {

    public static <A> CompletableFuture<List<A>> collect(List<CompletableFuture<A>> futures) {
        return CompletableFuture.allOf(futures.stream().toArray(CompletableFuture[]::new))
                .thenApply(done -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }
}
