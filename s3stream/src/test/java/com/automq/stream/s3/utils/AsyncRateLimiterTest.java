/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.utils;

import com.automq.stream.utils.AsyncRateLimiter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AsyncRateLimiterTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {
        AsyncRateLimiter asyncRateLimiter = new AsyncRateLimiter(1);
        long start = System.nanoTime();
        CompletableFuture<Void> cf1 = asyncRateLimiter.acquire(2);
        CompletableFuture<Void> cf2 = asyncRateLimiter.acquire(1).thenAccept(nil -> {
            long elapsed = System.nanoTime() - start;
            Assertions.assertTrue(elapsed > TimeUnit.SECONDS.toNanos(2) && elapsed < TimeUnit.SECONDS.toNanos(4));
        });
        CompletableFuture<Void> cf3 = asyncRateLimiter.acquire(1).thenAccept(nil -> {
            long elapsed = System.nanoTime() - start;
            Assertions.assertTrue(elapsed > TimeUnit.SECONDS.toNanos(3) && elapsed < TimeUnit.SECONDS.toNanos(5));
        });
        cf1.get();
        cf2.get();
        cf3.get();
        asyncRateLimiter.close();
    }

}
