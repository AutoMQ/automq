/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.utils;

import com.automq.stream.utils.AsyncRateLimiter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
