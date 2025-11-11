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

package com.automq.stream.s3;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Timeout(60)
public class AsyncNetworkBandwidthLimiterTest {

    @Test
    public void testByPassConsume() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 5000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 1);
        Assertions.assertEquals(9, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
    }

    @Test
    public void testByPassConsume2() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 20);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-10, bucket.getAvailableTokens());
        });
        cf.join();
        result.join();
    }

    @Test
    public void testThrottleConsume() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 1);
        Assertions.assertEquals(9, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
    }

    @Test
    public void testThrottleConsume2() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 20);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-10, bucket.getAvailableTokens());
        });
        cf.join();
        result.join();
    }

    @Test
    public void testThrottleConsume3() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 20);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
        cf = bucket.consume(ThrottleStrategy.CATCH_UP, 10);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(0, bucket.getAvailableTokens());
        });
        cf.join();
        result.join();
    }

    @Test
    public void testThrottleConsume4() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(
            AsyncNetworkBandwidthLimiter.Type.INBOUND, 100, 1000);
        bucket.consume(ThrottleStrategy.BYPASS, 1000);
        Assertions.assertEquals(-100, bucket.getAvailableTokens());
        CompletableFuture<Boolean> firstCompleted = new CompletableFuture<>();
        CompletableFuture<Void> cf1 = bucket.consume(ThrottleStrategy.CATCH_UP, 5);
        cf1 = cf1.thenApply(v -> {
            firstCompleted.complete(true);
            return null;
        });
        CompletableFuture<Void> cf2 = bucket.consume(ThrottleStrategy.CATCH_UP, 10);
        CompletableFuture<Void> result = cf2.thenAccept(v -> {
            Assertions.assertTrue(firstCompleted.isDone(),
                "First request should complete before second request");
        });
        result.join();
    }

    @Test
    public void testThrottleConsume5() throws InterruptedException {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 100, 200);
        bucket.consume(ThrottleStrategy.BYPASS, 1000);
        Assertions.assertEquals(-200, bucket.getAvailableTokens());
        Thread.sleep(500);
        bucket.consume(ThrottleStrategy.BYPASS, 500);
        bucket.consume(ThrottleStrategy.CATCH_UP, 5);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 10);
        cf.join();
        Assertions.assertEquals(-5, bucket.getAvailableTokens());
    }

    @Test
    public void testThrottleConsumeWithPriority() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 100, 1000);
        bucket.consume(ThrottleStrategy.BYPASS, 1000);
        Assertions.assertEquals(-100, bucket.getAvailableTokens());
        bucket.consume(ThrottleStrategy.COMPACTION, 10);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 80);
        CompletableFuture<Void> cf2 = bucket.consume(ThrottleStrategy.COMPACTION, 100);
        CompletableFuture<Void> result = cf2.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-10, bucket.getAvailableTokens());
            Assertions.assertFalse(cf.isDone());
        });
        result.join();
        CompletableFuture<Void> result2 = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(10, bucket.getAvailableTokens());
        });
        result2.join();
    }

    @Test
    public void testThrottleConsumeWithPriority1() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 1024 * 1024, 1000);
        bucket.consume(ThrottleStrategy.BYPASS, 1024 * 1024);
        Assertions.assertEquals(0, bucket.getAvailableTokens());
        CompletableFuture<Void> cf2 = bucket.consume(ThrottleStrategy.CATCH_UP, 5 * 1024 * 1024);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.COMPACTION, 5 * 1024 * 1024);
        TimerUtil timerUtil = new TimerUtil();
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(0, bucket.getAvailableTokens());
            Assertions.assertFalse(cf2.isDone());
            Assertions.assertTrue(timerUtil.elapsedAs(TimeUnit.SECONDS) <= 6);
        });
        result.join();
        cf2.join();
    }

    @Test
    @Timeout(10)
    public void testThrottleConsumeWithLargeChunk() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 1024 * 1024, 100);
        CompletableFuture<Void> bypassCf = bucket.consume(ThrottleStrategy.BYPASS, 1024 * 1024);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 10 * 1024 * 1024);
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(0, bucket.getAvailableTokens());
            Assertions.assertTrue(bypassCf.isDone());
        });
        result.join();
    }
}