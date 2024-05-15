/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
public class AsyncNetworkBandwidthLimiterTest {

    @Test
    public void testByPassConsume() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 5000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 1);
        Assertions.assertEquals(8, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
    }

    @Test
    public void testByPassConsume2() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 20);
        Assertions.assertEquals(-9, bucket.getAvailableTokens());
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-9, bucket.getAvailableTokens());
        });
        cf.join();
        result.join();
    }

    @Test
    public void testThrottleConsume() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 1);
        Assertions.assertEquals(8, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
    }

    @Test
    public void testThrottleConsume2() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 20);
        Assertions.assertEquals(-9, bucket.getAvailableTokens());
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-9, bucket.getAvailableTokens());
        });
        cf.join();
        result.join();
    }

    @Test
    public void testThrottleConsume3() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 20);
        Assertions.assertEquals(-9, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
        cf = bucket.consume(ThrottleStrategy.CATCH_UP, 10);
        Assertions.assertEquals(-9, bucket.getAvailableTokens());
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-1, bucket.getAvailableTokens());
            Assertions.assertEquals(0, bucket.getAvailableExtraTokens());
        });
        cf.join();
        result.join();
    }

    @Test
    public void testThrottleConsume4() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 100, 1000);
        bucket.consume(ThrottleStrategy.BYPASS, 1000);
        Assertions.assertEquals(-90, bucket.getAvailableTokens());
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 5);
        bucket.consume(ThrottleStrategy.CATCH_UP, 10);
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(85, bucket.getAvailableTokens());
            Assertions.assertEquals(0, bucket.getAvailableExtraTokens());
        });
        cf.join();
        result.join();
    }

    @Test
    public void testThrottleConsume5() throws InterruptedException {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 100, 100);
        bucket.consume(ThrottleStrategy.BYPASS, 1000);
        Assertions.assertEquals(-90, bucket.getAvailableTokens());
        Thread.sleep(500);
        bucket.consume(ThrottleStrategy.BYPASS, 100);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.CATCH_UP, 5);
        bucket.consume(ThrottleStrategy.CATCH_UP, 10);
        CompletableFuture<Void> result = cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertTrue(bucket.getAvailableTokens() < 0);
            Assertions.assertEquals(5, bucket.getAvailableExtraTokens());
        });
        result.join();
    }

    @Test
    public void testThrottleConsumeWithPriority() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 100, 1000);
        bucket.consume(ThrottleStrategy.BYPASS, 1000);
        Assertions.assertEquals(-90, bucket.getAvailableTokens());
        bucket.consume(ThrottleStrategy.CATCH_UP, 10);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.COMPACTION, 80);
        CompletableFuture<Void> cf2 = bucket.consume(ThrottleStrategy.CATCH_UP, 100);
        CompletableFuture<Void> result = cf2.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-20, bucket.getAvailableTokens());
            Assertions.assertEquals(0, bucket.getAvailableExtraTokens());
            Assertions.assertFalse(cf.isDone());
        });
        cf.join();
        result.join();
    }
}
