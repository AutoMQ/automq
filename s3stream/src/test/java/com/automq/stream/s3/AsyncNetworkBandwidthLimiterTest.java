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
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 5000, 100);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 1);
        Assertions.assertEquals(9, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
    }

    @Test
    public void testByPassConsume2() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000, 100);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 20);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-10, bucket.getAvailableTokens());
        });
        cf.join();
    }

    @Test
    public void testThrottleConsume() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000, 100);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.THROTTLE_1, 1);
        Assertions.assertEquals(9, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
    }

    @Test
    public void testThrottleConsume2() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000, 100);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.THROTTLE_1, 20);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(-10, bucket.getAvailableTokens());
        });
        cf.join();
    }

    @Test
    public void testThrottleConsume3() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000, 100);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.BYPASS, 20);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
        cf = bucket.consume(ThrottleStrategy.THROTTLE_1, 10);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(0, bucket.getAvailableTokens());
        });
        cf.join();
    }

    @Test
    public void testThrottleConsume4() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 100, 1000, 100);
        bucket.consume(ThrottleStrategy.BYPASS, 1000);
        Assertions.assertEquals(-100, bucket.getAvailableTokens());
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.THROTTLE_1, 5);
        bucket.consume(ThrottleStrategy.THROTTLE_1, 10);
        cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(0, bucket.getAvailableTokens());
            Assertions.assertEquals(5, bucket.getExtraTokens());
        });
        cf.join();
    }
}
