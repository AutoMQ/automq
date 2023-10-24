/*
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

import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

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
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.THROTTLE, 1);
        Assertions.assertEquals(9, bucket.getAvailableTokens());
        Assertions.assertTrue(cf.isDone());
    }

    @Test
    public void testThrottleConsume2() {
        AsyncNetworkBandwidthLimiter bucket = new AsyncNetworkBandwidthLimiter(AsyncNetworkBandwidthLimiter.Type.INBOUND, 10, 1000, 100);
        CompletableFuture<Void> cf = bucket.consume(ThrottleStrategy.THROTTLE, 20);
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
        cf = bucket.consume(ThrottleStrategy.THROTTLE, 10);
        Assertions.assertEquals(-10, bucket.getAvailableTokens());
        cf.whenComplete((v, e) -> {
            Assertions.assertNull(e);
            Assertions.assertEquals(0, bucket.getAvailableTokens());
        });
        cf.join();
    }
}
