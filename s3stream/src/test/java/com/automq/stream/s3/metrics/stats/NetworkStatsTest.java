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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.network.MeteredNetworkBandwidthLimiter;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.network.NetworkBandwidthMode;
import com.automq.stream.s3.network.ThrottleStrategy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
public class NetworkStatsTest {

    /**
     * Given sampled inbound and outbound rates, when calculating shared utilization, then both directions are summed.
     */
    @Test
    public void testSharedNetworkUtilUsesTotalRate() {
        assertEquals(0.7, NetworkStats.calculateNetworkUtil(30, 40, 100, NetworkBandwidthMode.SHARED), 0.0001);
    }

    /**
     * Given sampled inbound and outbound rates, when calculating separate utilization, then the busier direction is used.
     */
    @Test
    public void testSeparateNetworkUtilUsesMaxDirectionalRate() {
        assertEquals(0.4, NetworkStats.calculateNetworkUtil(30, 40, 100, NetworkBandwidthMode.SEPARATE), 0.0001);
    }

    /**
     * Given NetworkStats setup stores bandwidth mode, when calculating network utilization, then callers do not pass bandwidth or mode repeatedly.
     */
    @Test
    public void testSetupStoresNetworkUtilConfig() throws Exception {
        NetworkStats.getInstance().setup(100, NetworkBandwidthMode.SHARED);

        assertEquals(100L, fieldValue("networkBaselineBandwidth"));
        assertEquals(NetworkBandwidthMode.SHARED, fieldValue("networkBandwidthMode"));
    }

    /**
     * Given NetworkStats owns bandwidth mode, when inspecting public APIs, then old instance networkUtil overloads are not exposed.
     */
    @Test
    public void testNetworkUtilDoesNotExposeOldInstanceOverloads() {
        for (Method method : NetworkStats.class.getDeclaredMethods()) {
            if (!method.getName().equals("networkUtil")) {
                continue;
            }
            assertEquals(0, method.getParameterCount(), "NetworkStats instance networkUtil should not require bandwidth or mode");
        }
    }

    private static Object fieldValue(String fieldName) throws Exception {
        Field field = NetworkStats.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(NetworkStats.getInstance());
    }

    /**
     * Given direction-metered limiter wrappers, when delegated consumption waits asynchronously, then queue time is recorded for that direction.
     */
    @Test
    public void testMeteredLimiterRecordsQueueTimeOnlyWhenFutureIsNotDone() throws Exception {
        long baseline = inboundQueueTimeCount();
        CompletableFuture<Void> delayedFuture = new CompletableFuture<>();
        MeteredNetworkBandwidthLimiter limiter = new MeteredNetworkBandwidthLimiter(
            MeteredNetworkBandwidthLimiter.Direction.INBOUND, new TestLimiter(delayedFuture));

        limiter.consume(ThrottleStrategy.CATCH_UP, 10);
        assertEquals(baseline, inboundQueueTimeCount());

        delayedFuture.complete(null);
        assertEquals(baseline + 1, inboundQueueTimeCount());

        limiter.consume(ThrottleStrategy.CATCH_UP, 10).join();
        assertEquals(baseline + 1, inboundQueueTimeCount());
    }

    @SuppressWarnings("unchecked")
    private static long inboundQueueTimeCount() throws Exception {
        Field field = NetworkStats.class.getDeclaredField("networkInboundLimiterQueueTimeStatsMap");
        field.setAccessible(true);
        Map<ThrottleStrategy, ?> metrics = (Map<ThrottleStrategy, ?>) field.get(NetworkStats.getInstance());
        Object metric = metrics.get(ThrottleStrategy.CATCH_UP);
        if (metric == null) {
            return 0;
        }
        return (long) metric.getClass().getMethod("cumulativeCount").invoke(metric);
    }

    private static class TestLimiter implements NetworkBandwidthLimiter {
        private final CompletableFuture<Void> future;

        TestLimiter(CompletableFuture<Void> future) {
            this.future = future;
        }

        @Override
        public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
            return future;
        }

        @Override
        public long getMaxTokens() {
            return 0;
        }

        @Override
        public long getAvailableTokens() {
            return 0;
        }

        @Override
        public void shutdown() {
        }
    }
}
