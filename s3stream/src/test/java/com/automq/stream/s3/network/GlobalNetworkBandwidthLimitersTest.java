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

package com.automq.stream.s3.network;

import com.automq.stream.s3.Config;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class GlobalNetworkBandwidthLimitersTest {
    private GlobalNetworkBandwidthLimiters newLimiters() throws ReflectiveOperationException {
        var constructor = GlobalNetworkBandwidthLimiters.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    /** Defaults retain the legacy implementation, refill interval, and directional capacities. */
    @Test
    void legacyDefaults() throws ReflectiveOperationException {
        Config config = new Config();
        assertEquals(NetworkBandwidthLimiterType.LEGACY, config.networkLimiterType());
        assertEquals(10, config.refillPeriodMs());
        List<List<?>> arguments = new ArrayList<>();
        try (MockedConstruction<AsyncNetworkBandwidthLimiter> mocks = mockConstruction(AsyncNetworkBandwidthLimiter.class,
            (mock, context) -> arguments.add(context.arguments()))) {
            newLimiters().setup(config.networkLimiterType(), config.networkBandwidthMode(), config.networkBaselineBandwidth(),
                config.networkBurstBandwidth(), config.refillPeriodMs(), config.networkMaxBurstWindowMs());
            assertEquals(2, mocks.constructed().size());
            assertEquals(config.networkBaselineBandwidth(), arguments.get(0).get(2));
            assertEquals(config.networkBaselineBandwidth() * 5, arguments.get(1).get(2));
        }
    }

    /** Both directional views share one smooth limiter in shared mode and preserve the configured budgets and weights. */
    @Test
    void smoothShared() throws ReflectiveOperationException {
        List<SmoothNetworkBandwidthLimiter.Configuration> configurations = new ArrayList<>();
        try (MockedConstruction<SmoothNetworkBandwidthLimiter> mocks = mockConstruction(SmoothNetworkBandwidthLimiter.class,
            (mock, context) -> {
                configurations.add((SmoothNetworkBandwidthLimiter.Configuration) context.arguments().get(0));
                when(mock.consume(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
            })) {
            GlobalNetworkBandwidthLimiters global = newLimiters();
            global.setup(NetworkBandwidthLimiterType.SMOOTH, NetworkBandwidthMode.SHARED, 1024000, 2048000, 1, 10);
            assertEquals(1, mocks.constructed().size());
            SmoothNetworkBandwidthLimiter.Configuration config = configurations.get(0);
            assertEquals(1024, config.baselineTokens());
            assertEquals(2048, config.peakTokens());
            assertEquals(10240, config.capacity());
            assertEquals(32768, config.baseQuantum());
            assertEquals(Map.of(ThrottleStrategy.WAL, 10, ThrottleStrategy.WAL_UPLOAD, 4, ThrottleStrategy.COMPACTION, 3,
                ThrottleStrategy.TAIL, 2, ThrottleStrategy.CATCH_UP, 1, ThrottleStrategy.ICEBERG_WRITE, 1), config.weights());
            global.inbound().consume(ThrottleStrategy.TAIL, 10).join();
            global.outbound().consume(ThrottleStrategy.WAL_UPLOAD, 20).join();
            verify(mocks.constructed().get(0)).consume(ThrottleStrategy.TAIL, 10);
            verify(mocks.constructed().get(0)).consume(ThrottleStrategy.WAL_UPLOAD, 20);
            assertThrows(IllegalArgumentException.class, () -> global.setup(NetworkBandwidthLimiterType.LEGACY, NetworkBandwidthMode.SEPARATE, 1024000, 2048000, 10, 60000));
        }
    }

    /** Separate mode creates independent smooth limiters with identical budgets and no burst savings by default. */
    @Test
    void smoothSeparateWithoutDefaultBurst() throws ReflectiveOperationException {
        List<SmoothNetworkBandwidthLimiter.Configuration> configurations = new ArrayList<>();
        try (MockedConstruction<SmoothNetworkBandwidthLimiter> mocks = mockConstruction(SmoothNetworkBandwidthLimiter.class,
            (mock, context) -> configurations.add((SmoothNetworkBandwidthLimiter.Configuration) context.arguments().get(0)))) {
            Config config = new Config().networkBaselineBandwidth(1024000).networkLimiterType(NetworkBandwidthLimiterType.SMOOTH);
            newLimiters().setup(config.networkLimiterType(), config.networkBandwidthMode(), config.networkBaselineBandwidth(),
                config.networkBurstBandwidth(), config.refillPeriodMs(), config.networkMaxBurstWindowMs());
            assertEquals(2, mocks.constructed().size());
            assertEquals(configurations.get(0), configurations.get(1));
            assertEquals(0, configurations.get(0).capacity());
            assertEquals(configurations.get(0).baselineBandwidth(), configurations.get(0).burstBandwidth());
            assertEquals(6000, configurations.get(0).maxBurstWindows());
        }
    }

    /** Limiter type parsing accepts supported names and rejects misspellings. */
    @Test
    void parseType() {
        assertEquals(NetworkBandwidthLimiterType.LEGACY, NetworkBandwidthLimiterType.parse("legacy"));
        assertEquals(NetworkBandwidthLimiterType.SMOOTH, NetworkBandwidthLimiterType.parse("SMOOTH"));
        assertThrows(IllegalArgumentException.class, () -> NetworkBandwidthLimiterType.parse("other"));
    }
}
