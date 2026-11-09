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

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;

import io.opentelemetry.api.common.Attributes;

public class GlobalNetworkBandwidthLimiters {
    private static final Logger LOGGER = new LogContext().logger(GlobalNetworkBandwidthLimiters.class);

    private static final GlobalNetworkBandwidthLimiters INSTANCE = new GlobalNetworkBandwidthLimiters();
    private volatile boolean setup = false;
    private volatile NetworkBandwidthLimiter inboundLimiter = NetworkBandwidthLimiter.NOOP;
    private volatile NetworkBandwidthLimiter outboundLimiter = NetworkBandwidthLimiter.NOOP;

    private GlobalNetworkBandwidthLimiters() {}

    public static GlobalNetworkBandwidthLimiters instance() {
        return INSTANCE;
    }

    /** Initializes process-wide limiter views from startup settings; bandwidths are bytes/second and durations are milliseconds. */
    public synchronized void setup(NetworkBandwidthMode mode, long bandwidth,
        long burstBandwidth, int refillIntervalMs, long maxBurstWindowMs) {
        if (setup) {
            throw new IllegalArgumentException("Network bandwidth limiters are already setup");
        }
        long tokenSize = (long) (bandwidth * ((double) refillIntervalMs / 1000));
        if (tokenSize <= 0) {
            throw new IllegalArgumentException(String.format("tokenSize must be greater than 0, bandwidth: %d, refill period: %dms",
                bandwidth, refillIntervalMs));
        }
        NetworkStats.getInstance().setup(bandwidth, mode);

        if (mode == NetworkBandwidthMode.SHARED) {
            NetworkBandwidthLimiter physicalSharedLimiter = createLimiter(bandwidth, burstBandwidth, refillIntervalMs, maxBurstWindowMs);
            inboundLimiter = meter(MeteredNetworkBandwidthLimiter.Direction.INBOUND, physicalSharedLimiter);
            outboundLimiter = meter(MeteredNetworkBandwidthLimiter.Direction.OUTBOUND, physicalSharedLimiter);
            Metrics.LongGaugeBundle.LongGauge networkSharedAvailableBandwidthMetric =
                NetworkSharedAvailableBandwidthMetric.BUNDLE.register(MetricsLevel.INFO, Attributes.empty());
            networkSharedAvailableBandwidthMetric.record(() -> bandwidth - (long) NetworkStats.getInstance().networkSharedRate());
        } else {
            NetworkBandwidthLimiter physicalInboundLimiter = createLimiter(bandwidth, burstBandwidth, refillIntervalMs, maxBurstWindowMs);
            NetworkBandwidthLimiter physicalOutboundLimiter = createLimiter(bandwidth, burstBandwidth, refillIntervalMs, maxBurstWindowMs);
            inboundLimiter = meter(MeteredNetworkBandwidthLimiter.Direction.INBOUND, physicalInboundLimiter);
            outboundLimiter = meter(MeteredNetworkBandwidthLimiter.Direction.OUTBOUND, physicalOutboundLimiter);
            Metrics.LongGaugeBundle.LongGauge networkInboundAvailableBandwidthMetric =
                NetworkInboundAvailableBandwidthMetric.BUNDLE.register(MetricsLevel.INFO, Attributes.empty());
            networkInboundAvailableBandwidthMetric.record(() -> bandwidth - (long) NetworkStats.getInstance().networkInboundRate());
            Metrics.LongGaugeBundle.LongGauge networkOutboundAvailableBandwidthMetric =
                NetworkOutboundAvailableBandwidthMetric.BUNDLE.register(MetricsLevel.INFO, Attributes.empty());
            networkOutboundAvailableBandwidthMetric.record(() -> bandwidth - (long) NetworkStats.getInstance().networkOutboundRate());
        }
        setup = true;
        LOGGER.info("Global network bandwidth limiters setup, mode: {}, bandwidth: {}, refillIntervalMs: {}",
            mode.getName(), bandwidth, refillIntervalMs);
    }

    private static NetworkBandwidthLimiter createLimiter(long bandwidth, long burstBandwidth,
        int refillIntervalMs, long maxBurstWindowMs) {
        return new SmoothNetworkBandwidthLimiter(new SmoothNetworkBandwidthLimiter.Configuration(
            bandwidth, burstBandwidth, Duration.ofMillis(refillIntervalMs),
            maxBurstWindowMs / refillIntervalMs, 32 * 1024, Map.of(
                ThrottleStrategy.WAL_UPLOAD, 4,
                ThrottleStrategy.COMPACTION, 3,
                ThrottleStrategy.TAIL, 2,
                ThrottleStrategy.CATCH_UP, 1,
                ThrottleStrategy.ICEBERG_WRITE, 1)));
    }

    private NetworkBandwidthLimiter meter(MeteredNetworkBandwidthLimiter.Direction direction, NetworkBandwidthLimiter delegate) {
        return new MeteredNetworkBandwidthLimiter(direction, delegate);
    }

    public NetworkBandwidthLimiter inbound() {
        return inboundLimiter;
    }

    public NetworkBandwidthLimiter outbound() {
        return outboundLimiter;
    }

    private static class NetworkInboundAvailableBandwidthMetric {
        private static final Metrics.LongGaugeBundle BUNDLE = Metrics.instance()
            .longGauge("kafka_stream_network_inbound_available_bandwidth", "Network inbound available bandwidth", "bytes");
    }

    private static class NetworkOutboundAvailableBandwidthMetric {
        private static final Metrics.LongGaugeBundle BUNDLE = Metrics.instance()
            .longGauge("kafka_stream_network_outbound_available_bandwidth", "Network outbound available bandwidth", "bytes");
    }

    private static class NetworkSharedAvailableBandwidthMetric {
        private static final Metrics.LongGaugeBundle BUNDLE = Metrics.instance()
            .longGauge("kafka_stream_network_shared_available_bandwidth", "Network shared available bandwidth", "bytes");
    }

}
