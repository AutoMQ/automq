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

import io.opentelemetry.api.common.Attributes;

public class GlobalNetworkBandwidthLimiters {
    private static final Logger LOGGER = new LogContext().logger(GlobalNetworkBandwidthLimiters.class);
    private static final int OUTBOUND_MAX_TOKENS_MULTIPLIER = 5;
    private static final int SHARED_MAX_TOKENS_MULTIPLIER = 2;

    private static final GlobalNetworkBandwidthLimiters INSTANCE = new GlobalNetworkBandwidthLimiters();
    private volatile boolean setup = false;
    private volatile NetworkBandwidthLimiter inboundLimiter = AsyncNetworkBandwidthLimiter.NOOP;
    private volatile NetworkBandwidthLimiter outboundLimiter = AsyncNetworkBandwidthLimiter.NOOP;

    public static GlobalNetworkBandwidthLimiters instance() {
        return INSTANCE;
    }

    /**
     * Initializes global directional limiter views from the configured bandwidth mode.
     */
    public synchronized void setup(NetworkBandwidthMode mode, long bandwidth, int refillIntervalMs) {
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
            AsyncNetworkBandwidthLimiter physicalSharedLimiter = new AsyncNetworkBandwidthLimiter(
                tokenSize, refillIntervalMs, bandwidth * SHARED_MAX_TOKENS_MULTIPLIER);
            inboundLimiter = meter(MeteredNetworkBandwidthLimiter.Direction.INBOUND, physicalSharedLimiter);
            outboundLimiter = meter(MeteredNetworkBandwidthLimiter.Direction.OUTBOUND, physicalSharedLimiter);
            Metrics.LongGaugeBundle.LongGauge networkSharedAvailableBandwidthMetric =
                NetworkSharedAvailableBandwidthMetric.BUNDLE.register(MetricsLevel.INFO, Attributes.empty());
            networkSharedAvailableBandwidthMetric.record(() -> bandwidth - (long) NetworkStats.getInstance().networkSharedRate());
        } else {
            AsyncNetworkBandwidthLimiter physicalInboundLimiter = new AsyncNetworkBandwidthLimiter(
                tokenSize, refillIntervalMs, bandwidth);
            // Use a larger token pool for outbound traffic to avoid spikes caused by Upload WAL affecting
            // tail-reading performance.
            AsyncNetworkBandwidthLimiter physicalOutboundLimiter = new AsyncNetworkBandwidthLimiter(
                tokenSize, refillIntervalMs, bandwidth * OUTBOUND_MAX_TOKENS_MULTIPLIER);
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
        LOGGER.info("Global network bandwidth limiters setup, mode: {}, bandwidth: {}, tokenSize: {}, refillIntervalMs: {}",
            mode.getName(), bandwidth, tokenSize, refillIntervalMs);
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
