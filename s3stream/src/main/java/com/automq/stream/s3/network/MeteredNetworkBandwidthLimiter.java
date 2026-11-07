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

import java.util.concurrent.CompletableFuture;

import io.opentelemetry.api.common.Attributes;

/**
 * Records directional network metrics while delegating throttling to another limiter.
 */
public class MeteredNetworkBandwidthLimiter implements NetworkBandwidthLimiter {
    private final Direction direction;
    private final NetworkBandwidthLimiter delegate;
    private final Metrics.LongGaugeBundle.LongGauge queueSizeMetric;

    @SuppressWarnings("this-escape")
    public MeteredNetworkBandwidthLimiter(Direction direction, NetworkBandwidthLimiter delegate) {
        this.direction = direction;
        this.delegate = delegate;
        this.queueSizeMetric = queueSizeMetric(direction);
        this.queueSizeMetric.record(delegate::getQueueSize);
    }

    @Override
    public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
        long startNs = System.nanoTime();
        CompletableFuture<Void> cf = delegate.consume(throttleStrategy, size);
        boolean queued = !cf.isDone();
        cf.whenComplete((v, e) -> {
            NetworkStats.getInstance().recordNetworkUsageTotal(direction, throttleStrategy, size);
            if (queued) {
                NetworkStats.getInstance().recordNetworkLimiterQueueTime(
                    direction, throttleStrategy, System.nanoTime() - startNs);
            }
        });
        return cf;
    }

    @Override
    public long getMaxTokens() {
        return delegate.getMaxTokens();
    }

    @Override
    public long getAvailableTokens() {
        return delegate.getAvailableTokens();
    }

    @Override
    public int getQueueSize() {
        return delegate.getQueueSize();
    }

    @Override
    public void shutdown() {
        queueSizeMetric.close();
        // The delegate owns its lifecycle.
    }

    private static Metrics.LongGaugeBundle.LongGauge queueSizeMetric(Direction direction) {
        switch (direction) {
            case INBOUND:
                return NetworkInboundLimiterQueueSizeMetric.BUNDLE.register(MetricsLevel.DEBUG, Attributes.empty());
            case OUTBOUND:
                return NetworkOutboundLimiterQueueSizeMetric.BUNDLE.register(MetricsLevel.DEBUG, Attributes.empty());
            default:
                throw new IllegalArgumentException("Unsupported network bandwidth limiter direction: " + direction);
        }
    }

    public enum Direction {
        INBOUND,
        OUTBOUND
    }

    private static class NetworkInboundLimiterQueueSizeMetric {
        private static final Metrics.LongGaugeBundle BUNDLE = Metrics.instance()
            .longGauge("kafka_stream_network_inbound_limiter_queue_size", "Network inbound limiter queue size", "");
    }

    private static class NetworkOutboundLimiterQueueSizeMetric {
        private static final Metrics.LongGaugeBundle BUNDLE = Metrics.instance()
            .longGauge("kafka_stream_network_outbound_limiter_queue_size", "Network outbound limiter queue size", "");
    }
}
