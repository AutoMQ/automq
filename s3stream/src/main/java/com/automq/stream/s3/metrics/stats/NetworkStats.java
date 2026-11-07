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

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.wrapper.Counter;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;
import com.automq.stream.s3.network.MeteredNetworkBandwidthLimiter;
import com.automq.stream.s3.network.NetworkBandwidthMode;
import com.automq.stream.s3.network.ThrottleStrategy;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

public class NetworkStats {
    private static final long NETWORK_RATE_MIN_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    private static final AttributeKey<String> LABEL_TYPE = AttributeKey.stringKey("type");
    private static final Metrics.LongCounterBundle NETWORK_INBOUND_USAGE = Metrics.instance()
        .longCounter("kafka_stream_network_inbound_usage", "Network inbound usage", "bytes");
    private static final Metrics.LongCounterBundle NETWORK_OUTBOUND_USAGE = Metrics.instance()
        .longCounter("kafka_stream_network_outbound_usage", "Network outbound usage", "bytes");
    private static final NetworkStats INSTANCE = new NetworkStats();
    // <StreamId, <FastReadBytes, SlowReadBytes>>
    private final Map<Long, Pair<Counter, Counter>> streamReadBytesStats = new ConcurrentHashMap<>();
    private final Counter networkInboundUsageTotal = new Counter();
    private final Counter networkOutboundUsageTotal = new Counter();
    private final MinIntervalRate networkInboundRate = new MinIntervalRate(NETWORK_RATE_MIN_INTERVAL_MS);
    private final MinIntervalRate networkOutboundRate = new MinIntervalRate(NETWORK_RATE_MIN_INTERVAL_MS);
    private final Map<ThrottleStrategy, Metrics.LongCounterBundle.LongCounter> networkInboundUsageTotalStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, Metrics.LongCounterBundle.LongCounter> networkOutboundUsageTotalStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, DeltaHistogram> networkInboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, DeltaHistogram> networkOutboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();
    private volatile long networkBaselineBandwidth = -1;
    private volatile NetworkBandwidthMode networkBandwidthMode = NetworkBandwidthMode.SEPARATE;

    private NetworkStats() {
        for (ThrottleStrategy strategy : ThrottleStrategy.values()) {
            Metrics.LongCounterBundle.LongCounter inboundUsage = NETWORK_INBOUND_USAGE
                .register(MetricsLevel.INFO, attributes(strategy));
            inboundUsage.add(0);
            networkInboundUsageTotalStats.put(strategy, inboundUsage);

            Metrics.LongCounterBundle.LongCounter outboundUsage = NETWORK_OUTBOUND_USAGE
                .register(MetricsLevel.INFO, attributes(strategy));
            outboundUsage.add(0);
            networkOutboundUsageTotalStats.put(strategy, outboundUsage);
        }
    }

    public static NetworkStats getInstance() {
        return INSTANCE;
    }

    /**
     * Configures broker-level network utilization calculation.
     */
    public void setup(long baselineBandwidth, NetworkBandwidthMode mode) {
        this.networkBaselineBandwidth = baselineBandwidth;
        this.networkBandwidthMode = mode;
    }

    public void recordNetworkUsageTotal(MeteredNetworkBandwidthLimiter.Direction direction, ThrottleStrategy strategy, long value) {
        Counter total = direction == MeteredNetworkBandwidthLimiter.Direction.INBOUND ? networkInboundUsageTotal : networkOutboundUsageTotal;
        total.inc(value);
        Map<ThrottleStrategy, Metrics.LongCounterBundle.LongCounter> stats = direction == MeteredNetworkBandwidthLimiter.Direction.INBOUND ? networkInboundUsageTotalStats : networkOutboundUsageTotalStats;
        Metrics.LongCounterBundle.LongCounter metric = stats.get(strategy);
        if (metric == null) {
            if (direction == MeteredNetworkBandwidthLimiter.Direction.INBOUND) {
                metric = stats.computeIfAbsent(strategy, k -> NETWORK_INBOUND_USAGE.register(MetricsLevel.INFO, attributes(strategy)));
            } else {
                metric = stats.computeIfAbsent(strategy, k -> NETWORK_OUTBOUND_USAGE.register(MetricsLevel.INFO, attributes(strategy)));
            }
        }
        metric.add(value);
    }

    public Optional<Counter> fastReadBytesStats(long streamId) {
        Pair<Counter, Counter> pair = streamReadBytesStats.getOrDefault(streamId, null);
        return pair == null ? Optional.empty() : Optional.of(pair.getLeft());
    }

    public Optional<Counter> slowReadBytesStats(long streamId) {
        Pair<Counter, Counter> pair = streamReadBytesStats.getOrDefault(streamId, null);
        return pair == null ? Optional.empty() : Optional.of(pair.getRight());
    }

    public Counter networkInboundUsageTotal() {
        return networkInboundUsageTotal;
    }

    public Counter networkOutboundUsageTotal() {
        return networkOutboundUsageTotal;
    }

    /**
     * Returns inbound network usage bytes per second using a minimum 30-second smoothing interval.
     */
    public double networkInboundRate() {
        return networkInboundRate.update(networkInboundUsageTotal.get());
    }

    /**
     * Returns outbound network usage bytes per second using a minimum 30-second smoothing interval.
     */
    public double networkOutboundRate() {
        return networkOutboundRate.update(networkOutboundUsageTotal.get());
    }

    /**
     * Returns the total inbound and outbound network usage bytes per second.
     */
    public double networkSharedRate() {
        return networkInboundRate() + networkOutboundRate();
    }

    /**
     * Returns broker-level network utilization using the latest configured baseline bandwidth and bandwidth mode.
     * Returns {@link Double#NaN} before a valid network stats setup.
     */
    public double networkUtil() {
        if (networkBaselineBandwidth <= 0) {
            return Double.NaN;
        }
        return calculateNetworkUtil(networkInboundRate(), networkOutboundRate(), networkBaselineBandwidth, networkBandwidthMode);
    }

    /**
     * Calculates network utilization from sampled directional rates and the configured bandwidth mode.
     */
    static double calculateNetworkUtil(double inboundRate, double outboundRate, long baselineBandwidth, NetworkBandwidthMode mode) {
        if (baselineBandwidth <= 0) {
            return Double.NaN;
        }
        double inboundUtil = inboundRate / baselineBandwidth;
        double outboundUtil = outboundRate / baselineBandwidth;
        if (mode == NetworkBandwidthMode.SHARED) {
            return inboundUtil + outboundUtil;
        }
        return Math.max(inboundUtil, outboundUtil);
    }

    public void createStreamReadBytesStats(long streamId) {
        streamReadBytesStats.putIfAbsent(streamId, new ImmutablePair<>(new Counter(), new Counter()));
    }

    public void removeStreamReadBytesStats(long streamId) {
        streamReadBytesStats.remove(streamId);
    }

    public Map<Long, Pair<Counter, Counter>> allStreamReadBytesStats() {
        return streamReadBytesStats;
    }

    public void recordNetworkLimiterQueueTime(MeteredNetworkBandwidthLimiter.Direction direction, ThrottleStrategy strategy, long value) {
        DeltaHistogram metric;
        if (direction == MeteredNetworkBandwidthLimiter.Direction.INBOUND) {
            metric = networkInboundLimiterQueueTimeStatsMap.get(strategy);
            if (metric == null) {
                metric = networkInboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy,
                    k -> NetworkInboundLimiterQueueTimeMetric.BUNDLE.histogram(MetricsLevel.INFO, attributes(strategy)));
            }
        } else {
            metric = networkOutboundLimiterQueueTimeStatsMap.get(strategy);
            if (metric == null) {
                metric = networkOutboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy,
                    k -> NetworkOutboundLimiterQueueTimeMetric.BUNDLE.histogram(MetricsLevel.INFO, attributes(strategy)));
            }
        }
        metric.record(value);
    }

    private static class NetworkInboundLimiterQueueTimeMetric {
        private static final Metrics.HistogramBundle BUNDLE = Metrics.instance()
            .histogram("kafka_stream_network_inbound_limiter_queue_time", "Network inbound limiter queue time", "nanoseconds");
    }

    private static class NetworkOutboundLimiterQueueTimeMetric {
        private static final Metrics.HistogramBundle BUNDLE = Metrics.instance()
            .histogram("kafka_stream_network_outbound_limiter_queue_time", "Network outbound limiter queue time", "nanoseconds");
    }

    private static Attributes attributes(ThrottleStrategy strategy) {
        return Attributes.builder()
            .put(LABEL_TYPE, strategy.getName())
            .build();
    }
}
