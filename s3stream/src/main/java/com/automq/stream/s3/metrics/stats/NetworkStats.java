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

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.wrapper.Counter;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkStats {
    private static final NetworkStats INSTANCE = new NetworkStats();
    // <StreamId, <FastReadBytes, SlowReadBytes>>
    private final Map<Long, Pair<Counter, Counter>> streamReadBytesStats = new ConcurrentHashMap<>();
    private final Counter networkInboundUsageTotal = new Counter();
    private final Counter networkOutboundUsageTotal = new Counter();
    private final Map<ThrottleStrategy, CounterMetric> networkInboundUsageTotalStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, CounterMetric> networkOutboundUsageTotalStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, HistogramMetric> networkInboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, HistogramMetric> networkOutboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();

    private NetworkStats() {
    }

    public static NetworkStats getInstance() {
        return INSTANCE;
    }

    public CounterMetric networkUsageTotalStats(AsyncNetworkBandwidthLimiter.Type type, ThrottleStrategy strategy) {
        Map<ThrottleStrategy, CounterMetric> stats = type == AsyncNetworkBandwidthLimiter.Type.INBOUND ? networkInboundUsageTotalStats : networkOutboundUsageTotalStats;
        CounterMetric metric = stats.get(strategy);
        if (metric == null) {
            if (type == AsyncNetworkBandwidthLimiter.Type.INBOUND) {
                metric = stats.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkInboundUsageMetric(strategy, networkInboundUsageTotal::inc));
            } else {
                metric = stats.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkOutboundUsageMetric(strategy, networkOutboundUsageTotal::inc));
            }
        }
        return metric;
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

    public void createStreamReadBytesStats(long streamId) {
        streamReadBytesStats.putIfAbsent(streamId, new ImmutablePair<>(new Counter(), new Counter()));
    }

    public void removeStreamReadBytesStats(long streamId) {
        streamReadBytesStats.remove(streamId);
    }

    public Map<Long, Pair<Counter, Counter>> allStreamReadBytesStats() {
        return streamReadBytesStats;
    }

    public HistogramMetric networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type type, ThrottleStrategy strategy) {
        HistogramMetric metric;
        if (type == AsyncNetworkBandwidthLimiter.Type.INBOUND) {
            metric = networkInboundLimiterQueueTimeStatsMap.get(strategy);
            if (metric == null) {
                metric = networkInboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkInboundLimiterQueueTimeMetric(MetricsLevel.INFO, strategy));
            }
        } else {
            metric = networkOutboundLimiterQueueTimeStatsMap.get(strategy);
            if (metric == null) {
                metric = networkOutboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkOutboundLimiterQueueTimeMetric(MetricsLevel.INFO, strategy));
            }
        }
        return metric;
    }
}
