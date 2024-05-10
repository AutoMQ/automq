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
    private volatile static NetworkStats instance = null;
    // <StreamId, <FastReadBytes, SlowReadBytes>>
    private final Map<Long, Pair<Counter, Counter>> streamReadBytesStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, CounterMetric> networkInboundUsageTotalStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, CounterMetric> networkOutboundUsageTotalStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, HistogramMetric> networkInboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, HistogramMetric> networkOutboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();

    private NetworkStats() {
    }

    public static NetworkStats getInstance() {
        if (instance == null) {
            synchronized (NetworkStats.class) {
                if (instance == null) {
                    instance = new NetworkStats();
                }
            }
        }
        return instance;
    }

    public CounterMetric networkUsageTotalStats(AsyncNetworkBandwidthLimiter.Type type, ThrottleStrategy strategy) {
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND
                ? networkInboundUsageTotalStats.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkInboundUsageMetric(strategy))
                : networkOutboundUsageTotalStats.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkOutboundUsageMetric(strategy));
    }

    public Optional<Counter> fastReadBytesStats(long streamId) {
        Pair<Counter, Counter> pair = streamReadBytesStats.getOrDefault(streamId, null);
        return pair == null ? Optional.empty() : Optional.of(pair.getLeft());
    }

    public Optional<Counter> slowReadBytesStats(long streamId) {
        Pair<Counter, Counter> pair = streamReadBytesStats.getOrDefault(streamId, null);
        return pair == null ? Optional.empty() : Optional.of(pair.getRight());
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
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND
                ? networkInboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkInboundLimiterQueueTimeMetric(MetricsLevel.INFO, strategy))
                : networkOutboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkOutboundLimiterQueueTimeMetric(MetricsLevel.INFO, strategy));
    }
}
