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
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.yammer.metrics.core.MetricName;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkStats {
    private volatile static NetworkStats instance = null;

    private final Map<ThrottleStrategy, CounterMetric> networkInboundUsageStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, CounterMetric> networkOutboundUsageStats = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, YammerHistogramMetric> networkInboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();
    private final Map<ThrottleStrategy, YammerHistogramMetric> networkOutboundLimiterQueueTimeStatsMap = new ConcurrentHashMap<>();

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

    public CounterMetric networkUsageStats(AsyncNetworkBandwidthLimiter.Type type, ThrottleStrategy strategy) {
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND
                ? networkInboundUsageStats.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkInboundUsageMetric(strategy))
                : networkOutboundUsageStats.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkOutboundUsageMetric(strategy));
    }

    public YammerHistogramMetric networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type type, ThrottleStrategy strategy) {
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND
                ? networkInboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkInboundLimiterQueueTimeMetric(
                        new MetricName(NetworkStats.class, "NetworkInboundLimiterQueueTime-" + strategy.getName()),
                        MetricsLevel.INFO, strategy))
                : networkOutboundLimiterQueueTimeStatsMap.computeIfAbsent(strategy, k -> S3StreamMetricsManager.buildNetworkOutboundLimiterQueueTimeMetric(
                        new MetricName(NetworkStats.class, "NetworkOutboundLimiterQueueTime-" + strategy.getName()),
                        MetricsLevel.INFO, strategy));
    }
}
