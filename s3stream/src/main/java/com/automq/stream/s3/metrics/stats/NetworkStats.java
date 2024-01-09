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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;

public class NetworkStats {
    private volatile static NetworkStats instance = null;

    private final CounterMetric networkInboundUsageStats = S3StreamMetricsManager.buildNetworkInboundUsageMetric();
    private final CounterMetric networkOutboundUsageStats = S3StreamMetricsManager.buildNetworkOutboundUsageMetric();
    private final HistogramMetric networkInboundLimiterQueueTimeStats = S3StreamMetricsManager.buildNetworkInboundLimiterQueueTimeMetric();
    private final HistogramMetric networkOutboundLimiterQueueTimeStats = S3StreamMetricsManager.buildNetworkOutboundLimiterQueueTimeMetric();

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

    public CounterMetric networkUsageStats(AsyncNetworkBandwidthLimiter.Type type) {
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND ? networkInboundUsageStats : networkOutboundUsageStats;
    }

    public HistogramMetric networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type type) {
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND ? networkInboundLimiterQueueTimeStats : networkOutboundLimiterQueueTimeStats;
    }
}
