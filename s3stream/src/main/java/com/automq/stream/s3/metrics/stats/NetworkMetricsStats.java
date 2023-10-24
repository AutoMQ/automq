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

import com.automq.stream.s3.metrics.Counter;
import com.automq.stream.s3.metrics.Gauge;
import com.automq.stream.s3.metrics.S3StreamMetricsRegistry;
import com.automq.stream.s3.metrics.operations.S3MetricsType;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;

import java.util.Collections;

public class NetworkMetricsStats {

    public static final Counter NETWORK_INBOUND_USAGE = S3StreamMetricsRegistry.getMetricsGroup().newCounter(S3MetricsType.S3Network.getName(),
            S3MetricsType.S3Network.getName() + "InboundUsage", Collections.emptyMap());

    public static final Counter NETWORK_OUTBOUND_USAGE = S3StreamMetricsRegistry.getMetricsGroup().newCounter(S3MetricsType.S3Network.getName(),
            S3MetricsType.S3Network.getName() + "OutboundUsage", Collections.emptyMap());

    public static void registerNetworkInboundAvailableBandwidth(AsyncNetworkBandwidthLimiter.Type type, Gauge gauge) {
        S3StreamMetricsRegistry.getMetricsGroup().newGauge(S3MetricsType.S3Network.getName(),
                S3MetricsType.S3Network.getName() + type.getName() + "AvailableBandwidth", Collections.emptyMap(), gauge);
    }

    public static void registerNetworkLimiterQueueSize(AsyncNetworkBandwidthLimiter.Type type, Gauge gauge) {
        S3StreamMetricsRegistry.getMetricsGroup().newGauge(S3MetricsType.S3Network.getName(),
                S3MetricsType.S3Network.getName() + type.getName() + "QueueSize", Collections.emptyMap(), gauge);
    }
}
