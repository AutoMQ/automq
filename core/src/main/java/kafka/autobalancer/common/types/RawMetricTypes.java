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

package kafka.autobalancer.common.types;

import kafka.autobalancer.common.types.metrics.AbnormalLatency;
import kafka.autobalancer.common.types.metrics.AbnormalMetric;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RawMetricTypes {
    public static final byte PARTITION_BYTES_IN = (byte) 0;
    public static final byte PARTITION_BYTES_OUT = (byte) 1;
    public static final byte PARTITION_SIZE = (byte) 2;
    public static final byte BROKER_APPEND_LATENCY_AVG_MS = (byte) 3;
    public static final byte BROKER_MAX_PENDING_APPEND_LATENCY_MS = (byte) 4;
    public static final byte BROKER_MAX_PENDING_FETCH_LATENCY_MS = (byte) 5;
    public static final byte BROKER_METRIC_VERSION = (byte) 6;
    public static final Set<Byte> PARTITION_METRICS = Set.of(PARTITION_BYTES_IN, PARTITION_BYTES_OUT, PARTITION_SIZE);
    public static final Set<Byte> BROKER_METRICS = Set.of(BROKER_APPEND_LATENCY_AVG_MS,
            BROKER_MAX_PENDING_APPEND_LATENCY_MS, BROKER_MAX_PENDING_FETCH_LATENCY_MS, BROKER_METRIC_VERSION);
    public static final Map<Byte, AbnormalMetric> ABNORMAL_METRICS = Map.of(
            BROKER_APPEND_LATENCY_AVG_MS, new AbnormalLatency(100), // 100ms
            BROKER_MAX_PENDING_APPEND_LATENCY_MS, new AbnormalLatency(10000), // 10s
            BROKER_MAX_PENDING_FETCH_LATENCY_MS, new AbnormalLatency(10000)  // 10s
    );

    public static Set<Byte> requiredPartitionMetrics(MetricVersion metricVersion) {
        // same partition metrics requirement for ALL metric versions
        return PARTITION_METRICS;
    }

    public static Set<Byte> requiredBrokerMetrics(MetricVersion metricVersion) {
        if (metricVersion.isAfter(MetricVersion.V0)) {
            return BROKER_METRICS;
        }
        return Collections.emptySet();
    }

    public static AbnormalMetric ofAbnormalType(byte metricType) {
        return ABNORMAL_METRICS.get(metricType);
    }
}
