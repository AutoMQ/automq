/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.common.types;

import java.util.Map;
import kafka.autobalancer.common.types.metrics.AbnormalLatency;
import kafka.autobalancer.common.types.metrics.AbnormalMetric;

public class RawMetricTypes {
    public static final byte PARTITION_BYTES_IN = (byte) 0;
    public static final byte PARTITION_BYTES_OUT = (byte) 1;
    public static final byte PARTITION_SIZE = (byte) 2;
    public static final byte BROKER_APPEND_LATENCY_AVG_MS = (byte) 3;
    public static final byte BROKER_MAX_PENDING_APPEND_LATENCY_MS = (byte) 4;
    public static final byte BROKER_MAX_PENDING_FETCH_LATENCY_MS = (byte) 5;
    public static final byte BROKER_METRIC_VERSION = (byte) 6;
    public static final Map<Byte, AbnormalMetric> ABNORMAL_METRICS = Map.of(
            BROKER_APPEND_LATENCY_AVG_MS, new AbnormalLatency(100), // 100ms
            BROKER_MAX_PENDING_APPEND_LATENCY_MS, new AbnormalLatency(10000), // 10s
            BROKER_MAX_PENDING_FETCH_LATENCY_MS, new AbnormalLatency(10000)  // 10s
    );

    public static AbnormalMetric ofAbnormalType(byte metricType) {
        return ABNORMAL_METRICS.get(metricType);
    }
}
