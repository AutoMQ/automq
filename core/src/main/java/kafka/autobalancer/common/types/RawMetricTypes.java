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
import kafka.autobalancer.common.types.metrics.AbnormalQueueSize;

import java.util.Map;
import java.util.Set;

public class RawMetricTypes {
    public static final byte PARTITION_BYTES_IN = (byte) 0;
    public static final byte PARTITION_BYTES_OUT = (byte) 1;
    public static final byte PARTITION_SIZE = (byte) 2;
    public static final byte BROKER_APPEND_LATENCY_AVG_MS = (byte) 3;
    public static final byte BROKER_APPEND_QUEUE_SIZE = (byte) 4;
    public static final byte BROKER_FAST_READ_LATENCY_AVG_MS = (byte) 5;
    public static final byte BROKER_SLOW_READ_QUEUE_SIZE = (byte) 6;
    public static final Set<Byte> PARTITION_METRICS = Set.of(PARTITION_BYTES_IN, PARTITION_BYTES_OUT, PARTITION_SIZE);
    public static final Set<Byte> BROKER_METRICS = Set.of(BROKER_APPEND_LATENCY_AVG_MS, BROKER_APPEND_QUEUE_SIZE,
            BROKER_FAST_READ_LATENCY_AVG_MS, BROKER_SLOW_READ_QUEUE_SIZE);
    public static final Map<Byte, AbnormalMetric> ABNORMAL_METRICS = Map.of(
            BROKER_APPEND_LATENCY_AVG_MS, new AbnormalLatency(50),
            BROKER_APPEND_QUEUE_SIZE, new AbnormalQueueSize(100),
            BROKER_FAST_READ_LATENCY_AVG_MS, new AbnormalLatency(50),
            BROKER_SLOW_READ_QUEUE_SIZE, new AbnormalQueueSize(100)
    );

    public static AbnormalMetric ofAbnormalType(byte metricType) {
        return ABNORMAL_METRICS.get(metricType);
    }
}
