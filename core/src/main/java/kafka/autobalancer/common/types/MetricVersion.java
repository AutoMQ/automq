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

import java.util.Collections;
import java.util.Set;
import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.goals.NetworkInUsageDistributionGoal;
import kafka.autobalancer.goals.NetworkOutUsageDistributionGoal;

import java.util.Objects;

import static kafka.autobalancer.common.types.RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS;
import static kafka.autobalancer.common.types.RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS;
import static kafka.autobalancer.common.types.RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS;
import static kafka.autobalancer.common.types.RawMetricTypes.BROKER_METRIC_VERSION;
import static kafka.autobalancer.common.types.RawMetricTypes.PARTITION_BYTES_IN;
import static kafka.autobalancer.common.types.RawMetricTypes.PARTITION_BYTES_OUT;
import static kafka.autobalancer.common.types.RawMetricTypes.PARTITION_SIZE;

public class MetricVersion {
    public static final MetricVersion V0 = new MetricVersion((short) 0,
        Collections.emptySet(),
        Set.of(PARTITION_BYTES_IN, PARTITION_BYTES_OUT, PARTITION_SIZE));
    public static final MetricVersion V1 = new MetricVersion((short) 1,
        Set.of(BROKER_APPEND_LATENCY_AVG_MS, BROKER_MAX_PENDING_APPEND_LATENCY_MS, BROKER_MAX_PENDING_FETCH_LATENCY_MS, BROKER_METRIC_VERSION),
        Set.of(PARTITION_BYTES_IN, PARTITION_BYTES_OUT, PARTITION_SIZE)
    );
    public static final MetricVersion V2 = new MetricVersion((short) 2,
        Set.of(BROKER_APPEND_LATENCY_AVG_MS, BROKER_METRIC_VERSION),
        Set.of(PARTITION_BYTES_IN, PARTITION_BYTES_OUT, PARTITION_SIZE)
    );
    public static final MetricVersion LATEST_VERSION = V2;
    private final short value;
    private final Set<Byte> requiredBrokerMetrics;
    private final Set<Byte> requiredPartitionMetrics;

    public MetricVersion(short version, Set<Byte> requiredBrokerMetrics, Set<Byte> requiredPartitionMetrics) {
        this.value = version;
        this.requiredBrokerMetrics = requiredBrokerMetrics;
        this.requiredPartitionMetrics = requiredPartitionMetrics;
    }

    public static MetricVersion of(short version) {
        switch (version) {
            case 0:
                return V0;
            case 1:
                return V1;
            case 2:
                return V2;
            default:
                throw new IllegalArgumentException("Unknown metric version: " + version);
        }
    }

    public short value() {
        return value;
    }

    public Set<Byte> requiredBrokerMetrics() {
        return requiredBrokerMetrics;
    }

    public Set<Byte> requiredPartitionMetrics() {
        return requiredPartitionMetrics;
    }

    public boolean isSlowBrokerSupported() {
        return isAfter(V0);
    }

    public boolean isGoalSupported(Goal goal) {
        if (goal == null) {
            return false;
        }
        return goal.name().equals(NetworkInUsageDistributionGoal.class.getSimpleName())
                || goal.name().equals(NetworkOutUsageDistributionGoal.class.getSimpleName());
    }

    public boolean isAfter(MetricVersion other) {
        return value > other.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricVersion)) {
            return false;
        }

        MetricVersion that = (MetricVersion) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
