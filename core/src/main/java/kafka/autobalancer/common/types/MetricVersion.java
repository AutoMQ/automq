/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.common.types;

import kafka.autobalancer.goals.Goal;
import kafka.autobalancer.goals.NetworkInUsageDistributionGoal;
import kafka.autobalancer.goals.NetworkOutUsageDistributionGoal;

import java.util.Objects;

public class MetricVersion {
    public static final MetricVersion V0 = new MetricVersion((short) 0);
    public static final MetricVersion V1 = new MetricVersion((short) 1);
    public static final MetricVersion LATEST_VERSION = V1;
    private final short value;

    public MetricVersion(short version) {
        this.value = version;
    }

    public short value() {
        return value;
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
