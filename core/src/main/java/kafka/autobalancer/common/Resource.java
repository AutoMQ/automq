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

package kafka.autobalancer.common;

import java.util.List;

public enum Resource {
    CPU("CPU", 0, 0.001),
    NW_IN("NWIn", 1, 10),
    NW_OUT("NWOut", 2, 10),
    UNKNOWN("UNKNOWN", 999, 0);

    public static final Double IGNORED_VALUE = -1.0;
    // EPSILON_PERCENT defines the acceptable nuance when comparing the utilization of the resource.
    // This nuance is generated due to precision loss when summing up float type utilization value.
    // In stress test we find that for cluster of around 800,000 replicas, the summed up nuance can be
    // more than 0.1% of sum value.
    private static final double EPSILON_PERCENT = 0.0008;
    private static final List<Resource> CACHED_VALUES = List.of(
            Resource.CPU,
            Resource.NW_IN,
            Resource.NW_OUT
    );
    private final String resource;
    private final int id;
    private final double epsilon;

    Resource(String resource, int id, double epsilon) {
        this.resource = resource;
        this.id = id;
        this.epsilon = epsilon;
    }

    public static Resource of(int id) {
        if (id < 0 || id >= CACHED_VALUES.size()) {
            return UNKNOWN;
        }
        return CACHED_VALUES.get(id);
    }

    public String resourceString(double value) {
        String valueStr = "";
        if (value == IGNORED_VALUE) {
            valueStr = "ignored";
        } else {
            switch (this) {
                case CPU:
                    valueStr = String.format("%.2f%%", value * 100);
                    break;
                case NW_IN:
                case NW_OUT:
                    valueStr = String.format("%.2fKB/s", value / 1024);
                    break;
                default:
                    break;
            }
        }
        return this.resource + "=" + valueStr;
    }

    /**
     * Use this instead of values() because values() creates a new array each time.
     *
     * @return enumerated values in the same order as values()
     */
    public static List<Resource> cachedValues() {
        return CACHED_VALUES;
    }

    /**
     * @return The resource type.
     */
    public String resource() {
        return resource;
    }

    /**
     * @return The resource id.
     */
    public int id() {
        return id;
    }

    /**
     * The epsilon value used in comparing the given values.
     *
     * @param value1 The first value used in comparison.
     * @param value2 The second value used in comparison.
     * @return The epsilon value used in comparing the given values.
     */
    public double epsilon(double value1, double value2) {
        return Math.max(epsilon, EPSILON_PERCENT * (value1 + value2));
    }

    @Override
    public String toString() {
        return resource;
    }
}

