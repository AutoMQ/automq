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
    CPU("CPU", 0),
    NW_IN("NWIn", 1),
    NW_OUT("NWOut", 2),
    QPS_IN("QPSIn", 3),
    QPS_OUT("QPSOut", 4),
    UNKNOWN("UNKNOWN", 999);

    public static final Double IGNORED_VALUE = -1.0;
    private static final List<Resource> CACHED_VALUES = List.of(
            Resource.CPU,
            Resource.NW_IN,
            Resource.NW_OUT
    );
    private final String resource;
    private final int id;

    Resource(String resource, int id) {
        this.resource = resource;
        this.id = id;
    }

    public static Resource of(int id) {
        if (id < 0 || id >= CACHED_VALUES.size()) {
            return UNKNOWN;
        }
        return CACHED_VALUES.get(id);
    }

    public String resourceString(double value) {
        String valueStr = String.valueOf(value);
        if (value == IGNORED_VALUE) {
            valueStr = "ignored";
        } else {
            switch (this) {
                case CPU:
                    valueStr = String.format("%.2f%%", value * 100);
                    break;
                case NW_IN:
                case NW_OUT:
                    valueStr = Utils.formatDataSize((long) value) + "/s";
                    break;
                case QPS_IN:
                case QPS_OUT:
                    valueStr = String.format("%.2f/s", value);
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

    @Override
    public String toString() {
        return resource;
    }
}

