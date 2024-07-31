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

import kafka.autobalancer.common.Utils;

import java.util.Map;

public class Resource {
    public static final Double IGNORED_VALUE = -1.0;
    public static final byte NW_IN = (byte) 0;
    public static final byte NW_OUT = (byte) 1;

    public static final Map<Byte, String> HUMAN_READABLE_RESOURCE_NAMES = Map.of(
            NW_IN, "NWIn",
            NW_OUT, "NWOut"
    );

    public static String resourceString(byte type, double value) {
        if (!HUMAN_READABLE_RESOURCE_NAMES.containsKey(type)) {
            return "";
        }
        return HUMAN_READABLE_RESOURCE_NAMES.get(type) + "=" + valueString(type, value);
    }

    public static String valueString(byte type, double value) {
        String valueStr = String.valueOf(value);
        if (value == IGNORED_VALUE) {
            valueStr = "ignored";
        } else {
            switch (type) {
                case NW_IN:
                case NW_OUT:
                    valueStr = Utils.formatDataSize((long) value) + "/s";
                    break;
                default:
                    break;
            }
        }
        return valueStr;
    }
}
