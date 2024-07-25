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

package com.automq.stream.utils;

import org.apache.commons.lang3.StringUtils;

public class Systems {
    public static final int CPU_CORES = Runtime.getRuntime().availableProcessors();

    public static long getEnvLong(String name, long defaultValue) {
        String value = System.getenv(name);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    public static int getEnvInt(String name, int defaultValue) {
        String value = System.getenv(name);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
}
