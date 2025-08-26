/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.stream.utils;

import org.apache.commons.lang3.StringUtils;

import io.netty.util.internal.PlatformDependent;

public class Systems {
    public static final int CPU_CORES = Runtime.getRuntime().availableProcessors();
    public static final long HEAP_MEMORY_SIZE = Runtime.getRuntime().maxMemory();
    public static final long DIRECT_MEMORY_SIZE = PlatformDependent.maxDirectMemory();

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

    public static boolean getEnvBool(String name, boolean defaultValue) {
        String value = System.getenv(name);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }
}
