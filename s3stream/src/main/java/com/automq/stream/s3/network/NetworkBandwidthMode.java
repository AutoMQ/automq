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

package com.automq.stream.s3.network;

import java.util.Locale;

/**
 * Describes whether inbound and outbound network traffic use separate or shared bandwidth buckets.
 */
public enum NetworkBandwidthMode {
    SEPARATE("separate"),
    SHARED("shared");

    private final String name;

    NetworkBandwidthMode(String name) {
        this.name = name;
    }

    /**
     * Returns the config value used to select this mode.
     */
    public String getName() {
        return name;
    }

    /**
     * Parses a config value into a network bandwidth mode.
     *
     * @throws IllegalArgumentException if the value is not a supported mode.
     */
    public static NetworkBandwidthMode parse(String value) {
        String normalized = value == null ? null : value.trim().toLowerCase(Locale.ROOT);
        for (NetworkBandwidthMode mode : values()) {
            if (mode.name.equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT,
            "Unsupported network bandwidth mode: %s", value));
    }
}
