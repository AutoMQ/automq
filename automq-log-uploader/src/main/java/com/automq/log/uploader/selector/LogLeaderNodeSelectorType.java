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

package com.automq.log.uploader.selector;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Supported selector types.
 */
public enum LogLeaderNodeSelectorType {
    CUSTOM(null);

    private static final Map<String, LogLeaderNodeSelectorType> LOOKUP = new HashMap<>();

    static {
        for (LogLeaderNodeSelectorType value : values()) {
            if (value.type != null) {
                LOOKUP.put(value.type, value);
            }
        }
    }

    private final String type;

    LogLeaderNodeSelectorType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static LogLeaderNodeSelectorType fromString(String type) {
        if (type == null) {
            return CUSTOM;
        }
        return LOOKUP.getOrDefault(type.toLowerCase(Locale.ROOT), CUSTOM);
    }
}
