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

package org.apache.kafka.server.record;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public enum ErrorsTolerance {
    NONE("none"),
    INVALID_DATA("invalid_data"),
    ALL("all");

    private static final List<ErrorsTolerance> VALUES = asList(values());
    public final String name;

    ErrorsTolerance(String name) {
        this.name = name;
    }

    public static List<String> names() {
        return VALUES.stream().map(v -> v.name).collect(Collectors.toList());
    }

    public static ErrorsTolerance forName(String name) {
        if (name == null) {
            return INVALID_DATA;
        }
        String upperCaseName = name.toUpperCase(Locale.ROOT);
        try {
            return valueOf(upperCaseName);
        } catch (IllegalArgumentException e) {
            return INVALID_DATA;
        }
    }
}
