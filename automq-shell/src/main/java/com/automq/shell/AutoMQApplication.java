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

package com.automq.shell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AutoMQApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoMQApplication.class);
    private static final ConcurrentMap<Class<?>, Object> CONTAINER = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, String> CONTEXT = new ConcurrentHashMap<>();

    private static final String ATTR_CLUSTER_ID = "CLUSTER_ID";

    public static <T> boolean registerSingleton(Class<T> type, T singleton) {
        return registerSingleton(type, singleton, false);
    }

    public static <T> boolean registerSingleton(Class<T> type, T singleton, boolean override) {
        LOGGER.info("[AutoMQApplication] try to register singleton for class: {}", type.getSimpleName());
        if (override) {
            CONTAINER.put(type, singleton);
            return true;
        }
        return CONTAINER.putIfAbsent(type, singleton) == null;
    }

    public static <T> T getBean(Class<T> type) {
        return type.cast(CONTAINER.get(type));
    }

    public static void setAttribute(String key, String value) {
        LOGGER.info("[AutoMQApplication] try to set attribute {}: {}", key, value);
        CONTEXT.put(key, value);
    }

    public static String getAttribute(String key) {
        return CONTEXT.get(key);
    }

    public static void setClusterId(String clusterId) {
        setAttribute(ATTR_CLUSTER_ID, clusterId);
    }

    public static String getClusterId() {
        return getAttribute(ATTR_CLUSTER_ID);
    }
}
