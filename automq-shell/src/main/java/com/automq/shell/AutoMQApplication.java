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

package com.automq.shell;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AutoMQApplication {
    private static final ConcurrentMap<Class<?>, Object> CONTAINER = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, String> CONTEXT = new ConcurrentHashMap<>();

    private static final String ATTR_CLUSTER_ID = "CLUSTER_ID";

    public static <T> boolean registerSingleton(Class<T> type, T singleton) {
        return registerSingleton(type, singleton, false);
    }

    public static <T> boolean registerSingleton(Class<T> type, T singleton, boolean override) {
        if (override) {
            CONTAINER.put(type, singleton);
            return true;
        } else {
            return CONTAINER.putIfAbsent(type, singleton) == null;
        }
    }

    public static <T> T getBean(Class<T> type) {
        return type.cast(CONTAINER.get(type));
    }

    public static void setAttribute(String key, String value) {
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
