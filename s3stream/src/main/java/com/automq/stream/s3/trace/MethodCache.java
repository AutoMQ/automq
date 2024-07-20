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

package com.automq.stream.s3.trace;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

final class MethodCache<V> extends ClassValue<Map<Method, V>> {
    public V computeIfAbsent(Method key, Function<? super Method, ? extends V> mappingFunction) {
        return this.get(key.getDeclaringClass()).computeIfAbsent(key, mappingFunction);
    }

    @Override
    protected Map<Method, V> computeValue(Class<?> type) {
        return new ConcurrentHashMap<>();
    }
}
