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

package com.automq.stream.s3.trace;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.function.BiFunction;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;

public class AttributeBindings {
    private final BiFunction<AttributesBuilder, Object, AttributesBuilder>[] bindings;

    private AttributeBindings(BiFunction<AttributesBuilder, Object, AttributesBuilder>[] bindings) {
        this.bindings = bindings;
    }

    @SuppressWarnings("unchecked")
    public static AttributeBindings bind(Method method, String[] parametersNames) {
        Parameter[] parameters = method.getParameters();
        if (parameters.length != parametersNames.length) {
            return new AttributeBindings(null);
        }

        BiFunction<AttributesBuilder, Object, AttributesBuilder>[] bindings = new BiFunction[parametersNames.length];
        for (int i = 0; i < parametersNames.length; i++) {
            Parameter parameter = parameters[i];

            SpanAttribute spanAttribute = parameter.getAnnotation(SpanAttribute.class);
            if (spanAttribute == null) {
                bindings[i] = emptyBinding();
            } else {
                String attributeName = spanAttribute.value().isEmpty() ? parametersNames[i] : spanAttribute.value();
                bindings[i] = createBinding(attributeName, parameter.getParameterizedType());
            }
        }
        return new AttributeBindings(bindings);
    }

    static BiFunction<AttributesBuilder, Object, AttributesBuilder> emptyBinding() {
        return (builder, arg) -> builder;
    }

    static BiFunction<AttributesBuilder, Object, AttributesBuilder> createBinding(String name, Type type) {
        // Simple scalar parameter types
        if (type == String.class) {
            AttributeKey<String> key = AttributeKey.stringKey(name);
            return (builder, arg) -> builder.put(key, (String) arg);
        }
        if (type == long.class || type == Long.class) {
            AttributeKey<Long> key = AttributeKey.longKey(name);
            return (builder, arg) -> builder.put(key, (Long) arg);
        }
        if (type == double.class || type == Double.class) {
            AttributeKey<Double> key = AttributeKey.doubleKey(name);
            return (builder, arg) -> builder.put(key, (Double) arg);
        }
        if (type == boolean.class || type == Boolean.class) {
            AttributeKey<Boolean> key = AttributeKey.booleanKey(name);
            return (builder, arg) -> builder.put(key, (Boolean) arg);
        }
        if (type == int.class || type == Integer.class) {
            AttributeKey<Long> key = AttributeKey.longKey(name);
            return (builder, arg) -> builder.put(key, ((Integer) arg).longValue());
        }
        if (type == float.class || type == Float.class) {
            AttributeKey<Double> key = AttributeKey.doubleKey(name);
            return (builder, arg) -> builder.put(key, ((Float) arg).doubleValue());
        }

        // Default parameter types
        AttributeKey<String> key = AttributeKey.stringKey(name);
        return (builder, arg) -> builder.put(key, arg.toString());
    }

    public boolean isEmpty() {
        return bindings == null || bindings.length == 0;
    }

    public void apply(AttributesBuilder target, Object[] args) {
        if (args.length != bindings.length) {
            return;
        }

        for (int i = 0; i < args.length; i++) {
            bindings[i].apply(target, args[i]);
        }
    }
}
