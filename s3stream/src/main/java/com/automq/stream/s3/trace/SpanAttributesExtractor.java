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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

public final class SpanAttributesExtractor {

    private final MethodCache<AttributeBindings> cache;

    SpanAttributesExtractor(MethodCache<AttributeBindings> cache) {
        this.cache = cache;
    }

    public static SpanAttributesExtractor create() {
        return new SpanAttributesExtractor(new MethodCache<>());
    }

    public Attributes extract(Method method, String[] parametersNames, Object[] args) {
        AttributesBuilder attributes = Attributes.builder();
        AttributeBindings bindings =
            cache.computeIfAbsent(method, (Method m) -> AttributeBindings.bind(m, parametersNames));
        if (!bindings.isEmpty()) {
            bindings.apply(attributes, args);
        }
        return attributes.build();
    }
}
