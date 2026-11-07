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

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;

import java.util.function.Consumer;
import java.util.function.Supplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

public class CounterMetric extends ConfigurableMetric {
    private final Supplier<LongCounter> longCounterSupplier;
    private final Consumer<Long> onAdded;

    public CounterMetric(MetricsConfig metricsConfig, Supplier<LongCounter> longCounterSupplier) {
        this(metricsConfig, Attributes.empty(), longCounterSupplier);
    }

    public CounterMetric(MetricsConfig metricsConfig, Attributes extraAttributes, Supplier<LongCounter> longCounterSupplier) {
        this(metricsConfig, extraAttributes, longCounterSupplier, v -> { });
    }

    public CounterMetric(MetricsConfig metricsConfig, Attributes extraAttributes, Supplier<LongCounter> longCounterSupplier, Consumer<Long> onAdded) {
        super(metricsConfig, extraAttributes);
        this.longCounterSupplier = longCounterSupplier;
        this.onAdded = onAdded;
    }

    public boolean add(MetricsLevel metricsLevel, long value) {
        onAdded.accept(value);
        if (metricsLevel.isWithin(this.metricsLevel)) {
            longCounterSupplier.get().add(value, attributes);
            return true;
        }
        return false;
    }
}
