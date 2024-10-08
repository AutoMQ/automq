/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;

import java.util.function.Supplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

public class CounterMetric extends ConfigurableMetric {
    private final Supplier<LongCounter> longCounterSupplier;

    public CounterMetric(MetricsConfig metricsConfig, Supplier<LongCounter> longCounterSupplier) {
        super(metricsConfig, Attributes.empty());
        this.longCounterSupplier = longCounterSupplier;
    }

    public CounterMetric(MetricsConfig metricsConfig, Attributes extraAttributes, Supplier<LongCounter> longCounterSupplier) {
        super(metricsConfig, extraAttributes);
        this.longCounterSupplier = longCounterSupplier;
    }

    public boolean add(MetricsLevel metricsLevel, long value) {
        if (metricsLevel.isWithin(this.metricsLevel)) {
            longCounterSupplier.get().add(value, attributes);
            return true;
        }
        return false;
    }
}
