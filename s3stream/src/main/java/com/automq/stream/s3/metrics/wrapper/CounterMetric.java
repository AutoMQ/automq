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

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

import java.util.function.Supplier;

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
