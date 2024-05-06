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

public class CounterMetric extends ConfigurableMetric {
    private final LongCounter longCounter;

    public CounterMetric(MetricsConfig metricsConfig, LongCounter longCounter) {
        super(metricsConfig, Attributes.empty());
        this.longCounter = longCounter;
    }

    public CounterMetric(MetricsConfig metricsConfig, Attributes extraAttributes, LongCounter longCounter) {
        super(metricsConfig, extraAttributes);
        this.longCounter = longCounter;
    }

    public boolean add(MetricsLevel metricsLevel, long value) {
        if (metricsLevel.isWithin(this.metricsLevel)) {
            longCounter.add(value, attributes);
            return true;
        }
        return false;
    }
}
