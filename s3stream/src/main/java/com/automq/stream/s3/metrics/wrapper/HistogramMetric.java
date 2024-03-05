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
import io.opentelemetry.api.metrics.LongHistogram;

public class HistogramMetric extends ConfigurableMetrics {
    private final LongHistogram longHistogram;

    public HistogramMetric(MetricsConfig metricsConfig, LongHistogram longHistogram) {
        this(metricsConfig, Attributes.empty(), longHistogram);
    }

    public HistogramMetric(MetricsConfig metricsConfig, Attributes extraAttributes, LongHistogram longHistogram) {
        super(metricsConfig, extraAttributes);
        this.longHistogram = longHistogram;
    }

    public boolean record(MetricsLevel metricsLevel, long value) {
        if (metricsLevel.isWithin(this.metricsLevel)) {
            longHistogram.record(value, attributes);
            return true;
        }
        return false;
    }
}
