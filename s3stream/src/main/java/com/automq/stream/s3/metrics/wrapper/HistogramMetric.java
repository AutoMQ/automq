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

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import io.opentelemetry.api.common.Attributes;

public class HistogramMetric extends ConfigurableMetric {
    private final DeltaHistogram deltaHistogram;
    private final MetricsLevel currentMetricsLevel;

    public HistogramMetric(MetricsLevel currentMetricsLevel, MetricsConfig metricsConfig) {
        this(currentMetricsLevel, metricsConfig, Attributes.empty());
    }

    public HistogramMetric(MetricsLevel currentMetricsLevel, MetricsConfig metricsConfig, Attributes extraAttributes) {
        super(metricsConfig, extraAttributes);
        this.deltaHistogram = new DeltaHistogram();
        this.currentMetricsLevel = currentMetricsLevel;
    }

    public long count() {
        return deltaHistogram.cumulativeCount();
    }

    public long sum() {
        return deltaHistogram.cumulativeSum();
    }

    public double p50() {
        return deltaHistogram.p50();
    }

    public double p95() {
        return deltaHistogram.p95();
    }

    public double p99() {
        return deltaHistogram.p99();
    }

    public double max() {
        return deltaHistogram.max();
    }

    public double min() {
        return deltaHistogram.min();
    }

    public void record(long value) {
        deltaHistogram.record(value);
    }

    public boolean shouldRecord() {
        return currentMetricsLevel.isWithin(metricsLevel);
    }

    @Override
    public void onConfigChange(MetricsConfig metricsConfig) {
        super.onConfigChange(metricsConfig);
        deltaHistogram.setSnapshotInterval(metricsConfig.getMetricsReportIntervalMs());
    }

    DeltaHistogram getDeltaHistogram() {
        return deltaHistogram;
    }
}
