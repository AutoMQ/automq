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
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import io.opentelemetry.api.common.Attributes;

public class YammerHistogramMetric extends ConfigurableMetrics {
    private final Histogram histogram;
    private final MetricsLevel currentMetricsLevel;

    public YammerHistogramMetric(MetricName metricName, MetricsLevel currentMetricsLevel, MetricsConfig metricsConfig) {
        this(metricName, currentMetricsLevel, metricsConfig, Attributes.empty());
    }

    public YammerHistogramMetric(MetricName metricName, MetricsLevel currentMetricsLevel, MetricsConfig metricsConfig, Attributes extraAttributes) {
        super(metricsConfig, extraAttributes);
        this.histogram = S3StreamMetricsManager.METRICS_REGISTRY.newHistogram(metricName, true);
        this.currentMetricsLevel = currentMetricsLevel;
    }

    public long count() {
        return histogram.count();
    }

    public long sum() {
        return (long) histogram.sum();
    }

    public double p50() {
        return histogram.getSnapshot().getMedian();
    }

    public double p99() {
        return histogram.getSnapshot().get99thPercentile();
    }

    public double mean() {
        return histogram.mean();
    }

    public double max() {
        return histogram.max();
    }

    public void record(long value) {
        histogram.update(value);
    }

    public boolean shouldRecord() {
        return currentMetricsLevel.isWithin(metricsLevel);
    }
}
