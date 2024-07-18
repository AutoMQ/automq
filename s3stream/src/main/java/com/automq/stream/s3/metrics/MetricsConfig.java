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

package com.automq.stream.s3.metrics;

import io.opentelemetry.api.common.Attributes;

public class MetricsConfig {
    private static final long DEFAULT_METRICS_REPORT_INTERVAL_MS = 5000;
    private MetricsLevel metricsLevel;
    private Attributes baseAttributes;
    private long metricsReportIntervalMs;

    public MetricsConfig() {
        this(MetricsLevel.INFO, Attributes.empty());
    }

    public MetricsConfig(MetricsLevel metricsLevel, Attributes baseAttributes) {
        this(metricsLevel, baseAttributes, DEFAULT_METRICS_REPORT_INTERVAL_MS);
    }

    public MetricsConfig(MetricsLevel metricsLevel, Attributes baseAttributes, long interval) {
        this.metricsLevel = metricsLevel;
        this.baseAttributes = baseAttributes;
        this.metricsReportIntervalMs = interval;
    }

    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }

    public Attributes getBaseAttributes() {
        return baseAttributes;
    }

    public long getMetricsReportIntervalMs() {
        return metricsReportIntervalMs;
    }

    public void setMetricsLevel(MetricsLevel metricsLevel) {
        this.metricsLevel = metricsLevel;
    }

    public void setBaseAttributes(Attributes baseAttributes) {
        this.baseAttributes = baseAttributes;
    }

    public void setMetricsReportIntervalMs(long interval) {
        this.metricsReportIntervalMs = interval;
    }
}
