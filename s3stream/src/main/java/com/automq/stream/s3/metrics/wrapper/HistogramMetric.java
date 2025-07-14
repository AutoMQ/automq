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
