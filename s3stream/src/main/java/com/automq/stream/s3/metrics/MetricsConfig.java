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
