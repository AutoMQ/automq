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

package com.automq.opentelemetry.exporter.s3;

import com.automq.opentelemetry.exporter.MetricsExportConfig;
import com.automq.opentelemetry.exporter.MetricsExporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

/**
 * An adapter class that implements the MetricsExporter interface and uses S3MetricsExporter
 * for actual metrics exporting functionality.
 */
public class S3MetricsExporterAdapter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetricsExporterAdapter.class);

    private final MetricsExportConfig metricsExportConfig;

    /**
     * Creates a new S3MetricsExporterAdapter.
     *
     * @param metricsExportConfig The configuration for the S3 metrics exporter.
     */
    public S3MetricsExporterAdapter(MetricsExportConfig metricsExportConfig) {
        this.metricsExportConfig = metricsExportConfig;
        LOGGER.info("S3MetricsExporterAdapter initialized with labels :{}", metricsExportConfig.baseLabels());
    }

    @Override
    public MetricReader asMetricReader() {
        // Create and start the S3MetricsExporter
        S3MetricsExporter s3MetricsExporter = new S3MetricsExporter(metricsExportConfig);
        s3MetricsExporter.start();

        // Create and return the periodic metric reader
        return PeriodicMetricReader.builder(s3MetricsExporter)
                .setInterval(Duration.ofMillis(metricsExportConfig.intervalMs()))
                .build();
    }
}