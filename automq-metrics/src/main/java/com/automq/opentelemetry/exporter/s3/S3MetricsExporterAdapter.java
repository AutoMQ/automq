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

import com.automq.opentelemetry.exporter.MetricsExporter;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

/**
 * An adapter class that implements the MetricsExporter interface and uses S3MetricsExporter
 * for actual metrics exporting functionality.
 */
public class S3MetricsExporterAdapter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetricsExporterAdapter.class);

    private final String clusterId;
    private final int nodeId;
    private final int intervalMs;
    private final BucketURI metricsBucket;
    private final List<Pair<String, String>> baseLabels;
    private final UploaderNodeSelector nodeSelector;

    /**
     * Creates a new S3MetricsExporterAdapter.
     *
     * @param clusterId The cluster ID
     * @param nodeId The node ID
     * @param intervalMs The interval in milliseconds for metrics export
     * @param metricsBucket The bucket URI to export metrics to
     * @param baseLabels The base labels to include with metrics
     * @param nodeSelector The selector that determines if this node should upload metrics
     */
    public S3MetricsExporterAdapter(String clusterId, int nodeId, int intervalMs, BucketURI metricsBucket,
                                    List<Pair<String, String>> baseLabels, UploaderNodeSelector nodeSelector) {
        if (metricsBucket == null) {
            throw new IllegalArgumentException("bucket URI must be provided for s3 metrics exporter");
        }
        if (nodeSelector == null) {
            throw new IllegalArgumentException("node selector must be provided");
        }
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.intervalMs = intervalMs;
        this.metricsBucket = metricsBucket;
        this.baseLabels = baseLabels;
        this.nodeSelector = nodeSelector;
        LOGGER.info("S3MetricsExporterAdapter initialized with clusterId: {}, nodeId: {}, intervalMs: {}, bucket: {}",
                clusterId, nodeId, intervalMs, metricsBucket);
    }

    @Override
    public MetricReader asMetricReader() {
        // Create object storage for the bucket
        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(metricsBucket).threadPrefix("s3-metric").build();

        S3MetricsConfig metricsConfig = new S3MetricsConfig() {
            @Override
            public String clusterId() {
                return clusterId;
            }

            @Override
            public boolean isPrimaryUploader() {
                return nodeSelector.isPrimaryUploader();
            }

            @Override
            public int nodeId() {
                return nodeId;
            }

            @Override
            public ObjectStorage objectStorage() {
                return objectStorage;
            }

            @Override
            public List<Pair<String, String>> baseLabels() {
                return baseLabels;
            }
        };

        // Create and start the S3MetricsExporter
        S3MetricsExporter s3MetricsExporter = new S3MetricsExporter(metricsConfig);
        s3MetricsExporter.start();

        // Create and return the periodic metric reader
        return PeriodicMetricReader.builder(s3MetricsExporter)
                .setInterval(Duration.ofMillis(intervalMs))
                .build();
    }
}