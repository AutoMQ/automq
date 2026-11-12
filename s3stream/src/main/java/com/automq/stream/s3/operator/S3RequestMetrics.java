/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.AttributesUtils;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.metrics.stats.OperationLatencyMetrics;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches GET or PUT histograms by their enabled dimensions. Configuration is immutable;
 * disabled dimensions are collapsed before registration so they cannot duplicate exported series.
 */
final class S3RequestMetrics {
    private final S3Operation operation;
    private final boolean sourceEnabled;
    private final boolean readPurposeEnabled;
    private final Map<S3RequestSource, Map<S3ReadPurpose, Map<String, DeltaHistogram>>> successStats = new ConcurrentHashMap<>();
    private final Map<S3RequestSource, Map<S3ReadPurpose, Map<String, DeltaHistogram>>> failedStats = new ConcurrentHashMap<>();

    S3RequestMetrics(S3Operation operation, boolean sourceEnabled, boolean readPurposeEnabled) {
        this.operation = operation;
        this.sourceEnabled = sourceEnabled;
        this.readPurposeEnabled = sourceEnabled && readPurposeEnabled && operation == S3Operation.GET_OBJECT;
    }

    DeltaHistogram histogram(S3RequestSource source, S3ReadPurpose readPurpose, long size, boolean success) {
        S3RequestSource metricSource = sourceEnabled ? source : S3RequestSource.UNCLASSIFIED;
        S3ReadPurpose metricPurpose = hasReadPurpose(metricSource) ? readPurpose : S3ReadPurpose.OTHER;
        Map<String, DeltaHistogram> stats = (success ? successStats : failedStats)
            .computeIfAbsent(metricSource, ignored -> new ConcurrentHashMap<>())
            .computeIfAbsent(metricPurpose, ignored -> new ConcurrentHashMap<>());
        String sizeLabel = AttributesUtils.getObjectBucketLabel(size);
        return stats.computeIfAbsent(sizeLabel, label -> register(metricSource, metricPurpose, label, success));
    }

    private boolean hasReadPurpose(S3RequestSource source) {
        return readPurposeEnabled && source == S3RequestSource.ROUTER_WAL;
    }

    private DeltaHistogram register(S3RequestSource source, S3ReadPurpose readPurpose, String sizeLabel, boolean success) {
        String status = success ? S3StreamMetricsConstant.LABEL_STATUS_SUCCESS : S3StreamMetricsConstant.LABEL_STATUS_FAILED;
        if (!sourceEnabled) {
            return OperationLatencyMetrics.operation(MetricsLevel.INFO, operation.getType().getName(), operation.getName(), status, sizeLabel);
        }
        if (hasReadPurpose(source)) {
            return OperationLatencyMetrics.readRequest(MetricsLevel.INFO, status, sizeLabel, source, readPurpose);
        }
        return OperationLatencyMetrics.request(MetricsLevel.INFO, operation, status, sizeLabel, source);
    }
}
