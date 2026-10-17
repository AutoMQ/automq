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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * Shared declaration and registration point for the kafka_stream_operation_latency metric.
 */
public final class OperationLatencyMetrics {
    private static final AttributeKey<String> LABEL_OPERATION_TYPE = AttributeKey.stringKey("operation_type");
    private static final AttributeKey<String> LABEL_OPERATION_NAME = AttributeKey.stringKey("operation_name");
    private static final AttributeKey<String> LABEL_SIZE_NAME = AttributeKey.stringKey("size");
    private static final AttributeKey<String> LABEL_STAGE = AttributeKey.stringKey("stage");
    private static final AttributeKey<String> LABEL_STATUS = AttributeKey.stringKey("status");
    private static final Metrics.HistogramBundle OPERATION_LATENCY = Metrics.instance()
        .histogram("kafka_stream_operation_latency", "Operation latency", "nanoseconds");

    private OperationLatencyMetrics() {
    }

    public static DeltaHistogram operation(MetricsLevel metricsLevel, S3Operation operation) {
        return operation(metricsLevel, operation.getType().getName(), operation.getName());
    }

    public static DeltaHistogram operation(MetricsLevel metricsLevel, S3Operation operation, String status) {
        return operation(metricsLevel, operation.getType().getName(), operation.getName(), status);
    }

    public static DeltaHistogram operation(MetricsLevel metricsLevel, String operationType, String operationName) {
        return OPERATION_LATENCY.histogram(metricsLevel, operationAttributes(operationType, operationName));
    }

    public static DeltaHistogram operation(MetricsLevel metricsLevel, String operationType, String operationName,
        String status) {
        return OPERATION_LATENCY.histogram(metricsLevel, operationAttributes(operationType, operationName, status));
    }

    public static DeltaHistogram operation(MetricsLevel metricsLevel, String operationType, String operationName,
        String status, String sizeLabelName) {
        return OPERATION_LATENCY.histogram(metricsLevel,
            operationAttributes(operationType, operationName, status, sizeLabelName));
    }

    public static DeltaHistogram stage(MetricsLevel metricsLevel, S3Stage stage) {
        return OPERATION_LATENCY.histogram(metricsLevel, Attributes.builder()
            .putAll(operationAttributes(stage.getOperation().getType().getName(), stage.getOperation().getName()))
            .put(LABEL_STAGE, stage.getName())
            .build());
    }

    private static Attributes operationAttributes(String operationType, String operationName) {
        return Attributes.builder()
            .put(LABEL_OPERATION_TYPE, operationType)
            .put(LABEL_OPERATION_NAME, operationName)
            .build();
    }

    private static Attributes operationAttributes(String operationType, String operationName, String status) {
        return Attributes.builder()
            .putAll(operationAttributes(operationType, operationName))
            .put(LABEL_STATUS, status)
            .build();
    }

    private static Attributes operationAttributes(String operationType, String operationName, String status,
        String sizeLabelName) {
        return Attributes.builder()
            .putAll(operationAttributes(operationType, operationName, status))
            .put(LABEL_SIZE_NAME, sizeLabelName)
            .build();
    }
}
