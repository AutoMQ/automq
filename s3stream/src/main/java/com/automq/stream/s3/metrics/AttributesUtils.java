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

import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.network.ThrottleStrategy;

import io.opentelemetry.api.common.Attributes;

public class AttributesUtils {

    public static Attributes buildOperationAttributes(String operationType, String operationName) {
        return Attributes.builder()
            .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operationType)
            .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operationName)
            .build();
    }

    public static Attributes buildOperationAttributesWithStatus(String operationType, String operationName, String status) {
        return Attributes.builder()
            .putAll(buildOperationAttributes(operationType, operationName))
            .put(S3StreamMetricsConstant.LABEL_STATUS, status)
            .build();
    }

    public static Attributes buildAttributesStage(String stage) {
        return Attributes.builder()
                .put(S3StreamMetricsConstant.LABEL_STAGE, stage)
                .build();
    }

    public static Attributes buildAttributes(String status) {
        return Attributes.builder()
                .put(S3StreamMetricsConstant.LABEL_STATUS, status)
                .build();
    }

    public static Attributes buildAttributes(ThrottleStrategy strategy) {
        return Attributes.builder()
                .put(S3StreamMetricsConstant.LABEL_TYPE, strategy.getName())
                .build();
    }

    public static Attributes buildStatusStageAttributes(String status, String stage) {
        return Attributes.builder()
                .put(S3StreamMetricsConstant.LABEL_STATUS, status)
                .put(S3StreamMetricsConstant.LABEL_STAGE, stage)
                .build();
    }

    public static Attributes buildAttributes(S3Stage stage) {
        return Attributes.builder()
            .putAll(buildOperationAttributes(stage.getOperation().getType().getName(), stage.getOperation().getName()))
            .put(S3StreamMetricsConstant.LABEL_STAGE, stage.getName())
            .build();
    }

    public static Attributes buildOperationAttributesWithStatusAndSize(String operationType, String operationName, String status, String sizeLabelName) {
        return Attributes.builder()
            .putAll(buildOperationAttributesWithStatus(operationType, operationName, status))
            .put(S3StreamMetricsConstant.LABEL_SIZE_NAME, sizeLabelName)
            .build();
    }

    public static Attributes buildAttributes(S3ObjectStage objectStage) {
        return Attributes.builder()
            .put(S3StreamMetricsConstant.LABEL_STAGE, objectStage.getName())
            .build();
    }

    public static String getObjectBucketLabel(long objectSize) {
        int index = (int) Math.ceil(Math.log((double) objectSize / (16 * 1024)) / Math.log(2));
        index = Math.min(S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES.length - 1, Math.max(0, index));
        return S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES[index];
    }
}
