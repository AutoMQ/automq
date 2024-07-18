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

import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.network.ThrottleStrategy;
import io.opentelemetry.api.common.Attributes;

public class AttributesUtils {

    public static Attributes buildAttributes(S3Operation operation) {
        return Attributes.builder()
            .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
            .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
            .build();
    }

    public static Attributes buildAttributes(S3Operation operation, String status) {
        return Attributes.builder()
            .putAll(buildAttributes(operation))
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
            .putAll(buildAttributes(stage.getOperation()))
            .put(S3StreamMetricsConstant.LABEL_STAGE, stage.getName())
            .build();
    }

    public static Attributes buildAttributes(S3Operation operation, String status, String sizeLabelName) {
        return Attributes.builder()
            .putAll(buildAttributes(operation, status))
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
