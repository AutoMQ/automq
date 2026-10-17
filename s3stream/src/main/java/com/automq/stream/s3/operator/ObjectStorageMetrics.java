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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.AttributesUtils;
import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.OperationLatencyMetrics;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

final class ObjectStorageMetrics {
    private static final AttributeKey<String> LABEL_INDEX = AttributeKey.stringKey("index");
    private static final Metrics.LongGaugeBundle AVAILABLE_S3_INFLIGHT_READ_QUOTA = Metrics.instance()
        .longGauge("kafka_stream_available_s3_inflight_read_quota", "Available inflight S3 read quota", "");
    private static final Metrics.LongGaugeBundle AVAILABLE_S3_INFLIGHT_WRITE_QUOTA = Metrics.instance()
        .longGauge("kafka_stream_available_s3_inflight_write_quota", "Available inflight S3 write quota", "");
    private static final Metrics.LongCounterBundle.LongCounter UPLOAD_SIZE_TOTAL = Metrics.instance()
        .longCounter("kafka_stream_upload_size", "S3 upload size", "bytes")
        .register(MetricsLevel.INFO, Attributes.empty());
    private static final Metrics.LongCounterBundle.LongCounter DOWNLOAD_SIZE_TOTAL = Metrics.instance()
        .longCounter("kafka_stream_download_size", "S3 download size", "bytes")
        .register(MetricsLevel.INFO, Attributes.empty());
    private static final Metrics.HistogramBundle READ_S3_LIMITER_TIME = Metrics.instance()
        .histogram("kafka_stream_read_s3_limiter_time", "Time blocked on waiting for inflight read quota", "nanoseconds");
    private static final Metrics.HistogramBundle WRITE_S3_LIMITER_TIME = Metrics.instance()
        .histogram("kafka_stream_write_s3_limiter_time", "Time blocked on waiting for inflight write quota", "nanoseconds");
    private static final Map<String, DeltaHistogram> GET_OBJECT_SUCCESS_STATS = new ConcurrentHashMap<>();
    private static final Map<String, DeltaHistogram> GET_OBJECT_FAILED_STATS = new ConcurrentHashMap<>();
    private static final Map<String, DeltaHistogram> PUT_OBJECT_SUCCESS_STATS = new ConcurrentHashMap<>();
    private static final Map<String, DeltaHistogram> PUT_OBJECT_FAILED_STATS = new ConcurrentHashMap<>();
    private static final DeltaHistogram LIST_OBJECTS_SUCCESS_STATS = operationStats(
        S3Operation.LIST_OBJECTS, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private static final DeltaHistogram LIST_OBJECTS_FAILED_STATS = operationStats(
        S3Operation.LIST_OBJECTS, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private static final DeltaHistogram CREATE_MULTI_PART_UPLOAD_SUCCESS_STATS = operationStats(
        S3Operation.CREATE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private static final DeltaHistogram CREATE_MULTI_PART_UPLOAD_FAILED_STATS = operationStats(
        S3Operation.CREATE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private static final Map<String, DeltaHistogram> UPLOAD_PART_SUCCESS_STATS = new ConcurrentHashMap<>();
    private static final Map<String, DeltaHistogram> UPLOAD_PART_FAILED_STATS = new ConcurrentHashMap<>();
    private static final DeltaHistogram UPLOAD_PART_COPY_SUCCESS_STATS = operationStats(
        S3Operation.UPLOAD_PART_COPY, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private static final DeltaHistogram UPLOAD_PART_COPY_FAILED_STATS = operationStats(
        S3Operation.UPLOAD_PART_COPY, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private static final DeltaHistogram COMPLETE_MULTI_PART_UPLOAD_SUCCESS_STATS = operationStats(
        S3Operation.COMPLETE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private static final DeltaHistogram COMPLETE_MULTI_PART_UPLOAD_FAILED_STATS = operationStats(
        S3Operation.COMPLETE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private static final Map<Integer, DeltaHistogram> READ_S3_LIMITER_STATS = new ConcurrentHashMap<>();
    private static final Map<Integer, DeltaHistogram> WRITE_S3_LIMITER_STATS = new ConcurrentHashMap<>();

    private ObjectStorageMetrics() {
    }

    static void registerInflightReadQuota(int index, LongSupplier supplier) {
        AVAILABLE_S3_INFLIGHT_READ_QUOTA.register(MetricsLevel.DEBUG, attributes(index)).record(supplier);
    }

    static void registerInflightWriteQuota(int index, LongSupplier supplier) {
        AVAILABLE_S3_INFLIGHT_WRITE_QUOTA.register(MetricsLevel.DEBUG, attributes(index)).record(supplier);
    }

    static void recordUploadSize(long size) {
        UPLOAD_SIZE_TOTAL.add(size);
    }

    static void recordDownloadSize(long size) {
        DOWNLOAD_SIZE_TOTAL.add(size);
    }

    static void recordGetObject(long size, boolean success, long timeNanos) {
        objectSizeStats(GET_OBJECT_SUCCESS_STATS, GET_OBJECT_FAILED_STATS, S3Operation.GET_OBJECT, size, success)
            .record(timeNanos);
    }

    static void recordPutObject(long size, boolean success, long timeNanos) {
        objectSizeStats(PUT_OBJECT_SUCCESS_STATS, PUT_OBJECT_FAILED_STATS, S3Operation.PUT_OBJECT, size, success)
            .record(timeNanos);
    }

    static void recordUploadPart(long size, boolean success, long timeNanos) {
        objectSizeStats(UPLOAD_PART_SUCCESS_STATS, UPLOAD_PART_FAILED_STATS, S3Operation.UPLOAD_PART, size, success)
            .record(timeNanos);
    }

    static void recordListObjects(boolean success, long timeNanos) {
        (success ? LIST_OBJECTS_SUCCESS_STATS : LIST_OBJECTS_FAILED_STATS).record(timeNanos);
    }

    static void recordCreateMultipartUpload(boolean success, long timeNanos) {
        (success ? CREATE_MULTI_PART_UPLOAD_SUCCESS_STATS : CREATE_MULTI_PART_UPLOAD_FAILED_STATS).record(timeNanos);
    }

    static void recordUploadPartCopy(boolean success, long timeNanos) {
        (success ? UPLOAD_PART_COPY_SUCCESS_STATS : UPLOAD_PART_COPY_FAILED_STATS).record(timeNanos);
    }

    static void recordCompleteMultipartUpload(boolean success, long timeNanos) {
        (success ? COMPLETE_MULTI_PART_UPLOAD_SUCCESS_STATS : COMPLETE_MULTI_PART_UPLOAD_FAILED_STATS).record(timeNanos);
    }

    static void recordReadS3Limiter(int index, long timeNanos) {
        READ_S3_LIMITER_STATS.computeIfAbsent(index,
            k -> READ_S3_LIMITER_TIME.histogram(MetricsLevel.DEBUG, attributes(index))).record(timeNanos);
    }

    static void recordWriteS3Limiter(int index, long timeNanos) {
        WRITE_S3_LIMITER_STATS.computeIfAbsent(index,
            k -> WRITE_S3_LIMITER_TIME.histogram(MetricsLevel.DEBUG, attributes(index))).record(timeNanos);
    }

    private static DeltaHistogram objectSizeStats(Map<String, DeltaHistogram> successStats,
        Map<String, DeltaHistogram> failedStats, S3Operation operation, long size, boolean success) {
        String label = AttributesUtils.getObjectBucketLabel(size);
        if (success) {
            return successStats.computeIfAbsent(label, name -> operationStats(
                operation, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS, label));
        } else {
            return failedStats.computeIfAbsent(label, name -> operationStats(
                operation, S3StreamMetricsConstant.LABEL_STATUS_FAILED, label));
        }
    }

    private static DeltaHistogram operationStats(S3Operation operation, String status) {
        return OperationLatencyMetrics.operation(MetricsLevel.INFO, operation, status);
    }

    private static DeltaHistogram operationStats(S3Operation operation, String status, String sizeLabelName) {
        return OperationLatencyMetrics.operation(MetricsLevel.INFO, operation.getType().getName(),
            operation.getName(), status, sizeLabelName);
    }

    private static Attributes attributes(int index) {
        return Attributes.of(LABEL_INDEX, String.valueOf(index));
    }
}
