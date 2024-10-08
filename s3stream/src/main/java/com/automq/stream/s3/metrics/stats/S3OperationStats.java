/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.AttributesUtils;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class S3OperationStats {
    private static volatile S3OperationStats instance = null;
    public final CounterMetric uploadSizeTotalStats = S3StreamMetricsManager.buildS3UploadSizeMetric();
    public final CounterMetric downloadSizeTotalStats = S3StreamMetricsManager.buildS3DownloadSizeMetric();
    private final Map<String, HistogramMetric> getObjectSuccessStats = new ConcurrentHashMap<>();
    private final Map<String, HistogramMetric> getObjectFailedStats = new ConcurrentHashMap<>();
    private final Map<String, HistogramMetric> putObjectSuccessStats = new ConcurrentHashMap<>();
    private final Map<String, HistogramMetric> putObjectFailedStats = new ConcurrentHashMap<>();
    private final HistogramMetric listObjectsStatsSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.LIST_OBJECTS.getType().getName(), S3Operation.LIST_OBJECTS.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric listObjectsStatsFailedStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.LIST_OBJECTS.getType().getName(), S3Operation.LIST_OBJECTS.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final HistogramMetric deleteObjectSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.DELETE_OBJECT.getType().getName(), S3Operation.DELETE_OBJECT.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric deleteObjectFailedStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.DELETE_OBJECT.getType().getName(), S3Operation.DELETE_OBJECT.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final HistogramMetric deleteObjectsSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.DELETE_OBJECTS.getType().getName(), S3Operation.DELETE_OBJECTS.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric deleteObjectsFailedStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.DELETE_OBJECTS.getType().getName(), S3Operation.DELETE_OBJECTS.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final HistogramMetric createMultiPartUploadSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.CREATE_MULTI_PART_UPLOAD.getType().getName(),
        S3Operation.CREATE_MULTI_PART_UPLOAD.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric createMultiPartUploadFailedStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.CREATE_MULTI_PART_UPLOAD.getType().getName(),
        S3Operation.CREATE_MULTI_PART_UPLOAD.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final Map<String, HistogramMetric> uploadPartSuccessStats = new ConcurrentHashMap<>();
    private final Map<String, HistogramMetric> uploadPartFailedStats = new ConcurrentHashMap<>();
    private final HistogramMetric uploadPartCopySuccessStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.UPLOAD_PART_COPY.getType().getName(), S3Operation.UPLOAD_PART_COPY.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric uploadPartCopyFailedStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.UPLOAD_PART_COPY.getType().getName(), S3Operation.UPLOAD_PART_COPY.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final HistogramMetric completeMultiPartUploadSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.COMPLETE_MULTI_PART_UPLOAD.getType().getName(),
        S3Operation.COMPLETE_MULTI_PART_UPLOAD.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric completeMultiPartUploadFailedStats = S3StreamMetricsManager.buildOperationMetric(
        MetricsLevel.INFO, S3Operation.COMPLETE_MULTI_PART_UPLOAD.getType().getName(),
        S3Operation.COMPLETE_MULTI_PART_UPLOAD.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED);

    private S3OperationStats() {
    }

    public static S3OperationStats getInstance() {
        if (instance == null) {
            synchronized (StreamOperationStats.class) {
                if (instance == null) {
                    instance = new S3OperationStats();
                }
            }
        }
        return instance;
    }

    public HistogramMetric getObjectStats(long size, boolean isSuccess) {
        String label = AttributesUtils.getObjectBucketLabel(size);
        if (isSuccess) {
            return getObjectSuccessStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                MetricsLevel.INFO, S3Operation.GET_OBJECT.getType().getName(), S3Operation.GET_OBJECT.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS, label));
        } else {
            return getObjectFailedStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                MetricsLevel.INFO, S3Operation.GET_OBJECT.getType().getName(), S3Operation.GET_OBJECT.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED, label));
        }
    }

    public HistogramMetric putObjectStats(long size, boolean isSuccess) {
        String label = AttributesUtils.getObjectBucketLabel(size);
        if (isSuccess) {
            return putObjectSuccessStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                MetricsLevel.INFO, S3Operation.PUT_OBJECT.getType().getName(), S3Operation.PUT_OBJECT.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS, label));
        } else {
            return putObjectFailedStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                MetricsLevel.INFO, S3Operation.PUT_OBJECT.getType().getName(), S3Operation.PUT_OBJECT.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED, label));
        }
    }

    public HistogramMetric uploadPartStats(long size, boolean isSuccess) {
        String label = AttributesUtils.getObjectBucketLabel(size);
        if (isSuccess) {
            return uploadPartSuccessStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                MetricsLevel.INFO, S3Operation.UPLOAD_PART.getType().getName(), S3Operation.UPLOAD_PART.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS, label));
        } else {
            return uploadPartFailedStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                MetricsLevel.INFO, S3Operation.UPLOAD_PART.getType().getName(), S3Operation.UPLOAD_PART.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED, label));
        }
    }

    public HistogramMetric listObjectsStats(boolean isSuccess) {
        return isSuccess ? listObjectsStatsSuccessStats : listObjectsStatsFailedStats;
    }

    public HistogramMetric deleteObjectStats(boolean isSuccess) {
        return isSuccess ? deleteObjectSuccessStats : deleteObjectFailedStats;
    }

    public HistogramMetric deleteObjectsStats(boolean isSuccess) {
        return isSuccess ? deleteObjectsSuccessStats : deleteObjectsFailedStats;
    }

    public HistogramMetric uploadPartCopyStats(boolean isSuccess) {
        return isSuccess ? uploadPartCopySuccessStats : uploadPartCopyFailedStats;
    }

    public HistogramMetric createMultiPartUploadStats(boolean isSuccess) {
        return isSuccess ? createMultiPartUploadSuccessStats : createMultiPartUploadFailedStats;
    }

    public HistogramMetric completeMultiPartUploadStats(boolean isSuccess) {
        return isSuccess ? completeMultiPartUploadSuccessStats : completeMultiPartUploadFailedStats;
    }
}
