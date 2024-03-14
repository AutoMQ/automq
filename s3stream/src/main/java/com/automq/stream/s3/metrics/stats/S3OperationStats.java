/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
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
import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;
import com.yammer.metrics.core.MetricName;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class S3OperationStats {
    private volatile static S3OperationStats instance = null;
    public final CounterMetric uploadSizeTotalStats = S3StreamMetricsManager.buildS3UploadSizeMetric();
    public final CounterMetric downloadSizeTotalStats = S3StreamMetricsManager.buildS3DownloadSizeMetric();
    private final Map<String, YammerHistogramMetric> getObjectSuccessStats = new ConcurrentHashMap<>();
    private final Map<String, YammerHistogramMetric> getObjectFailedStats = new ConcurrentHashMap<>();
    private final Map<String, YammerHistogramMetric> putObjectSuccessStats = new ConcurrentHashMap<>();
    private final Map<String, YammerHistogramMetric> putObjectFailedStats = new ConcurrentHashMap<>();
    private final YammerHistogramMetric deleteObjectSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.DELETE_OBJECT.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS),
        MetricsLevel.INFO, S3Operation.DELETE_OBJECT, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final YammerHistogramMetric deleteObjectFailedStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.DELETE_OBJECT.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_FAILED),
        MetricsLevel.INFO, S3Operation.DELETE_OBJECT, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final YammerHistogramMetric deleteObjectsSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.DELETE_OBJECTS.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS),
        MetricsLevel.INFO, S3Operation.DELETE_OBJECTS, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final YammerHistogramMetric deleteObjectsFailedStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.DELETE_OBJECTS.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_FAILED),
        MetricsLevel.INFO, S3Operation.DELETE_OBJECTS, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final YammerHistogramMetric createMultiPartUploadSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.CREATE_MULTI_PART_UPLOAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS),
        MetricsLevel.INFO, S3Operation.CREATE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final YammerHistogramMetric createMultiPartUploadFailedStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.CREATE_MULTI_PART_UPLOAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_FAILED),
        MetricsLevel.INFO, S3Operation.CREATE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final Map<String, YammerHistogramMetric> uploadPartSuccessStats = new ConcurrentHashMap<>();
    private final Map<String, YammerHistogramMetric> uploadPartFailedStats = new ConcurrentHashMap<>();
    private final YammerHistogramMetric uploadPartCopySuccessStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.UPLOAD_PART_COPY.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS),
        MetricsLevel.INFO, S3Operation.UPLOAD_PART_COPY, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final YammerHistogramMetric uploadPartCopyFailedStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.UPLOAD_PART_COPY.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_FAILED),
        MetricsLevel.INFO, S3Operation.UPLOAD_PART_COPY, S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final YammerHistogramMetric completeMultiPartUploadSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.COMPLETE_MULTI_PART_UPLOAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS),
        MetricsLevel.INFO, S3Operation.COMPLETE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final YammerHistogramMetric completeMultiPartUploadFailedStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(S3OperationStats.class, S3Operation.COMPLETE_MULTI_PART_UPLOAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_FAILED),
        MetricsLevel.INFO, S3Operation.COMPLETE_MULTI_PART_UPLOAD, S3StreamMetricsConstant.LABEL_STATUS_FAILED);

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

    public YammerHistogramMetric getObjectStats(long size, boolean isSuccess) {
        String label = AttributesUtils.getObjectBucketLabel(size);
        if (isSuccess) {
            return getObjectSuccessStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                new MetricName(S3OperationStats.class, S3Operation.GET_OBJECT.getUniqueKey()
                    + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS + label),
                MetricsLevel.INFO, S3Operation.GET_OBJECT, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS, label));
        } else {
            return getObjectFailedStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                new MetricName(S3OperationStats.class, S3Operation.GET_OBJECT.getUniqueKey()
                    + S3StreamMetricsConstant.LABEL_STATUS_FAILED + label),
                MetricsLevel.INFO, S3Operation.GET_OBJECT, S3StreamMetricsConstant.LABEL_STATUS_FAILED, label));
        }
    }

    public YammerHistogramMetric putObjectStats(long size, boolean isSuccess) {
        String label = AttributesUtils.getObjectBucketLabel(size);
        if (isSuccess) {
            return putObjectSuccessStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                new MetricName(S3OperationStats.class, S3Operation.PUT_OBJECT.getUniqueKey()
                    + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS + label),
                MetricsLevel.INFO, S3Operation.PUT_OBJECT, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS, label));
        } else {
            return putObjectFailedStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                new MetricName(S3OperationStats.class, S3Operation.PUT_OBJECT.getUniqueKey() +
                    S3StreamMetricsConstant.LABEL_STATUS_FAILED + label),
                MetricsLevel.INFO, S3Operation.PUT_OBJECT, S3StreamMetricsConstant.LABEL_STATUS_FAILED, label));
        }
    }

    public YammerHistogramMetric uploadPartStats(long size, boolean isSuccess) {
        String label = AttributesUtils.getObjectBucketLabel(size);
        if (isSuccess) {
            return uploadPartSuccessStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                new MetricName(S3OperationStats.class, S3Operation.UPLOAD_PART.getUniqueKey() +
                    S3StreamMetricsConstant.LABEL_STATUS_SUCCESS + label),
                MetricsLevel.INFO, S3Operation.UPLOAD_PART, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS, label));
        } else {
            return uploadPartFailedStats.computeIfAbsent(label, name -> S3StreamMetricsManager.buildOperationMetric(
                new MetricName(S3OperationStats.class, S3Operation.UPLOAD_PART.getUniqueKey() +
                    S3StreamMetricsConstant.LABEL_STATUS_FAILED + label),
                MetricsLevel.INFO, S3Operation.UPLOAD_PART, S3StreamMetricsConstant.LABEL_STATUS_FAILED, label));
        }
    }

    public YammerHistogramMetric deleteObjectStats(boolean isSuccess) {
        return isSuccess ? deleteObjectSuccessStats : deleteObjectFailedStats;
    }

    public YammerHistogramMetric deleteObjectsStats(boolean isSuccess) {
        return isSuccess ? deleteObjectsSuccessStats : deleteObjectsFailedStats;
    }

    public YammerHistogramMetric uploadPartCopyStats(boolean isSuccess) {
        return isSuccess ? uploadPartCopySuccessStats : uploadPartCopyFailedStats;
    }

    public YammerHistogramMetric createMultiPartUploadStats(boolean isSuccess) {
        return isSuccess ? createMultiPartUploadSuccessStats : createMultiPartUploadFailedStats;
    }

    public YammerHistogramMetric completeMultiPartUploadStats(boolean isSuccess) {
        return isSuccess ? completeMultiPartUploadSuccessStats : completeMultiPartUploadFailedStats;
    }
}
