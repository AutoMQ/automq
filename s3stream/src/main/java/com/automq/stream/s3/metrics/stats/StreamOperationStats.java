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

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;
import com.yammer.metrics.core.MetricName;

public class StreamOperationStats {
    private volatile static StreamOperationStats instance = null;
    public final YammerHistogramMetric createStreamStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StreamOperationStats.class, S3Operation.CREATE_STREAM.getUniqueKey()), MetricsLevel.INFO, S3Operation.CREATE_STREAM);
    public final YammerHistogramMetric openStreamStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StreamOperationStats.class, S3Operation.OPEN_STREAM.getUniqueKey()), MetricsLevel.INFO, S3Operation.OPEN_STREAM);
    public final YammerHistogramMetric appendStreamStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StreamOperationStats.class, S3Operation.APPEND_STREAM.getUniqueKey()), MetricsLevel.INFO, S3Operation.APPEND_STREAM);
    public final YammerHistogramMetric fetchStreamStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StreamOperationStats.class, S3Operation.FETCH_STREAM.getUniqueKey()), MetricsLevel.INFO, S3Operation.FETCH_STREAM);
    public final YammerHistogramMetric trimStreamStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StreamOperationStats.class, S3Operation.TRIM_STREAM.getUniqueKey()), MetricsLevel.INFO, S3Operation.TRIM_STREAM);
    private final YammerHistogramMetric closeStreamSuccessStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StreamOperationStats.class, S3Operation.CLOSE_STREAM.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_SUCCESS),
        MetricsLevel.INFO, S3Operation.CLOSE_STREAM, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final YammerHistogramMetric closeStreamFailStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StreamOperationStats.class, S3Operation.CLOSE_STREAM.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_FAILED),
        MetricsLevel.INFO, S3Operation.CLOSE_STREAM, S3StreamMetricsConstant.LABEL_STATUS_FAILED);

    private StreamOperationStats() {
    }

    public static StreamOperationStats getInstance() {
        if (instance == null) {
            synchronized (StreamOperationStats.class) {
                if (instance == null) {
                    instance = new StreamOperationStats();
                }
            }
        }
        return instance;
    }

    public YammerHistogramMetric closeStreamStats(boolean isSuccess) {
        return isSuccess ? closeStreamSuccessStats : closeStreamFailStats;
    }
}
