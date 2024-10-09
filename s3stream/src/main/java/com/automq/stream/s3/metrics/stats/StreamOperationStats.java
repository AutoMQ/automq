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

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

public class StreamOperationStats {
    private static volatile StreamOperationStats instance = null;
    public final HistogramMetric createStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.CREATE_STREAM.getType().getName(), S3Operation.CREATE_STREAM.getName());
    public final HistogramMetric openStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.OPEN_STREAM.getType().getName(), S3Operation.OPEN_STREAM.getName());
    public final HistogramMetric appendStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STREAM.getType().getName(), S3Operation.APPEND_STREAM.getName());
    public final HistogramMetric fetchStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.FETCH_STREAM.getType().getName(), S3Operation.FETCH_STREAM.getName());
    public final HistogramMetric trimStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.TRIM_STREAM.getType().getName(), S3Operation.TRIM_STREAM.getName());
    private final HistogramMetric closeStreamSuccessLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.CLOSE_STREAM.getType().getName(), S3Operation.CLOSE_STREAM.getName(), S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric closeStreamFailLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.CLOSE_STREAM.getType().getName(), S3Operation.CLOSE_STREAM.getName(), S3StreamMetricsConstant.LABEL_STATUS_FAILED);

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

    public HistogramMetric closeStreamStats(boolean isSuccess) {
        return isSuccess ? closeStreamSuccessLatency : closeStreamFailLatency;
    }
}
