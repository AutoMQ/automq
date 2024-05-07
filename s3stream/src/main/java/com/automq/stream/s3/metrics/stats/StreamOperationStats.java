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
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

public class StreamOperationStats {
    private volatile static StreamOperationStats instance = null;
    public final HistogramMetric createStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.CREATE_STREAM);
    public final HistogramMetric openStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.OPEN_STREAM);
    public final HistogramMetric appendStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STREAM);
    public final HistogramMetric fetchStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.FETCH_STREAM);
    public final HistogramMetric trimStreamLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.TRIM_STREAM);
    private final HistogramMetric closeStreamSuccessLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.CLOSE_STREAM, S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric closeStreamFailLatency = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.CLOSE_STREAM, S3StreamMetricsConstant.LABEL_STATUS_FAILED);

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
