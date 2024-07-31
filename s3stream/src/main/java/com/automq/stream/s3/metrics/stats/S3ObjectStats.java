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
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

public class S3ObjectStats {
    private volatile static S3ObjectStats instance = null;

    public final CounterMetric objectNumInTotalStats = S3StreamMetricsManager.buildObjectNumMetric();
    public final HistogramMetric objectStageUploadPartStats = S3StreamMetricsManager
            .buildObjectStageCostMetric(MetricsLevel.DEBUG, S3ObjectStage.UPLOAD_PART);
    public final HistogramMetric objectStageReadyCloseStats = S3StreamMetricsManager
            .buildObjectStageCostMetric(MetricsLevel.DEBUG, S3ObjectStage.READY_CLOSE);
    public final HistogramMetric objectStageTotalStats = S3StreamMetricsManager
            .buildObjectStageCostMetric(MetricsLevel.DEBUG, S3ObjectStage.TOTAL);
    public final HistogramMetric objectUploadSizeStats = S3StreamMetricsManager.buildObjectUploadSizeMetric(MetricsLevel.DEBUG);

    private S3ObjectStats() {
    }

    public static S3ObjectStats getInstance() {
        if (instance == null) {
            synchronized (S3ObjectStats.class) {
                if (instance == null) {
                    instance = new S3ObjectStats();
                }
            }
        }
        return instance;
    }
}
