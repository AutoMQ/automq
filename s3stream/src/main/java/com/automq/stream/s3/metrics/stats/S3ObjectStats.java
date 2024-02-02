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

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

public class S3ObjectStats {
    private volatile static S3ObjectStats instance = null;

    public final CounterMetric objectNumInTotalStats = S3StreamMetricsManager.buildObjectNumMetric();
    public final HistogramMetric objectStageUploadPartStats = S3StreamMetricsManager.buildObjectStageCostMetric(S3ObjectStage.UPLOAD_PART);
    public final HistogramMetric objectStageReadyCloseStats = S3StreamMetricsManager.buildObjectStageCostMetric(S3ObjectStage.READY_CLOSE);
    public final HistogramMetric objectStageTotalStats = S3StreamMetricsManager.buildObjectStageCostMetric(S3ObjectStage.TOTAL);
    public final HistogramMetric objectUploadSizeStats = S3StreamMetricsManager.buildObjectUploadSizeMetric();
    public final HistogramMetric objectDownloadSizeStats = S3StreamMetricsManager.buildObjectDownloadSizeMetric();

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
