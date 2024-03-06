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
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;
import com.yammer.metrics.core.MetricName;

public class S3ObjectStats {
    private volatile static S3ObjectStats instance = null;

    public final CounterMetric objectNumInTotalStats = S3StreamMetricsManager.buildObjectNumMetric();
    public final YammerHistogramMetric objectStageUploadPartStats = S3StreamMetricsManager.buildObjectStageCostMetric(
        new MetricName(S3ObjectStats.class, S3ObjectStage.UPLOAD_PART.getUniqueKey()), MetricsLevel.DEBUG, S3ObjectStage.UPLOAD_PART);
    public final YammerHistogramMetric objectStageReadyCloseStats = S3StreamMetricsManager.buildObjectStageCostMetric(
        new MetricName(S3ObjectStats.class, S3ObjectStage.READY_CLOSE.getUniqueKey()), MetricsLevel.DEBUG, S3ObjectStage.READY_CLOSE);
    public final YammerHistogramMetric objectStageTotalStats = S3StreamMetricsManager.buildObjectStageCostMetric(
        new MetricName(S3ObjectStats.class, S3ObjectStage.TOTAL.getUniqueKey()), MetricsLevel.DEBUG, S3ObjectStage.TOTAL);
    public final YammerHistogramMetric objectUploadSizeStats = S3StreamMetricsManager.buildObjectUploadSizeMetric(
        new MetricName(S3ObjectStats.class, "ObjectUploadSize"), MetricsLevel.DEBUG);

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
