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
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

public class MetadataStats {
    private static volatile MetadataStats instance = null;
    private final HistogramMetric getObjectsTimeSuccessStats = S3StreamMetricsManager.buildGetObjectsTimeMetric(MetricsLevel.INFO,
        S3StreamMetricsConstant.LABEL_STATUS_SUCCESS);
    private final HistogramMetric getObjectsTimeFailedStats = S3StreamMetricsManager.buildGetObjectsTimeMetric(MetricsLevel.INFO,
        S3StreamMetricsConstant.LABEL_STATUS_FAILED);
    private final CounterMetric rangeIndexUpdateCountStats = S3StreamMetricsManager.buildRangeIndexCacheOperationMetric(S3StreamMetricsConstant.LABEL_STATUS_UPDATE);
    private final CounterMetric rangeIndexInvalidateCountStats = S3StreamMetricsManager.buildRangeIndexCacheOperationMetric(S3StreamMetricsConstant.LABEL_STATUS_INVALIDATE);
    private final CounterMetric rangeIndexHitCountStats = S3StreamMetricsManager.buildRangeIndexCacheOperationMetric(S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final CounterMetric rangeIndexMissCountStats = S3StreamMetricsManager.buildRangeIndexCacheOperationMetric(S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final HistogramMetric rangeIndexSkippedObjectNumStats = S3StreamMetricsManager.buildObjectsToSearchMetric(MetricsLevel.INFO);

    private MetadataStats() {
    }

    public static MetadataStats getInstance() {
        if (instance == null) {
            synchronized (MetadataStats.class) {
                if (instance == null) {
                    instance = new MetadataStats();
                }
            }
        }
        return instance;
    }

    public HistogramMetric getObjectsTimeSuccessStats() {
        return getObjectsTimeSuccessStats;
    }

    public HistogramMetric getObjectsTimeFailedStats() {
        return getObjectsTimeFailedStats;
    }

    public CounterMetric getRangeIndexUpdateCountStats() {
        return rangeIndexUpdateCountStats;
    }

    public CounterMetric getRangeIndexInvalidateCountStats() {
        return rangeIndexInvalidateCountStats;
    }

    public CounterMetric getRangeIndexHitCountStats() {
        return rangeIndexHitCountStats;
    }

    public CounterMetric getRangeIndexMissCountStats() {
        return rangeIndexMissCountStats;
    }

    public HistogramMetric getRangeIndexSkippedObjectNumStats() {
        return rangeIndexSkippedObjectNumStats;
    }
}
