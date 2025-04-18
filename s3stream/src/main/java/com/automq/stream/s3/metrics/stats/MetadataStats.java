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
