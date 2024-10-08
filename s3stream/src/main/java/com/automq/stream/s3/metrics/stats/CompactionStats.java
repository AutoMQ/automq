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

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;

public class CompactionStats {
    private static volatile CompactionStats instance = null;

    public final CounterMetric compactionReadSizeStats = S3StreamMetricsManager.buildCompactionReadSizeMetric();
    public final CounterMetric compactionWriteSizeStats = S3StreamMetricsManager.buildCompactionWriteSizeMetric();

    private CompactionStats() {
    }

    public static CompactionStats getInstance() {
        if (instance == null) {
            synchronized (CompactionStats.class) {
                if (instance == null) {
                    instance = new CompactionStats();
                }
            }
        }
        return instance;
    }

}
