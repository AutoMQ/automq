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
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ByteBufStats {
    private volatile static ByteBufStats instance = null;

    private final Map<String, HistogramMetric> allocateByteBufSizeStats = new ConcurrentHashMap<>();

    private ByteBufStats() {
    }

    public static ByteBufStats getInstance() {
        if (instance == null) {
            synchronized (ByteBufStats.class) {
                if (instance == null) {
                    instance = new ByteBufStats();
                }
            }
        }
        return instance;
    }

    public HistogramMetric allocateByteBufSizeStats(String source) {
        return allocateByteBufSizeStats.computeIfAbsent(source, S3StreamMetricsManager::buildAllocateByteBufSizeMetric);
    }
}
