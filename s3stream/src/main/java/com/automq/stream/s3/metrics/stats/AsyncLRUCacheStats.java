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
import com.automq.stream.s3.metrics.wrapper.CounterMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncLRUCacheStats {
    private volatile static AsyncLRUCacheStats instance = null;

    private final Map<String, CounterMetric> evictCount = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> hitCount = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> missCount = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> putCount = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> popCount = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> overwriteCount = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> removeNotCompleted = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> removeCompleted = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> itemCompleteExceptionally = new ConcurrentHashMap<>();

    private AsyncLRUCacheStats() {
    }

    public static AsyncLRUCacheStats getInstance() {
        if (instance == null) {
            synchronized (NetworkStats.class) {
                if (instance == null) {
                    instance = new AsyncLRUCacheStats();
                }
            }
        }
        return instance;
    }


    public void markEvict(String cacheName) {
        evictCount.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCacheEvictMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

    public void markHit(String cacheName) {
        hitCount.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCacheHitMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

    public void markMiss(String cacheName) {
        missCount.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCacheMissMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

    public void markPut(String cacheName) {
        putCount.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCachePutMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

    public void markPop(String cacheName) {
        popCount.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCachePopMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

    public void markOverWrite(String cacheName) {
        overwriteCount.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCacheOverwriteMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

    public void markRemoveNotCompleted(String cacheName) {
        removeNotCompleted.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCacheRemoveNotCompleteMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

    public void markRemoveCompleted(String cacheName) {
        removeCompleted.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCacheRemoveCompleteMetric)
            .add(MetricsLevel.DEBUG, 1);
    }


    public void markItemCompleteExceptionally(String cacheName) {
        itemCompleteExceptionally.computeIfAbsent(cacheName, S3StreamMetricsManager::buildAsyncCacheItemCompleteExceptionallyMetric)
            .add(MetricsLevel.DEBUG, 1);
    }

}
