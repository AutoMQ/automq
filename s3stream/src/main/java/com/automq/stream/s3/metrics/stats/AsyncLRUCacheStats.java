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
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncLRUCacheStats {
    private static volatile AsyncLRUCacheStats instance = null;

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
            synchronized (AsyncLRUCacheStats.class) {
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
