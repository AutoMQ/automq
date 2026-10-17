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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * An asynchronous LRU cache that supports asynchronous value computation.
 *
 * @param <K> key type
 * @param <V> value type, NOTE: V must not override equals and hashCode
 */
public class AsyncLRUCache<K, V extends AsyncMeasurable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncLRUCache.class);
    private static final AttributeKey<String> LABEL_CACHE_NAME = AttributeKey.stringKey("cacheName");
    private static final Metrics.LongGaugeBundle ASYNC_CACHE_ITEM_NUMBER = Metrics.instance()
        .longGauge("kafka_stream_async_cache_item_count", "AsyncLRU cache item number", "");
    private static final Metrics.LongGaugeBundle ASYNC_CACHE_SIZE = Metrics.instance()
        .longGauge("kafka_stream_async_cache_item_size", "AsyncLRU cache size", "");
    private static final Metrics.LongGaugeBundle ASYNC_CACHE_MAX_SIZE = Metrics.instance()
        .longGauge("kafka_stream_async_cache_max_size", "AsyncLRU cache max size", "");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_EVICT_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_evict", "AsyncLRU cache evict count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_HIT_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_hit", "AsyncLRU cache hit count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_MISS_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_miss", "AsyncLRU cache miss count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_PUT_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_put", "AsyncLRU cache put item count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_POP_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_pop", "AsyncLRU cache pop item count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_OVERWRITE_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_overwrite", "AsyncLRU cache overwrite item count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_REMOVE_NOT_COMPLETE_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_remove_item_not_complete",
            "AsyncLRU cache remove not completed item count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_REMOVE_COMPLETE_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_remove_item_complete",
            "AsyncLRU cache remove completed item count", "count");
    private static final Metrics.LongCounterBundle ASYNC_CACHE_ITEM_COMPLETE_EXCEPTIONALLY_COUNT = Metrics.instance()
        .longCounter("kafka_stream_async_cache_item_complete_exceptionally",
            "AsyncLRU cache item complete exceptionally count", "count");
    private final long maxSize;
    private final Metrics.LongCounterBundle.LongCounter evictCount;
    private final Metrics.LongCounterBundle.LongCounter hitCount;
    private final Metrics.LongCounterBundle.LongCounter missCount;
    private final Metrics.LongCounterBundle.LongCounter putCount;
    private final Metrics.LongCounterBundle.LongCounter popCount;
    private final Metrics.LongCounterBundle.LongCounter overwriteCount;
    private final Metrics.LongCounterBundle.LongCounter removeNotCompleted;
    private final Metrics.LongCounterBundle.LongCounter removeCompleted;
    private final Metrics.LongCounterBundle.LongCounter itemCompleteExceptionally;
    final AtomicLong totalSize = new AtomicLong(0);
    final LRUCache<K, V> cache = new LRUCache<>();
    final Set<V> completedSet = new HashSet<>();
    final Set<V> removedSet = new HashSet<>();

    public AsyncLRUCache(String cacheName, long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive");
        }
        this.maxSize = maxSize;

        Attributes attributes = Attributes.of(LABEL_CACHE_NAME, cacheName);
        ASYNC_CACHE_SIZE.register(MetricsLevel.DEBUG, attributes).record(this::totalSize);
        ASYNC_CACHE_MAX_SIZE.register(MetricsLevel.DEBUG, attributes).record(() -> maxSize);
        ASYNC_CACHE_ITEM_NUMBER.register(MetricsLevel.DEBUG, attributes).record(this::size);
        evictCount = ASYNC_CACHE_EVICT_COUNT.register(MetricsLevel.DEBUG, attributes);
        hitCount = ASYNC_CACHE_HIT_COUNT.register(MetricsLevel.DEBUG, attributes);
        missCount = ASYNC_CACHE_MISS_COUNT.register(MetricsLevel.DEBUG, attributes);
        putCount = ASYNC_CACHE_PUT_COUNT.register(MetricsLevel.DEBUG, attributes);
        popCount = ASYNC_CACHE_POP_COUNT.register(MetricsLevel.DEBUG, attributes);
        overwriteCount = ASYNC_CACHE_OVERWRITE_COUNT.register(MetricsLevel.DEBUG, attributes);
        removeNotCompleted = ASYNC_CACHE_REMOVE_NOT_COMPLETE_COUNT.register(MetricsLevel.DEBUG, attributes);
        removeCompleted = ASYNC_CACHE_REMOVE_COMPLETE_COUNT.register(MetricsLevel.DEBUG, attributes);
        itemCompleteExceptionally = ASYNC_CACHE_ITEM_COMPLETE_EXCEPTIONALLY_COUNT.register(MetricsLevel.DEBUG, attributes);
    }

    public static <K, V extends AsyncMeasurable> AsyncLRUCache<K, V> create(String cacheName, long maxSize) {
        return new AsyncLRUCache<>(cacheName, maxSize);
    }

    public synchronized void put(K key, V value) {
        V oldValue = cache.get(key);
        if (oldValue != null && oldValue != value) {
            overwriteCount.add(1);
            cache.remove(key);
            afterRemoveValue(oldValue);
        } else {
            putCount.add(1);
        }

        cache.put(key, value);
        value.size().whenComplete((v, ex) -> {
            synchronized (AsyncLRUCache.this) {
                if (ex != null) {
                    itemCompleteExceptionally.add(1);
                    cache.remove(key);
                } else if (!removedSet.contains(value)) {
                    completedSet.add(value);
                    if (totalSize.addAndGet(v) > maxSize) {
                        evict();
                    }
                } else {
                    try {
                        value.close();
                    } catch (Exception e) {
                        LOGGER.error("Failed to close {}", value, e);
                    }
                }
                removedSet.remove(value);
            }
        });
    }

    public synchronized V get(K key) {
        V val = cache.get(key);
        if (val == null) {
            missCount.add(1);
        } else {
            hitCount.add(1);
        }
        return val;
    }

    public synchronized V computeIfAbsent(K key, Function<? super K, ? extends V> valueMapper) {
        V value = cache.get(key);
        if (value == null) {
            value = valueMapper.apply(key);
            if (value != null) {
                put(key, value);
            }
        }
        return value;
    }

    public synchronized void inLockRun(Runnable runnable) {
        runnable.run();
    }

    public synchronized boolean remove(K key) {
        V value = cache.get(key);
        if (value == null) {
            return false;
        }
        cache.remove(key);
        afterRemoveValue(value);
        return true;
    }

    private synchronized void afterRemoveValue(V value) {
        try {
            boolean completed = completedSet.remove(value);
            if (completed) {
                removeCompleted.add(1);
                totalSize.addAndGet(-value.size().get());
                value.close();
            } else {
                removeNotCompleted.add(1);
                removedSet.add(value);
            }
        } catch (Throwable e) {
            LOGGER.error("Failed to remove {}", value, e);
        }
    }

    public synchronized Map.Entry<K, V> pop() {
        Map.Entry<K, V> entry = cache.pop();
        if (entry != null) {
            afterRemoveValue(entry.getValue());
        }

        popCount.add(1);

        return entry;
    }

    public synchronized int size() {
        return cache.size();
    }

    public synchronized boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    public synchronized void clear() {
        while (cache.size() > 0) {
            pop();
        }
    }

    public long totalSize() {
        return totalSize.get();
    }

    private synchronized void evict() {
        while (totalSize.get() > maxSize && cache.size() > 1) {
            Optional.ofNullable(cache.pop()).ifPresent(entry -> {
                V value = entry.getValue();
                afterRemoveValue(value);
            });
        }

        evictCount.add(1);
    }
}
