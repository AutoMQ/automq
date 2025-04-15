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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.AsyncLRUCacheStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * An asynchronous LRU cache that supports asynchronous value computation.
 *
 * @param <K> key type
 * @param <V> value type, NOTE: V must not override equals and hashCode
 */
public class AsyncLRUCache<K, V extends AsyncMeasurable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncLRUCache.class);
    private final AsyncLRUCacheStats stats = AsyncLRUCacheStats.getInstance();
    private final String cacheName;
    private final long maxSize;
    final AtomicLong totalSize = new AtomicLong(0);
    final LRUCache<K, V> cache = new LRUCache<>();
    final Set<V> completedSet = new HashSet<>();
    final Set<V> removedSet = new HashSet<>();

    public AsyncLRUCache(String cacheName, long maxSize) {
        this.cacheName = cacheName;
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive");
        }
        this.maxSize = maxSize;
    }

    public void init() {
        S3StreamMetricsManager.registerAsyncCacheSizeSupplier(this::totalSize, cacheName);
        S3StreamMetricsManager.registerAsyncCacheMaxSizeSupplier(() -> maxSize, cacheName);
        S3StreamMetricsManager.registerAsyncCacheItemNumberSupplier(this::size, cacheName);
    }

    public synchronized void put(K key, V value) {
        V oldValue = cache.get(key);
        if (oldValue != null && oldValue != value) {
            stats.markOverWrite(cacheName);
            cache.remove(key);
            afterRemoveValue(oldValue);
        } else {
            stats.markPut(cacheName);
        }

        cache.put(key, value);
        value.size().whenComplete((v, ex) -> {
            synchronized (AsyncLRUCache.this) {
                if (ex != null) {
                    stats.markItemCompleteExceptionally(cacheName);
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
            stats.markMiss(cacheName);
        } else {
            stats.markHit(cacheName);
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
                stats.markRemoveCompleted(cacheName);
                totalSize.addAndGet(-value.size().get());
                value.close();
            } else {
                stats.markRemoveNotCompleted(cacheName);
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

        stats.markPop(cacheName);

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

        stats.markEvict(cacheName);
    }
}
