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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncLRUCache<K, V extends AsyncMeasurable> extends LRUCache<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncLRUCache.class);
    private final int maxSize;
    final AtomicInteger totalSize = new AtomicInteger(0);
    final Set<V> completedSet = new HashSet<>();
    final Set<V> removedSet = new HashSet<>();

    public AsyncLRUCache(int maxSize) {
        super();
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive");
        }
        this.maxSize = maxSize;
    }

    @Override
    public synchronized void put(K key, V value) {
        super.put(key, value);
        value.size().whenComplete((v, ex) -> {
            synchronized (AsyncLRUCache.this) {
                if (ex != null) {
                    remove(key);
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

    private synchronized void evict() {
        while (totalSize.get() > maxSize && cache.size() > 1) {
            Optional.ofNullable(pop()).ifPresent(entry -> {
                V value = entry.getValue();
                try {
                    boolean completed = completedSet.remove(value);
                    if (completed) {
                        totalSize.addAndGet(-value.size().get());
                        value.close();
                    } else {
                        removedSet.add(value);
                    }
                } catch (Throwable e) {
                    LOGGER.error("Failed to evict {}", value, e);
                }
            });
        }
    }

    public int totalSize() {
        return totalSize.get();
    }
}
