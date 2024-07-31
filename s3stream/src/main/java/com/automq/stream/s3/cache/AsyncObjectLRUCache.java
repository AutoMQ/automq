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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncObjectLRUCache<K, V extends AsyncMeasurable> extends LRUCache<K, V> {
    private final int maxSize;

    public AsyncObjectLRUCache(int maxSize) {
        super();
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive");
        }
        this.maxSize = maxSize;
    }

    @Override
    public synchronized void put(K key, V value) {
        evict();
        super.put(key, value);
        value.size().thenAccept(v -> evict());
    }

    private synchronized void evict() {
        AtomicInteger totalSize = new AtomicInteger(totalSize());
        while (totalSize.get() > maxSize) {
            Optional.ofNullable(pop()).ifPresent(entry -> {
                try {
                    CompletableFuture<Integer> cf = entry.getValue().size();
                    if (cf.isDone()) {
                        totalSize.addAndGet(-cf.get());
                    }
                    entry.getValue().close();
                } catch (Throwable t) {
                    // ignore
                }
            });
        }
    }

    public synchronized int totalSize() {
        return cacheEntrySet.stream().mapToInt(entry -> {
            if (!entry.getValue().size().isDone()) {
                return 0;
            }
            try {
                return entry.getValue().size().get();
            } catch (Exception e) {
                return 0;
            }
        }).sum();
    }
}
