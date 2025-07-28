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
package com.automq.stream.s3.index.lazy;

import com.automq.stream.s3.index.NodeRangeIndexCache;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.utils.ThreadUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class StreamSetObjectRangeIndex {
    public static final boolean ENABLED = System.getenv().containsKey("AUTOMQ_STREAM_SET_RANGE_INDEX_ENABLED");
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSetObjectRangeIndex.class);
    private static volatile StreamSetObjectRangeIndex instance = null;

    public static final ExecutorService UPDATE_INDEX_THREAD_POOL = Executors.newSingleThreadExecutor(
        ThreadUtils.createThreadFactory("StreamSetObjectRangeIndex", true));

    private static final Object DUMMAY_OBJECT = new Object();
    private final ConcurrentHashMap<Long/*streamId*/, TreeMap<Long/*startOffset*/, Long/*objectId*/>> streamOffsetIndexMap =
        new ConcurrentHashMap<>(10240);
    private final Cache<Long /*streamId*/, Object> expireCache;

    private final Cache<Long/*objectId*/, Long> lastReaderUpdateTime = CacheBuilder.newBuilder()
        .maximumSize(20000)
        .expireAfterWrite(Duration.ofMinutes(1))
        .build();

    private final Striped<Lock> lock = Striped.lock(64);

    public StreamSetObjectRangeIndex(int maxSize, long expireTimeMs, Ticker ticker) {
        expireCache = CacheBuilder.newBuilder()
            .ticker(ticker)
            .maximumSize(maxSize)
            .expireAfterWrite(Duration.ofMillis(expireTimeMs))
            .removalListener(notification -> {
                if (notification.getKey() != null) {
                    streamOffsetIndexMap.remove(notification.getKey());
                }
            })
            .build();
    }

    public static StreamSetObjectRangeIndex getInstance() {
        if (instance == null) {
            synchronized (NodeRangeIndexCache.class) {
                if (instance == null) {
                    instance = new StreamSetObjectRangeIndex(20 * 10000,
                        TimeUnit.MINUTES.toMillis(10), Ticker.systemTicker());
                }
            }
        }
        return instance;
    }

    public void updateIndex(Long objectId, Long nodeId, Long streamId,
                                         List<StreamOffsetRange> streamOffsetRanges) {
        if (!ENABLED) {
            return;
        }

        if (lastReaderUpdateTime.getIfPresent(objectId) != null) {
            return;
        } else {
            lastReaderUpdateTime.put(objectId, System.currentTimeMillis());
        }

        try {
            touch(streamId);
            updateIndex(objectId, streamOffsetRanges);
        } catch (Exception e) {
            LOGGER.error("Failed to update index for reader: {}, nodeId: {}, streamId: {}",
                objectId, nodeId, streamId, e);
        }
    }

    public void touch(Long streamId) {
        try {
            expireCache.get(streamId, () -> DUMMAY_OBJECT);
        } catch (Exception ignored) {

        }
    }

    @VisibleForTesting
    public void clear() {
        streamOffsetIndexMap.clear();
    }

    public void updateIndex(Long objectId, List<StreamOffsetRange> streamOffsetRanges) {
        for (StreamOffsetRange streamOffsetRange : streamOffsetRanges) {
            Long streamId = streamOffsetRange.streamId();
            Long startOffset = streamOffsetRange.startOffset();

            withLock(streamId, () -> {
                TreeMap<Long, Long> offsetMap = streamOffsetIndexMap.computeIfAbsent(streamId, k -> new TreeMap<>());
                offsetMap.put(startOffset, objectId);
                touch(streamId);
            });
        }
    }

    private <T> T withLockReturn(Long streamId, Callable<T> callable) {
        Lock l = lock.get(streamId);
        try {
            l.lock();
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            l.unlock();
        }
    }

    private void withLock(Long streamId, Runnable runnable) {
        Lock l = lock.get(streamId);
        try {
            l.lock();
            runnable.run();
        } finally {
            l.unlock();
        }
    }

    public void invalid(int nodeId, long streamId, long startOffset, Long objectId) {
        TreeMap<Long, Long> longLongTreeMap = streamOffsetIndexMap.get(streamId);
        if (longLongTreeMap == null) {
            return;
        }

        withLock(streamId, () -> {
            longLongTreeMap.remove(startOffset, objectId);
        });
    }

    public CompletableFuture<Long> searchObjectId(int nodeId, long streamId, long startOffset) {
        TreeMap<Long, Long> offsetMap = streamOffsetIndexMap.get(streamId);
        if (offsetMap == null) {
            return CompletableFuture.completedFuture(-1L);
        }

        return withLockReturn(streamId, () -> {
            Map.Entry<Long, Long> entry = offsetMap.floorEntry(startOffset);
            if (entry == null) {
                return CompletableFuture.completedFuture(-1L);
            }

            touch(streamId);

            return CompletableFuture.completedFuture(entry.getValue());
        });
    }
}
