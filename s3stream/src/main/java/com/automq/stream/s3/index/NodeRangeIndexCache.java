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

package com.automq.stream.s3.index;

import com.automq.stream.s3.cache.AsyncLRUCache;
import com.automq.stream.s3.cache.AsyncMeasurable;
import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Time;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

public class NodeRangeIndexCache {
    private static final int MAX_CACHE_SIZE = 100 * 1024 * 1024;
    private static final int DEFAULT_EXPIRE_TIME_MS = 60000;
    public static final int ZGC_OBJECT_HEADER_SIZE_BYTES = 16;
    public static final int MIN_CACHE_UPDATE_INTERVAL_MS = 1000; // 1s
    private static final int DEFAULT_UPDATE_CONCURRENCY_LIMIT = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRangeIndexCache.class);
    private static final AttributeKey<String> LABEL_TYPE = AttributeKey.stringKey("type");
    private static final String LABEL_STATUS_UPDATE = "update";
    private static final String LABEL_STATUS_INVALIDATE = "invalidate";
    private static final Metrics.LongCounterBundle RANGE_INDEX_CACHE_OPERATION_COUNT = Metrics.instance()
        .longCounter("kafka_stream_node_range_index_cache_operation_count", "Range index cache operation count", "");
    private static final Metrics.LongCounterBundle.LongCounter RANGE_INDEX_UPDATE_COUNT =
        RANGE_INDEX_CACHE_OPERATION_COUNT.register(MetricsLevel.INFO,
            attributes(LABEL_STATUS_UPDATE));
    private static final Metrics.LongCounterBundle.LongCounter RANGE_INDEX_INVALIDATE_COUNT =
        RANGE_INDEX_CACHE_OPERATION_COUNT.register(MetricsLevel.INFO,
            attributes(LABEL_STATUS_INVALIDATE));
    private static final Metrics.LongCounterBundle.LongCounter RANGE_INDEX_HIT_COUNT =
        RANGE_INDEX_CACHE_OPERATION_COUNT.register(MetricsLevel.INFO, attributes(S3StreamMetricsConstant.LABEL_STATUS_HIT));
    private static final Metrics.LongCounterBundle.LongCounter RANGE_INDEX_MISS_COUNT =
        RANGE_INDEX_CACHE_OPERATION_COUNT.register(MetricsLevel.INFO, attributes(S3StreamMetricsConstant.LABEL_STATUS_MISS));
    private static volatile NodeRangeIndexCache instance = null;
    private final ExpireLRUCache nodeRangeIndexMap = new ExpireLRUCache(MAX_CACHE_SIZE, DEFAULT_EXPIRE_TIME_MS);
    private final Map<Long, Long> nodeCacheUpdateTimestamp = new ConcurrentHashMap<>();
    private final Semaphore updateLimiter = new Semaphore(DEFAULT_UPDATE_CONCURRENCY_LIMIT * Systems.CPU_CORES);

    private NodeRangeIndexCache() {

    }

    public static NodeRangeIndexCache getInstance() {
        if (instance == null) {
            synchronized (NodeRangeIndexCache.class) {
                if (instance == null) {
                    instance = new NodeRangeIndexCache();
                }
            }
        }
        return instance;
    }

    /**
     * Records a remote range index cache update.
     */
    public static void recordRangeIndexUpdate() {
        RANGE_INDEX_UPDATE_COUNT.add(1);
    }

    /**
     * Records a remote range index cache invalidation.
     */
    public static void recordRangeIndexInvalidate() {
        RANGE_INDEX_INVALIDATE_COUNT.add(1);
    }

    /**
     * Records a remote range index cache hit.
     */
    public static void recordRangeIndexHit() {
        RANGE_INDEX_HIT_COUNT.add(1);
    }

    /**
     * Records a remote range index cache miss.
     */
    public static void recordRangeIndexMiss() {
        RANGE_INDEX_MISS_COUNT.add(1);
    }

    void clear() {
        this.nodeRangeIndexMap.clear();
        this.nodeCacheUpdateTimestamp.clear();
    }

    // fot test only
    boolean isValid(long nodeId) {
        return this.nodeRangeIndexMap.containsKey(nodeId);
    }

    // for test only
    ExpireLRUCache cache() {
        return this.nodeRangeIndexMap;
    }

    public synchronized void invalidate(long nodeId) {
        this.nodeRangeIndexMap.remove(nodeId);
    }

    public synchronized CompletableFuture<Long> searchObjectId(long nodeId, long streamId, long startOffset,
        Supplier<CompletableFuture<Map<Long, List<RangeIndex>>>> cacheSupplier) {
        return searchObjectId(nodeId, streamId, startOffset, cacheSupplier, Time.SYSTEM);
    }

    public synchronized CompletableFuture<Long> searchObjectId(long nodeId, long streamId, long startOffset,
        Supplier<CompletableFuture<Map<Long, List<RangeIndex>>>> cacheSupplier, Time time) {
        StreamRangeIndexCache indexCache = this.nodeRangeIndexMap.get(nodeId);
        if (indexCache == null) {
            long now = time.milliseconds();
            long expect = this.nodeCacheUpdateTimestamp.getOrDefault(nodeId, 0L) + MIN_CACHE_UPDATE_INTERVAL_MS;
            if (expect > now) {
                // Skip updating from remote
                return CompletableFuture.completedFuture(-1L);
            }
            if (!updateLimiter.tryAcquire()) {
                return CompletableFuture.completedFuture(-1L);
            }

            this.nodeCacheUpdateTimestamp.put(nodeId, now);
            CompletableFuture<Map<Long, List<RangeIndex>>> cf = cacheSupplier.get();
            cf.whenComplete((v, e) -> updateLimiter.release());
            indexCache = new StreamRangeIndexCache(cf);
            this.nodeRangeIndexMap.put(nodeId, indexCache);
            recordRangeIndexUpdate();
            LOGGER.info("Update stream range index for node {}, concurrency limiter left: {}", nodeId, updateLimiter.availablePermits());
        }
        return indexCache.searchObjectId(streamId, startOffset);
    }

    private static Attributes attributes(String type) {
        return Attributes.of(LABEL_TYPE, type);
    }

    static class StreamRangeIndexCache implements AsyncMeasurable {
        private final CompletableFuture<Map<Long, List<RangeIndex>>> streamRangeIndexMapCf;
        private CompletableFuture<Integer> sizeCf;

        public StreamRangeIndexCache(CompletableFuture<Map<Long, List<RangeIndex>>> streamRangeIndexMapCf) {
            this.streamRangeIndexMapCf = streamRangeIndexMapCf;
        }

        public CompletableFuture<Long> searchObjectId(long streamId, long startOffset) {
            return this.streamRangeIndexMapCf.thenApply(v -> LocalStreamRangeIndexCache.binarySearchObjectId(startOffset, v.get(streamId)));
        }

        @Override
        public synchronized CompletableFuture<Integer> size() {
            if (sizeCf == null) {
                sizeCf = this.streamRangeIndexMapCf.thenApply(v -> v.values().stream()
                    .mapToInt(rangeIndices -> Long.BYTES + ZGC_OBJECT_HEADER_SIZE_BYTES + rangeIndices.size() * RangeIndex.OBJECT_SIZE).sum());
            }
            return sizeCf;
        }

        @Override
        public void close() {

        }
    }

    static class ExpireLRUCache extends AsyncLRUCache<Long, StreamRangeIndexCache> {
        private static final Object DUMMY_OBJECT = new Object();
        private final Cache<Long, Object> expireCache;

        public ExpireLRUCache(int maxSize, int expireTimeMs) {
            this(maxSize, expireTimeMs, Ticker.systemTicker());
        }

        public ExpireLRUCache(int maxSize, int expireTimeMs, Ticker ticker) {
            super("NodeRangeIndex", maxSize);
            expireCache = CacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(Duration.ofMillis(expireTimeMs))
                .removalListener((RemovalListener<Long, Object>) notification -> remove(notification.getKey()))
                .build();
        }

        @Override
        public synchronized void put(Long key, StreamRangeIndexCache value) {
            super.put(key, value);
            touch(key);
        }

        @Override
        public synchronized StreamRangeIndexCache get(Long key) {
            touch(key);
            return super.get(key);
        }

        private void touch(long key) {
            try {
                expireCache.get(key, () -> DUMMY_OBJECT);
            } catch (Exception ignored) {

            }
        }
    }
}
