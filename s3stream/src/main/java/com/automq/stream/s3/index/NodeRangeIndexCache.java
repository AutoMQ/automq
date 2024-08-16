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

package com.automq.stream.s3.index;

import com.automq.stream.s3.cache.AsyncMeasurable;
import com.automq.stream.s3.cache.AsyncLRUCache;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRangeIndexCache {
    private static final int MAX_CACHE_SIZE = 100 * 1024 * 1024;
    public static final int ZGC_OBJECT_HEADER_SIZE_BYTES = 16;
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRangeIndexCache.class);
    private volatile static NodeRangeIndexCache instance = null;
    private final LRUCache nodeRangeIndexMap = new LRUCache(MAX_CACHE_SIZE);

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

    void clear() {
        this.nodeRangeIndexMap.clear();
    }

    // fot test only
    boolean isValid(long nodeId) {
        return this.nodeRangeIndexMap.containsKey(nodeId);
    }

    // for test only
    LRUCache cache() {
        return this.nodeRangeIndexMap;
    }

    public synchronized void invalidate(long nodeId) {
        this.nodeRangeIndexMap.remove(nodeId);
    }

    public synchronized CompletableFuture<Long> searchObjectId(long nodeId, long streamId, long startOffset,
        Supplier<CompletableFuture<Map<Long, List<RangeIndex>>>> cacheSupplier) {
        StreamRangeIndexCache indexCache = this.nodeRangeIndexMap.get(nodeId);
        if (indexCache == null) {
            indexCache = new StreamRangeIndexCache(cacheSupplier.get());
            this.nodeRangeIndexMap.put(nodeId, indexCache);
            LOGGER.info("Update stream range index for node {}", nodeId);
        }
        return indexCache.searchObjectId(streamId, startOffset);
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

    static class LRUCache extends AsyncLRUCache<Long, StreamRangeIndexCache> {
        public LRUCache(int maxSize) {
            super("NodeRangeIndex", maxSize);
        }
    }
}
