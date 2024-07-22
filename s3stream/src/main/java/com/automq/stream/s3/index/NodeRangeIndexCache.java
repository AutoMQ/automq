/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.index;

import com.automq.stream.s3.cache.AsyncMeasurable;
import com.automq.stream.s3.cache.AsyncObjectLRUCache;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRangeIndexCache {
    private static final Integer MAX_CACHE_SIZE = 100 * 1024 * 1024;
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
        LOGGER.info("Invalidate stream range index for node {}", nodeId);
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

        public StreamRangeIndexCache(CompletableFuture<Map<Long, List<RangeIndex>>> streamRangeIndexMapCf) {
            this.streamRangeIndexMapCf = streamRangeIndexMapCf;
        }

        public CompletableFuture<Long> searchObjectId(long streamId, long startOffset) {
            return this.streamRangeIndexMapCf.thenApply(v -> LocalStreamRangeIndexCache.binarySearchObjectId(startOffset, v.get(streamId)));
        }

        @Override
        public CompletableFuture<Integer> size() {
            return this.streamRangeIndexMapCf.thenApply(v -> v.values().stream()
                .mapToInt(rangeIndices -> Long.BYTES + rangeIndices.size() * RangeIndex.SIZE).sum());
        }

        @Override
        public void close() {

        }
    }

    static class LRUCache extends AsyncObjectLRUCache<Long, StreamRangeIndexCache> {
        public LRUCache(int maxSize) {
            super(maxSize);
        }
    }
}
