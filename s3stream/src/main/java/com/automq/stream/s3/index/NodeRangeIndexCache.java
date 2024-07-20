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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRangeIndexCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRangeIndexCache.class);
    private volatile static NodeRangeIndexCache instance = null;
    private final Map<Long, StreamRangeIndexCache> nodeRangeIndexMap = new ConcurrentHashMap<>();

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

    public void clear() {
        this.nodeRangeIndexMap.clear();
    }

    // fot test only
    boolean isValid(long nodeId) {
        StreamRangeIndexCache indexCache = this.nodeRangeIndexMap.get(nodeId);
        return indexCache != null && indexCache.isValid();
    }

    public void invalidate(long nodeId) {
        this.nodeRangeIndexMap.computeIfPresent(nodeId, (k, v) -> {
            v.invalidate();
            LOGGER.info("Invalidate stream range index for node {}", nodeId);
            return v;
        });
    }

    public CompletableFuture<Long> searchObjectId(long nodeId, long streamId, long startOffset,
        Supplier<CompletableFuture<Map<Long, List<RangeIndex>>>> cacheSupplier) {
        StreamRangeIndexCache indexCache = this.nodeRangeIndexMap.compute(nodeId, (k, v) -> {
            if (v == null || !v.isValid()) {
                LOGGER.info("Update stream range index for node {}", nodeId);
                return new StreamRangeIndexCache(cacheSupplier.get());
            }
            return v;
        });
        return indexCache.searchObjectId(streamId, startOffset);
    }

    static class StreamRangeIndexCache {
        private final CompletableFuture<Map<Long, List<RangeIndex>>> streamRangeIndexMapCf;
        private boolean valid;

        public StreamRangeIndexCache(CompletableFuture<Map<Long, List<RangeIndex>>> streamRangeIndexMapCf) {
            this.streamRangeIndexMapCf = streamRangeIndexMapCf;
            this.valid = true;
        }

        public CompletableFuture<Long> searchObjectId(long streamId, long startOffset) {
            if (!this.valid) {
                return CompletableFuture.completedFuture(-1L);
            }
            return this.streamRangeIndexMapCf.thenApply(v -> LocalStreamRangeIndexCache.binarySearchObjectId(startOffset, v.get(streamId)));
        }

        public void invalidate() {
            this.valid = false;
        }

        public boolean isValid() {
            return this.valid;
        }
    }
}
