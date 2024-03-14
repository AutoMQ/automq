/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class StreamCache {
    private final NavigableMap<Long, BlockCache.CacheBlock> blocks = new TreeMap<>();

    public void put(BlockCache.CacheBlock cacheBlock) {
        blocks.put(cacheBlock.firstOffset, cacheBlock);
    }

    public BlockCache.CacheBlock remove(long startOffset) {
        return blocks.remove(startOffset);
    }

    NavigableMap<Long, BlockCache.CacheBlock> blocks() {
        return blocks;
    }

    public NavigableMap<Long, BlockCache.CacheBlock> tailBlocks(long startOffset) {
        Map.Entry<Long, BlockCache.CacheBlock> floorEntry = blocks.floorEntry(startOffset);
        return blocks.tailMap(floorEntry != null ? floorEntry.getKey() : startOffset, true);
    }
}
