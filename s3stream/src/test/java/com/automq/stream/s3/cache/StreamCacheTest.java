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

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.model.StreamRecordBatch;
import java.util.List;
import java.util.NavigableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamCacheTest {

    @Test
    public void testTailMap() {
        StreamCache streamCache = new StreamCache();
        BlockCache.CacheBlock block1 = new BlockCache.CacheBlock(List.of(
            new StreamRecordBatch(0, 0, 0, 10, TestUtils.random(8)),
            new StreamRecordBatch(0, 0, 10, 20, TestUtils.random(8))), null);
        streamCache.put(block1);

        BlockCache.CacheBlock block2 = new BlockCache.CacheBlock(List.of(
            new StreamRecordBatch(0, 0, 50, 20, TestUtils.random(8)),
            new StreamRecordBatch(0, 0, 70, 30, TestUtils.random(8))), null);
        streamCache.put(block2);

        BlockCache.CacheBlock block3 = new BlockCache.CacheBlock(List.of(
            new StreamRecordBatch(0, 0, 30, 20, TestUtils.random(8))), null);
        streamCache.put(block3);

        NavigableMap<Long, BlockCache.CacheBlock> tailBlocks = streamCache.tailBlocks(5);
        Assertions.assertEquals(streamCache.blocks(), tailBlocks);

        tailBlocks = streamCache.tailBlocks(80);
        Assertions.assertEquals(1, tailBlocks.size());
        Assertions.assertEquals(50, tailBlocks.firstKey());
        Assertions.assertEquals(block2, tailBlocks.firstEntry().getValue());
    }

    @Test
    public void testRemove() {
        StreamCache streamCache = new StreamCache();
        streamCache.put(new BlockCache.CacheBlock(List.of(
            new StreamRecordBatch(0, 0, 0, 10, TestUtils.random(8)),
            new StreamRecordBatch(0, 0, 10, 20, TestUtils.random(8))), null));

        streamCache.put(new BlockCache.CacheBlock(List.of(
            new StreamRecordBatch(0, 0, 50, 20, TestUtils.random(8)),
            new StreamRecordBatch(0, 0, 70, 30, TestUtils.random(8))), null));

        streamCache.put(new BlockCache.CacheBlock(List.of(
            new StreamRecordBatch(0, 0, 30, 20, TestUtils.random(8))), null));

        streamCache.remove(30);

    }
}
