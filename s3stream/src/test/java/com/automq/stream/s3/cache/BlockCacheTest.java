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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class BlockCacheTest {

    private static StreamRecordBatch newRecord(long streamId, long offset, int count, int size) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(size));
    }

    private BlockCache createBlockCache() {
        BlockCache blockCache = new BlockCache(1024 * 1024 * 1024);

        blockCache.put(233L, List.of(
            newRecord(233L, 10L, 2, 1),
            newRecord(233L, 12L, 2, 1)
        ));

        blockCache.put(233L, List.of(
            newRecord(233L, 16L, 4, 1),
            newRecord(233L, 20L, 2, 1)
        ));

        // overlap
        blockCache.put(233L, List.of(
            newRecord(233L, 12L, 2, 1),
            newRecord(233L, 14L, 1, 1),
            newRecord(233L, 15L, 1, BlockCache.BLOCK_SIZE),
            newRecord(233L, 16L, 4, 1),
            newRecord(233L, 20L, 2, 1),
            newRecord(233L, 22L, 1, 1),
            newRecord(233L, 23L, 1, 1)
        ));
        return blockCache;
    }

    @Test
    public void testPutGet() {
        BlockCache blockCache = createBlockCache();

        BlockCache.GetCacheResult rst = blockCache.get(233L, 10L, 24L, BlockCache.BLOCK_SIZE * 2);
        List<StreamRecordBatch> records = rst.getRecords();
        assertEquals(8, records.size());
        assertEquals(10L, records.get(0).getBaseOffset());
        assertEquals(12L, records.get(1).getBaseOffset());
        assertEquals(14L, records.get(2).getBaseOffset());
        assertEquals(15L, records.get(3).getBaseOffset());
        assertEquals(16L, records.get(4).getBaseOffset());
        assertEquals(20L, records.get(5).getBaseOffset());
        assertEquals(22L, records.get(6).getBaseOffset());
        assertEquals(23L, records.get(7).getBaseOffset());
    }

    @Test
    public void testPutGet2() {
        BlockCache blockCache = createBlockCache();

        BlockCache.GetCacheResult rst = blockCache.get(233L, 18L, 22L, BlockCache.BLOCK_SIZE * 2);
        List<StreamRecordBatch> records = rst.getRecords();
        assertEquals(2, records.size());
        assertEquals(16L, records.get(0).getBaseOffset());
        assertEquals(20L, records.get(1).getBaseOffset());
    }

    @Test
    public void testPutGet3() {
        BlockCache blockCache = createBlockCache();
        blockCache.put(233L, 26L, 40L, List.of(
            newRecord(233L, 26L, 4, 1),
            newRecord(233L, 30L, 10, 4)
        ));

        BlockCache.GetCacheResult rst = blockCache.get(233L, 27L, 35L, BlockCache.BLOCK_SIZE * 2);
        List<StreamRecordBatch> records = rst.getRecords();
        assertEquals(2, records.size());
        assertEquals(26L, records.get(0).getBaseOffset());
        assertEquals(30L, records.get(1).getBaseOffset());
        assertEquals(1, rst.getReadAheadRecords().size());
        assertEquals(new DefaultS3BlockCache.ReadAheadRecord(40L), rst.getReadAheadRecords().get(0));
    }

    @Test
    public void testRangeCheck() {
        BlockCache blockCache = createBlockCache();
        blockCache.put(233L, List.of(
            newRecord(233L, 26L, 4, 1),
            newRecord(233L, 30L, 10, 4)
        ));

        assertTrue(blockCache.checkRange(233, 10, 2));
        assertTrue(blockCache.checkRange(233, 11, BlockCache.BLOCK_SIZE));
        assertTrue(blockCache.checkRange(233, 20, 3));
        assertTrue(blockCache.checkRange(233, 26, 4));
        assertFalse(blockCache.checkRange(233, 20, 6));
    }

    @Test
    public void testEvict() {
        BlockCache blockCache = new BlockCache(4);
        blockCache.put(233L, List.of(
            newRecord(233L, 10L, 2, 2),
            newRecord(233L, 12L, 2, 1)
        ));

        assertEquals(2, blockCache.get(233L, 10L, 20L, 1000).getRecords().size());

        blockCache.put(233L, List.of(
            newRecord(233L, 16L, 4, 1),
            newRecord(233L, 20L, 2, 1)
        ));
        assertEquals(0, blockCache.get(233L, 10L, 20L, 1000).getRecords().size());
        assertEquals(2, blockCache.get(233L, 16, 21L, 1000).getRecords().size());
    }

    @Test
    public void testLRU() {
        LRUCache<Long, Boolean> lru = new LRUCache<>();
        lru.put(1L, true);
        lru.put(2L, true);
        lru.put(3L, true);
        lru.touchIfExist(2L);
        assertEquals(1, lru.pop().getKey());
        assertEquals(3, lru.pop().getKey());
        assertEquals(2, lru.pop().getKey());
        assertNull(lru.pop());
    }

    @Test
    public void testReadAhead() {
        BlockCache blockCache = new BlockCache(16 * 1024 * 1024);
        blockCache.put(233L, 10L, 12L, List.of(
            newRecord(233L, 10, 1, 1024 * 1024),
            newRecord(233L, 11, 1, 1024)
        ));

        // first read the block
        BlockCache.GetCacheResult rst = blockCache.get(233L, 10, 11, Integer.MAX_VALUE);
        assertEquals(1, rst.getRecords().size());
        assertEquals(10L, rst.getRecords().get(0).getBaseOffset());
        assertEquals(12, rst.getReadAheadRecords().get(0).nextRAOffset());

        // repeat read the block, the readahead mark is clear.
        rst = blockCache.get(233L, 10, 11, Integer.MAX_VALUE);
        assertTrue(rst.getReadAheadRecords().isEmpty());
    }

}
