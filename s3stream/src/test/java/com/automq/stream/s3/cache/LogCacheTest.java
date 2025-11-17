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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.LogCache.LogCacheBlock;
import com.automq.stream.s3.model.StreamRecordBatch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class LogCacheTest {

    @Test
    public void testPutGet() {
        LogCache logCache = new LogCache(1024 * 1024, 1024 * 1024);

        logCache.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)));
        logCache.put(new StreamRecordBatch(233L, 0L, 11L, 2, TestUtils.random(20)));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 13L, 2, TestUtils.random(20)));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 20L, 1, TestUtils.random(20)));
        logCache.put(new StreamRecordBatch(233L, 0L, 21L, 1, TestUtils.random(20)));

        List<StreamRecordBatch> records = logCache.get(233L, 10L, 21L, 1000);
        assertEquals(1, records.size());
        assertEquals(20L, records.get(0).getBaseOffset());

        records = logCache.get(233L, 10L, 15L, 1000);
        assertEquals(3, records.size());
        assertEquals(10L, records.get(0).getBaseOffset());

        records = logCache.get(233L, 0L, 9L, 1000);
        assertEquals(0, records.size());

        records = logCache.get(233L, 10L, 16L, 1000);
        assertEquals(0, records.size());

        records = logCache.get(233L, 12L, 16L, 1000);
        assertEquals(0, records.size());
    }

    @Test
    public void testOffsetIndex() {
        LogCache cache = new LogCache(Integer.MAX_VALUE, Integer.MAX_VALUE);

        for (int i = 0; i < 100000; i++) {
            cache.put(new StreamRecordBatch(233L, 0L, i, 1, TestUtils.random(1)));
        }

        long start = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            cache.get(233L, i, i + 1, 1000);
        }
        System.out.println("cost: " + (System.nanoTime() - start) / 1000 + "us");
        Map<Long, LogCache.IndexAndCount> offsetIndexMap = cache.blocks.get(0).map.get(233L).offsetIndexMap;
        assertEquals(1, offsetIndexMap.size());
        assertEquals(100000, offsetIndexMap.get(100000L).index);
    }

    @Test
    public void testClearStreamRecords() {
        LogCache logCache = new LogCache(1024 * 1024, 1024 * 1024);

        logCache.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)));
        logCache.put(new StreamRecordBatch(233L, 0L, 11L, 2, TestUtils.random(20)));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 13L, 2, TestUtils.random(20)));

        logCache.put(new StreamRecordBatch(234L, 0L, 13L, 2, TestUtils.random(20)));

        assertTrue(logCache.blocks.get(0).containsStream(233L));
        assertTrue(logCache.blocks.get(1).containsStream(234L));
        logCache.clearStreamRecords(233L);
        assertFalse(logCache.blocks.get(0).containsStream(233L));
        assertTrue(logCache.blocks.get(1).containsStream(234L));

        logCache.clearStreamRecords(234L);
        assertFalse(logCache.blocks.get(0).containsStream(233L));
        assertFalse(logCache.blocks.get(1).containsStream(234L));
    }

    @Test
    public void testIsDiscontinuous() {
        LogCacheBlock left = new LogCacheBlock(1024L * 1024);
        left.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)));

        LogCacheBlock right = new LogCacheBlock(1024L * 1024);
        right.put(new StreamRecordBatch(233L, 0L, 13L, 1, TestUtils.random(20)));

        assertTrue(LogCache.isDiscontinuous(left, right));

        left = new LogCacheBlock(1024L * 1024);
        left.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)));
        left.put(new StreamRecordBatch(234L, 0L, 10L, 1, TestUtils.random(20)));

        right = new LogCacheBlock(1024L * 1024);
        right.put(new StreamRecordBatch(233L, 0L, 11L, 1, TestUtils.random(20)));
        assertFalse(LogCache.isDiscontinuous(left, right));
    }

    @Test
    public void testMergeBlock() {
        long size = 0;
        LogCacheBlock left = new LogCacheBlock(1024L * 1024);
        left.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)));
        left.put(new StreamRecordBatch(234L, 0L, 100L, 1, TestUtils.random(20)));
        size += left.size();

        LogCacheBlock right = new LogCacheBlock(1024L * 1024);
        right.put(new StreamRecordBatch(233L, 0L, 11L, 1, TestUtils.random(20)));
        right.put(new StreamRecordBatch(235L, 0L, 200L, 1, TestUtils.random(20)));
        size += right.size();

        LogCache.mergeBlock(left, right);
        assertEquals(size, left.size());
        LogCache.StreamCache stream233 = left.map.get(233L);
        assertEquals(10, stream233.startOffset());
        assertEquals(12, stream233.endOffset());
        assertEquals(2, stream233.records.size());
        assertEquals(10, stream233.records.get(0).getBaseOffset());
        assertEquals(11, stream233.records.get(1).getBaseOffset());

        LogCache.StreamCache stream234 = left.map.get(234L);
        assertEquals(100, stream234.startOffset());
        assertEquals(101, stream234.endOffset());
        assertEquals(1, stream234.records.size());
        assertEquals(100, stream234.records.get(0).getBaseOffset());

        LogCache.StreamCache stream235 = left.map.get(235L);
        assertEquals(200, stream235.startOffset());
        assertEquals(201, stream235.endOffset());
        assertEquals(1, stream235.records.size());
        assertEquals(200, stream235.records.get(0).getBaseOffset());
    }

    @Test
    public void testTryMergeLogic() throws ExecutionException, InterruptedException {
        LogCache logCache = new LogCache(Long.MAX_VALUE, 10_000L);
        final long streamId = 233L;
        final int blocksToCreate = LogCache.MERGE_BLOCK_THRESHOLD + 2;

        // create multiple blocks, each containing one record for the same stream with contiguous offsets
        for (int i = 0; i < blocksToCreate; i++) {
            logCache.put(new StreamRecordBatch(streamId, 0L, i, 1, TestUtils.random(1)));
            logCache.archiveCurrentBlock();
        }

        int before = logCache.blocks.size();
        assertTrue(before > LogCache.MERGE_BLOCK_THRESHOLD, "need more than 8 blocks to exercise tryMerge");

        LogCache.LogCacheBlock left = logCache.blocks.get(0);
        LogCache.LogCacheBlock right = logCache.blocks.get(1);

        // verify contiguous condition before merge: left.end == right.start
        LogCache.StreamCache leftCache = left.map.get(streamId);
        LogCache.StreamCache rightCache = right.map.get(streamId);
        assertEquals(leftCache.endOffset(), rightCache.startOffset());

        // mark both blocks free to trigger tryMerge (called inside markFree)
        logCache.markFree(left).get();
        logCache.markFree(right).get();

        int after = logCache.blocks.size();
        assertEquals(before - 1, after, "two adjacent free contiguous blocks should be merged into one");

        // verify merged block contains both records and correct range
        LogCache.LogCacheBlock merged = logCache.blocks.get(0);
        assertTrue(merged.free);
        LogCache.StreamCache mergedCache = merged.map.get(streamId);
        assertEquals(2, mergedCache.records.size());
        assertEquals(0L, mergedCache.startOffset());
        assertEquals(2L, mergedCache.endOffset());
        assertEquals(0L, mergedCache.records.get(0).getBaseOffset());
        assertEquals(1L, mergedCache.records.get(1).getBaseOffset());
    }

}
