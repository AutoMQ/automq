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
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class LogCacheTest {

    @Test
    public void testPutGet() {
        LogCache logCache = new LogCache(1024 * 1024, 1024 * 1024);
        long walOffset = 0L;

        logCache.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        logCache.put(new StreamRecordBatch(233L, 0L, 11L, 2, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 13L, 2, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 20L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        logCache.put(new StreamRecordBatch(233L, 0L, 21L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

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
        long walOffset = 0L;
        for (int i = 0; i < 100000; i++) {
            cache.put(new StreamRecordBatch(233L, 0L, i, 1, TestUtils.random(1)), DefaultRecordOffset.of(0, walOffset++, 0));
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
        long walOffset = 0L;

        logCache.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        logCache.put(new StreamRecordBatch(233L, 0L, 11L, 2, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 13L, 2, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        logCache.put(new StreamRecordBatch(234L, 0L, 13L, 2, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

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
        long walOffset = 0L;
        left.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        LogCacheBlock right = new LogCacheBlock(1024L * 1024);
        right.put(new StreamRecordBatch(233L, 0L, 13L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        assertTrue(LogCache.isDiscontinuous(left, right));

        left = new LogCacheBlock(1024L * 1024);
        walOffset = 0L;
        left.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        left.put(new StreamRecordBatch(234L, 0L, 10L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        right = new LogCacheBlock(1024L * 1024);
        right.put(new StreamRecordBatch(233L, 0L, 11L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        assertFalse(LogCache.isDiscontinuous(left, right));
    }

    @Test
    public void testMergeBlock() {
        long size = 0;
        LogCacheBlock left = new LogCacheBlock(1024L * 1024);
        long walOffset = 0L;
        left.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        left.put(new StreamRecordBatch(234L, 0L, 100L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        size += left.size();

        LogCacheBlock right = new LogCacheBlock(1024L * 1024);
        right.put(new StreamRecordBatch(233L, 0L, 11L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        right.put(new StreamRecordBatch(235L, 0L, 200L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
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
    public void testTruncateStreamRecords() {
        LogCache cache = new LogCache(1024 * 1024, 1024 * 1024);
        long walOffset = 0L;

        cache.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));
        cache.put(new StreamRecordBatch(233L, 0L, 11L, 1, TestUtils.random(20)), DefaultRecordOffset.of(0, walOffset++, 0));

        Optional<LogCache.TruncateResult> result = cache.truncateStreamRecords(233L, 12L);
        assertTrue(result.isPresent());
        LogCache.TruncateResult truncateResult = result.get();
        assertTrue(truncateResult.freedBytes > 0);
        assertNotNull(truncateResult.firstRemovedWalOffset);
        assertEquals(1L, DefaultRecordOffset.of(truncateResult.firstRemovedWalOffset).offset());

        List<StreamRecordBatch> remaining = cache.get(233L, 12L, 14L, 1000);
        assertEquals(0, remaining.size());

        assertTrue(cache.truncateStreamRecords(233L, 12L).isEmpty());
    }

}
