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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.model.StreamRecordBatch;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

}
