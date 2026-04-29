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

package com.automq.stream.s3;

import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.automq.stream.s3.TestUtils.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class WALRecoveryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALRecoveryTest.class);

    private static StreamRecordBatch newRecord(long streamId, long offset) {
        return StreamRecordBatch.of(streamId, 0, offset, 1, random(1));
    }

    @Test
    public void testRecoverContinuousRecords() {
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(newRecord(233L, 10L)),
            new TestRecoverResult(newRecord(233L, 11L)),
            new TestRecoverResult(newRecord(233L, 12L)),
            new TestRecoverResult(newRecord(233L, 15L)),
            new TestRecoverResult(newRecord(234L, 20L))
        );

        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(233L, 11L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 1 << 30, LOGGER, results::add);
        assertEquals(1, results.size());
        LogCache.LogCacheBlock cacheBlock = results.get(0);
        // ignore closed stream (234), drop discontinuous record (15).
        assertEquals(1, cacheBlock.records().size());
        List<StreamRecordBatch> streamRecords = cacheBlock.records().get(233L);
        assertEquals(2, streamRecords.size());
        assertEquals(11L, streamRecords.get(0).getBaseOffset());
        assertEquals(12L, streamRecords.get(1).getBaseOffset());
    }

    @Test
    public void testRecoverDataLoss() {
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(newRecord(233L, 10L)),
            new TestRecoverResult(newRecord(233L, 11L)),
            new TestRecoverResult(newRecord(233L, 12L))
        );

        // simulate data loss: stream endOffset=5 but first WAL record starts at 10
        // all records are dropped as discontinuous
        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(233L, 5L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 1 << 30, LOGGER, results::add);
        assertEquals(1, results.size());
        assertEquals(0, results.get(0).records().size());
    }

    @Test
    public void testRecoverOutOfOrderRecords() {
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(newRecord(42L, 9L)),
            new TestRecoverResult(newRecord(42L, 10L)),
            new TestRecoverResult(newRecord(42L, 13L)),
            new TestRecoverResult(newRecord(42L, 11L)),
            new TestRecoverResult(newRecord(42L, 12L)),
            new TestRecoverResult(newRecord(42L, 14L)),
            new TestRecoverResult(newRecord(42L, 20L))
        );

        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 10L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 1 << 30, LOGGER, results::add);
        assertEquals(1, results.size());
        LogCache.LogCacheBlock cacheBlock = results.get(0);
        // Discontinuous records (13, 14, 20) are dropped; only continuous records from endOffset are kept.
        assertEquals(1, cacheBlock.records().size());
        List<StreamRecordBatch> streamRecords = cacheBlock.records().get(42L);
        assertEquals(3, streamRecords.size());
        assertEquals(10L, streamRecords.get(0).getBaseOffset());
        assertEquals(11L, streamRecords.get(1).getBaseOffset());
        assertEquals(12L, streamRecords.get(2).getBaseOffset());
    }

    @Test
    public void testSegmentedRecovery() {
        // Each record occupies ~145 bytes (1 byte payload + 144 overhead).
        // With maxCacheSize=200, a block becomes full after 2 records (290 bytes >= 200).
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(newRecord(42L, 10L)),
            new TestRecoverResult(newRecord(42L, 11L)),
            new TestRecoverResult(newRecord(42L, 12L)),
            new TestRecoverResult(newRecord(42L, 13L)),
            new TestRecoverResult(newRecord(42L, 14L))
        );

        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 10L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 200L, LOGGER, results::add);
        assertEquals(3, results.size());

        // block 0: [10, 11] — full
        assertTrue(results.get(0).isFull());
        assertEquals(List.of(10L, 11L),
            results.get(0).records().get(42L).stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList()));

        // block 1: [12, 13] — full
        assertTrue(results.get(1).isFull());
        assertEquals(List.of(12L, 13L),
            results.get(1).records().get(42L).stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList()));

        // block 2: [14] — not full
        assertFalse(results.get(2).isFull());
        assertEquals(List.of(14L),
            results.get(2).records().get(42L).stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList()));

        // streamEndOffsets should be updated to the last recovered offset
        assertEquals(15L, streamEndOffsets.get(42L));
    }

    /**
     * When offset delta overflow forces a block split, records after the split must still be recovered.
     */
    @Test
    public void testSegmentedRecoveryPendingRecordUpdatesOffset() {
        // Offset delta overflow forces a block split after the first record.
        // All three records must be recovered across the resulting blocks.
        long bigCount = Integer.MAX_VALUE;
        long secondOffset = bigCount;
        long thirdOffset = secondOffset + 1;
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, 0L, (int) bigCount, random(1))),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, secondOffset, 1, random(1))),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, thirdOffset, 1, random(1)))
        );

        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 0L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 1L << 30, LOGGER, results::add);

        // Should produce 2 blocks (overflow forces split)
        assertTrue(results.size() >= 2, "Expected multiple blocks but got " + results.size());

        // Collect all recovered base offsets
        List<Long> allOffsets = new ArrayList<>();
        for (LogCache.LogCacheBlock block : results) {
            List<StreamRecordBatch> records = block.records().get(42L);
            if (records != null) {
                records.forEach(r -> allOffsets.add(r.getBaseOffset()));
            }
        }
        // All 3 records must be present
        assertEquals(List.of(0L, secondOffset, thirdOffset), allOffsets);
    }

    /**
     * Each block's lastRecordOffset must reflect the last record actually added to that block.
     */
    @Test
    public void testSegmentedRecoveryLastRecordOffset() {
        long bigCount = Integer.MAX_VALUE;
        long secondOffset = bigCount;
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, 0L, (int) bigCount, random(1)),
                DefaultRecordOffset.of(0, 100, 0)),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, secondOffset, 1, random(1)),
                DefaultRecordOffset.of(0, 200, 0)),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, secondOffset + 1, 1, random(1)),
                DefaultRecordOffset.of(0, 300, 0))
        );

        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 0L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 1L << 30, LOGGER, results::add);
        assertTrue(results.size() >= 2, "Expected multiple blocks but got " + results.size());

        // The first block's lastRecordOffset must be the WAL offset of the last record added to it.
        LogCache.LogCacheBlock firstBlock = results.get(0);
        assertEquals(DefaultRecordOffset.of(0, 100, 0), firstBlock.lastRecordOffset());
    }

    static class TestRecoverResult implements RecoverResult {
        private final StreamRecordBatch record;
        private final RecordOffset offset;

        public TestRecoverResult(StreamRecordBatch record) {
            this(record, DefaultRecordOffset.of(0, 0, 0));
        }

        public TestRecoverResult(StreamRecordBatch record, RecordOffset offset) {
            this.record = record;
            this.offset = offset;
        }

        @Override
        public StreamRecordBatch record() {
            return record;
        }

        @Override
        public RecordOffset recordOffset() {
            return offset;
        }
    }
}
