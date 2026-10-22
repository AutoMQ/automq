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

import com.automq.stream.api.LinkRecordDecoder;
import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.TestUtils.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class WALRecoveryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALRecoveryTest.class);

    private static StreamRecordBatch newRecord(long streamId, long offset) {
        return StreamRecordBatch.of(streamId, 0, offset, 1, random(1), DefaultByteBufSupplier.INSTANCE);
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
        // Each record occupies 1025 bytes (1 byte payload + 1 KiB cache overhead).
        // With maxCacheSize=2000, a block becomes full after 2 records (2050 bytes >= 2000).
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(newRecord(42L, 10L)),
            new TestRecoverResult(newRecord(42L, 11L)),
            new TestRecoverResult(newRecord(42L, 12L)),
            new TestRecoverResult(newRecord(42L, 13L)),
            new TestRecoverResult(newRecord(42L, 14L))
        );

        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 10L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 2_000L, LOGGER, results::add);
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
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, 0L, (int) bigCount, random(1), DefaultByteBufSupplier.INSTANCE)),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, secondOffset, 1, random(1), DefaultByteBufSupplier.INSTANCE)),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, thirdOffset, 1, random(1), DefaultByteBufSupplier.INSTANCE))
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
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, 0L, (int) bigCount, random(1), DefaultByteBufSupplier.INSTANCE),
                DefaultRecordOffset.of(0, 100, 0)),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, secondOffset, 1, random(1), DefaultByteBufSupplier.INSTANCE),
                DefaultRecordOffset.of(0, 200, 0)),
            new TestRecoverResult(StreamRecordBatch.of(42L, 0, secondOffset + 1, 1, random(1), DefaultByteBufSupplier.INSTANCE),
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

    /**
     * Expanded link payloads determine block boundaries and accounting while preserving WAL and stream offsets.
     */
    @Test
    public void testRecoverLinkRecordsUsesDecodedSize() {
        List<RecoverResult> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            records.add(new TestRecoverResult(newLinkRecord(42, i), DefaultRecordOffset.of(0, i, 0)));
        }
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(4096);
        when(decoder.decode(any(), any())).thenAnswer(invocation -> {
            StreamRecordBatch src = invocation.getArgument(0);
            StreamRecordBatch decoded = StreamRecordBatch.of(src.getStreamId(), src.getEpoch(), src.getBaseOffset(),
                -src.getCount(), random(4096), DefaultByteBufSupplier.INSTANCE);
            src.release();
            return CompletableFuture.completedFuture(decoded);
        });
        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(records.iterator(), streamEndOffsets, 6000, LOGGER, blocks::add);
            assertEquals(3, blocks.size());
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L), blocks.stream()
                .flatMap(block -> block.records().get(42L).stream()).map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList()));
            assertEquals(2L * (4096 + 1024), blocks.get(0).size());
            assertEquals(DefaultRecordOffset.of(0, 1, 0), blocks.get(0).lastRecordOffset());
            assertEquals(DefaultRecordOffset.of(0, 3, 0), blocks.get(1).lastRecordOffset());
            assertEquals(DefaultRecordOffset.of(0, 4, 0), blocks.get(2).lastRecordOffset());
            assertEquals(5L, streamEndOffsets.get(42L));
        } finally {
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /**
     * Recovery splits at the per-stream payload limit even with an unbounded cache limit.
     */
    @ParameterizedTest
    @ValueSource(ints = {600_000_000, 1 << 30})
    public void testRecoverLinkRecordsPreventsSizeOverflow(int decodedSize) {
        List<RecoverResult> records = List.of(
            new TestRecoverResult(newLinkRecord(42, 0), DefaultRecordOffset.of(0, 100, 0)),
            new TestRecoverResult(newLinkRecord(42, 1), DefaultRecordOffset.of(0, 200, 0)),
            new TestRecoverResult(newLinkRecord(42, 2), DefaultRecordOffset.of(0, 300, 0)));
        LinkRecordDecoder decoder = largeRecordDecoder(decodedSize);
        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(records.iterator(), streamEndOffsets, Long.MAX_VALUE, LOGGER, blocks::add);
            assertEquals(3, blocks.size());
            for (int i = 0; i < blocks.size(); i++) {
                assertEquals((long) decodedSize + 1024, blocks.get(i).size());
                assertEquals(List.of((long) i), blocks.get(i).records().get(42L).stream()
                    .map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList()));
                assertEquals(DefaultRecordOffset.of(0, (i + 1) * 100, 0), blocks.get(i).lastRecordOffset());
                assertTrue(DefaultUploadWriteAheadLogTask.streamSize(blocks.get(i).records().get(42L)) > 0);
            }
            assertEquals(3L, streamEndOffsets.get(42L));
        } finally {
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /**
     * An upload failure releases the deferred link record without starting its decode or advancing its stream offset.
     */
    @Test
    public void testRecoverReleasesPendingRecordOnUploadFailure() {
        StreamRecordBatch pending = newLinkRecord(42, 1);
        ByteBuf pendingBuffer = pending.encoded();
        List<RecoverResult> records = List.of(
            new TestRecoverResult(newLinkRecord(42, 0)), new TestRecoverResult(pending));
        LinkRecordDecoder decoder = largeRecordDecoder(600_000_000);
        RuntimeException failure = new RuntimeException("upload failed");
        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 0L));
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            assertSame(failure, assertThrows(RuntimeException.class, () ->
                WALRecovery.recover(records.iterator(), streamEndOffsets, Long.MAX_VALUE, LOGGER, block -> {
                    WALRecovery.releaseAllRecords(block.records().values());
                    throw failure;
                })));
            assertEquals(0, pendingBuffer.refCnt());
            assertEquals(1L, streamEndOffsets.get(42L));
        }
    }

    /**
     * Closed, committed, and discontinuous link records are discarded before consulting the decoder.
     */
    @Test
    public void testRecoverFiltersLinkRecordsBeforeSizing() {
        List<RecoverResult> records = List.of(
            new TestRecoverResult(newLinkRecord(43, 0)),
            new TestRecoverResult(newLinkRecord(42, 0)),
            new TestRecoverResult(newLinkRecord(42, 2)));
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(records.iterator(), new HashMap<>(Map.of(42L, 1L)), 6000, LOGGER,
                block -> assertTrue(block.records().isEmpty()));
            verify(decoder, never()).decodedSize(any(ByteBuf.class));
            verify(decoder, never()).decode(any(), any());
        }
    }

    /**
     * Recovery caps its soft block budget at INT_MAX and accounts for the last accepted record without int overflow.
     */
    @Test
    public void testRecoverCapsBlockBudgetWithLongAccounting() {
        int decodedSize = (int) Constants.MAX_UPLOAD_STREAM_SIZE;
        List<RecoverResult> records = List.of(
            new TestRecoverResult(newLinkRecord(42, 0), DefaultRecordOffset.of(0, 100, 0)),
            new TestRecoverResult(newLinkRecord(43, 0), DefaultRecordOffset.of(0, 200, 0)),
            new TestRecoverResult(newLinkRecord(44, 0), DefaultRecordOffset.of(0, 300, 0)));
        Map<Long, Long> endOffsets = new HashMap<>(Map.of(42L, 0L, 43L, 0L, 44L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        LinkRecordDecoder decoder = largeRecordDecoder(decodedSize);
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(records.iterator(), endOffsets, Long.MAX_VALUE, LOGGER, blocks::add);
            assertEquals(2, blocks.size());
            assertEquals(2L * (decodedSize + 1024L), blocks.get(0).size());
            assertEquals(decodedSize + 1024L, blocks.get(1).size());
            assertEquals(DefaultRecordOffset.of(0, 200, 0), blocks.get(0).lastRecordOffset());
            assertEquals(DefaultRecordOffset.of(0, 300, 0), blocks.get(1).lastRecordOffset());
            assertEquals(Map.of(42L, 1L, 43L, 1L, 44L, 1L), endOffsets);
        } finally {
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /**
     * A link whose decoded payload cannot fit in any upload batch fails before decoding and releases its source.
     */
    @Test
    public void testRecoverRejectsOversizedLinkBeforeDecoding() {
        StreamRecordBatch record = newLinkRecord(42, 0);
        ByteBuf buffer = record.encoded();
        LinkRecordDecoder decoder = largeRecordDecoder((int) Constants.MAX_UPLOAD_STREAM_SIZE + 1);
        Map<Long, Long> endOffsets = new HashMap<>(Map.of(42L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            assertThrows(IllegalArgumentException.class, () -> WALRecovery.recover(
                List.<RecoverResult>of(new TestRecoverResult(record)).iterator(), endOffsets, Long.MAX_VALUE, LOGGER, blocks::add));
            assertEquals(0, buffer.refCnt());
            assertEquals(0L, endOffsets.get(42L));
            assertTrue(blocks.isEmpty());
            verify(decoder, never()).decode(any(), any());
        }
    }

    private static StreamRecordBatch newLinkRecord(long streamId, long offset) {
        return StreamRecordBatch.of(streamId, 0, offset, -1, random(1), DefaultByteBufSupplier.INSTANCE);
    }

    private static LinkRecordDecoder largeRecordDecoder(int size) {
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(size);
        when(decoder.decode(any(), any())).thenAnswer(invocation -> {
            StreamRecordBatch src = invocation.getArgument(0);
            StreamRecordBatch decoded = mock(StreamRecordBatch.class);
            when(decoded.getBaseOffset()).thenReturn(src.getBaseOffset());
            when(decoded.size()).thenReturn(size);
            src.release();
            return CompletableFuture.completedFuture(decoded);
        });
        return decoder;
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
