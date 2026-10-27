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
import java.util.Iterator;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
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
        assertTrue(results.isEmpty());
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
        // With maxCacheSize=2050, exactly 2 records fill one returned batch and block.
        List<RecoverResult> recoverResults = List.of(
            new TestRecoverResult(newRecord(42L, 10L)),
            new TestRecoverResult(newRecord(42L, 11L)),
            new TestRecoverResult(newRecord(42L, 12L)),
            new TestRecoverResult(newRecord(42L, 13L)),
            new TestRecoverResult(newRecord(42L, 14L))
        );

        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 10L));
        List<LogCache.LogCacheBlock> results = new ArrayList<>();
        WALRecovery.recover(recoverResults.iterator(), streamEndOffsets, 2_050L, LOGGER, results::add);
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
            return CompletableFuture.completedFuture(decoded);
        });
        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(records.iterator(), streamEndOffsets, 6000, LOGGER, blocks::add);
            assertEquals(5, blocks.size());
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L), blocks.stream()
                .flatMap(block -> block.records().get(42L).stream()).map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList()));
            for (int i = 0; i < blocks.size(); i++) {
                assertEquals(4096L + 1024, blocks.get(i).size());
                assertEquals(DefaultRecordOffset.of(0, i, 0), blocks.get(i).lastRecordOffset());
            }
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
     * An upload failure releases both the pending decoded record and the iterator's remaining prefetched results.
     */
    @Test
    public void testRecoverReleasesPendingRecordOnUploadFailure() {
        StreamRecordBatch pending = newLinkRecord(42, 1);
        ByteBuf pendingBuffer = pending.encoded();
        StreamRecordBatch prefetched = newLinkRecord(42, 2);
        ByteBuf prefetchedBuffer = prefetched.encoded();
        List<RecoverResult> records = List.of(
            new TestRecoverResult(newLinkRecord(42, 0)), new TestRecoverResult(pending), new TestRecoverResult(prefetched));
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(600_000_000);
        List<StreamRecordBatch> decodedRecords = new ArrayList<>();
        when(decoder.decode(any(), any())).thenAnswer(invocation -> {
            StreamRecordBatch src = invocation.getArgument(0);
            StreamRecordBatch result = mock(StreamRecordBatch.class);
            when(result.getStreamId()).thenReturn(src.getStreamId());
            when(result.getBaseOffset()).thenReturn(src.getBaseOffset());
            when(result.getLastOffset()).thenReturn(src.getLastOffset());
            when(result.size()).thenReturn(600_000_000);
            decodedRecords.add(result);
            return CompletableFuture.completedFuture(result);
        });
        RuntimeException failure = new RuntimeException("upload failed");
        Map<Long, Long> streamEndOffsets = new HashMap<>(Map.of(42L, 0L));
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            assertSame(failure, assertThrows(RuntimeException.class, () ->
                WALRecovery.recover(records.iterator(), streamEndOffsets, Long.MAX_VALUE, LOGGER, block -> {
                    WALRecovery.releaseAllRecords(block.records().values());
                    assertEquals(0, pendingBuffer.refCnt());
                    assertEquals(0, prefetchedBuffer.refCnt());
                    throw failure;
                })));
            assertEquals(0, pendingBuffer.refCnt());
            assertEquals(0, prefetchedBuffer.refCnt());
            assertEquals(3, decodedRecords.size());
            decodedRecords.forEach(record -> verify(record).release());
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
     * Recovery caps returned batches at INT_MAX without overflowing long size accounting.
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
            assertEquals(3, blocks.size());
            for (int i = 0; i < blocks.size(); i++) {
                assertEquals(decodedSize + 1024L, blocks.get(i).size());
                assertEquals(DefaultRecordOffset.of(0, (i + 1) * 100, 0), blocks.get(i).lastRecordOffset());
            }
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

    /** A decode failure truncates the whole WAL, including later records of otherwise healthy streams. */
    @Test
    public void testDecodeFailureRecoversGlobalPrefix() {
        StreamRecordBatch first = newLinkRecord(42, 0);
        StreamRecordBatch decodedFirst = newRecord(42, 0);
        StreamRecordBatch failed = newLinkRecord(43, 0);
        StreamRecordBatch laterSameStream = newRecord(42, 1);
        StreamRecordBatch laterOtherStream = newLinkRecord(44, 0);
        List<RecoverResult> records = List.of(
            new TestRecoverResult(first, DefaultRecordOffset.of(0, 100, 0)),
            new TestRecoverResult(failed, DefaultRecordOffset.of(0, 200, 0)),
            new TestRecoverResult(laterSameStream, DefaultRecordOffset.of(0, 300, 0)),
            new TestRecoverResult(laterOtherStream, DefaultRecordOffset.of(0, 400, 0)));
        List<ByteBuf> sources = records.stream().map(r -> r.record().encoded()).collect(Collectors.toList());
        StreamRecordBatch decodedSuffix = newRecord(44, 0);
        ByteBuf suffixBuffer = decodedSuffix.encoded();
        RuntimeException failure = new RuntimeException("missing RouterChannel object");
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(1);
        when(decoder.decode(any(), any())).thenAnswer(invocation -> {
            if (invocation.getArgument(0) == first) {
                return CompletableFuture.completedFuture(decodedFirst);
            }
            if (invocation.getArgument(0) == failed) {
                return CompletableFuture.failedFuture(failure);
            }
            return CompletableFuture.completedFuture(decodedSuffix);
        });
        Map<Long, Long> ends = new HashMap<>(Map.of(42L, 0L, 43L, 0L, 44L, 0L));
        List<LogCache.LogCacheBlock> recovered = new ArrayList<>();
        Logger logger = mock(Logger.class);
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(records.iterator(), ends, 100_000, logger, recovered::add);
            assertEquals(1, recovered.size());
            assertEquals(Map.of(42L, List.of(decodedFirst)), recovered.get(0).records());
            assertEquals(DefaultRecordOffset.of(0, 100, 0), recovered.get(0).lastRecordOffset());
            assertEquals(Map.of(42L, 1L, 43L, 0L, 44L, 0L), ends);
            assertEquals(0, suffixBuffer.refCnt());
            verify(logger).error(anyString(), eq(DefaultRecordOffset.of(0, 200, 0)), eq(43L), eq(0L), same(failure));
        } finally {
            recovered.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
        sources.forEach(buf -> assertEquals(0, buf.refCnt()));
    }

    /** A failed block releases its deferred record without advancing the stream past the retained prefix. */
    @Test
    public void testDecodeFailureReleasesDeferredRecord() {
        StreamRecordBatch first = newRecord(42, 0);
        StreamRecordBatch second = newRecord(42, 1);
        StreamRecordBatch failed = newLinkRecord(42, 2);
        StreamRecordBatch deferred = newLinkRecord(42, 3);
        ByteBuf failedBuffer = failed.encoded();
        ByteBuf deferredBuffer = deferred.encoded();
        List<RecoverResult> records = List.of(new TestRecoverResult(first), new TestRecoverResult(second),
            new TestRecoverResult(failed), new TestRecoverResult(deferred));
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(600_000_000);
        when(decoder.decode(any(), any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("missing")));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        Map<Long, Long> ends = new HashMap<>(Map.of(42L, 0L));
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(records.iterator(), ends, Long.MAX_VALUE, LOGGER, blocks::add);
            assertEquals(List.of(first, second), blocks.get(0).records().get(42L));
            assertEquals(2L, ends.get(42L));
            assertEquals(0, failedBuffer.refCnt());
            assertEquals(0, deferredBuffer.refCnt());
            verify(decoder).decode(same(deferred), any());
        } finally {
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /** Concurrent decode failures choose the earliest WAL record, regardless of completion order. */
    @Test
    public void testDecodeFailureUsesWalOrder() {
        StreamRecordBatch first = newRecord(42, 0);
        StreamRecordBatch failed = newLinkRecord(43, 0);
        StreamRecordBatch later = newLinkRecord(42, 1);
        StreamRecordBatch last = newLinkRecord(44, 0);
        CompletableFuture<StreamRecordBatch> firstFailure = new CompletableFuture<>();
        RuntimeException failure = new RuntimeException("earliest WAL failure");
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(1);
        when(decoder.decode(any(), any())).thenAnswer(invocation -> {
            StreamRecordBatch src = invocation.getArgument(0);
            if (src == failed) {
                return firstFailure;
            }
            if (src == last) {
                firstFailure.completeExceptionally(failure);
            }
            return CompletableFuture.failedFuture(new IllegalStateException("later WAL failure"));
        });
        Map<Long, Long> ends = new HashMap<>(Map.of(42L, 0L, 43L, 0L, 44L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        Logger logger = mock(Logger.class);
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(List.<RecoverResult>of(new TestRecoverResult(first),
                new TestRecoverResult(failed, DefaultRecordOffset.of(0, 200, 0)),
                new TestRecoverResult(later), new TestRecoverResult(last)).iterator(), ends, 100_000, logger, blocks::add);
            assertEquals(Map.of(42L, List.of(first)), blocks.get(0).records());
            assertEquals(Map.of(42L, 1L, 43L, 0L, 44L, 0L), ends);
            verify(logger).error(anyString(), eq(DefaultRecordOffset.of(0, 200, 0)), eq(43L), eq(0L), same(failure));
        } finally {
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /** Recovery keeps earlier blocks and stops reading later WAL blocks after a decode failure. */
    @Test
    public void testDecodeFailureStopsAfterEarlierBlocks() {
        StreamRecordBatch first = newRecord(42, 0);
        StreamRecordBatch second = newRecord(42, 1);
        StreamRecordBatch failed = newLinkRecord(42, 2);
        StreamRecordBatch suffix = newRecord(43, 0);
        StreamRecordBatch unread = newRecord(43, 1);
        var iterator = List.<RecoverResult>of(new TestRecoverResult(first), new TestRecoverResult(second),
            new TestRecoverResult(failed), new TestRecoverResult(suffix), new TestRecoverResult(unread)).iterator();
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(1);
        when(decoder.decode(any(), any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("missing")));
        Map<Long, Long> ends = new HashMap<>(Map.of(42L, 0L, 43L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(iterator, ends, 2050, LOGGER, blocks::add);
            assertEquals(List.of(first, second), blocks.get(0).records().get(42L));
            assertEquals(1, blocks.size());
            assertEquals(Map.of(42L, 2L, 43L, 0L), ends);
            assertSame(unread, iterator.next().record());
        } finally {
            unread.release();
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /** Malformed link metadata must truncate at that record even when sizing fails before decoding. */
    @Test
    public void testSizingFailureRecoversPrefix() {
        StreamRecordBatch first = newRecord(42, 0);
        StreamRecordBatch failed = newLinkRecord(43, 0);
        StreamRecordBatch suffix = newRecord(42, 1);
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        RuntimeException failure = new IllegalArgumentException("invalid link header");
        when(decoder.decodedSize(any())).thenThrow(failure);
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        Map<Long, Long> ends = new HashMap<>(Map.of(42L, 0L, 43L, 0L));
        Logger logger = mock(Logger.class);
        Iterator<RecoverResult> iterator = List.<RecoverResult>of(new TestRecoverResult(first),
            new TestRecoverResult(failed, DefaultRecordOffset.of(0, 200, 0)), new TestRecoverResult(suffix)).iterator();
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(iterator, ends, 100_000, logger, blocks::add);
            assertSame(suffix, iterator.next().record());
            assertEquals(Map.of(42L, List.of(first)), blocks.get(0).records());
            assertEquals(Map.of(42L, 1L, 43L, 0L), ends);
            verify(decoder, never()).decode(any(), any());
            verify(logger).error(anyString(), eq(DefaultRecordOffset.of(0, 200, 0)), eq(43L), eq(0L), same(failure));
        } finally {
            suffix.release();
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /** A sizing failure stops prefetch, but an earlier decode failure still determines the WAL cutoff. */
    @Test
    public void testEarlierDecodeFailurePrecedesSizingFailure() {
        StreamRecordBatch first = newRecord(42, 0);
        StreamRecordBatch failed = newLinkRecord(43, 0);
        StreamRecordBatch malformed = newLinkRecord(42, 1);
        ByteBuf failedBuffer = failed.encoded();
        ByteBuf malformedBuffer = malformed.encoded();
        RuntimeException failure = new IllegalStateException("earlier decode failure");
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(1).thenThrow(new IllegalArgumentException("invalid header"));
        when(decoder.decode(any(), any())).thenReturn(CompletableFuture.failedFuture(failure));
        Map<Long, Long> ends = new HashMap<>(Map.of(42L, 0L, 43L, 0L));
        List<LogCache.LogCacheBlock> blocks = new ArrayList<>();
        Logger logger = mock(Logger.class);
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(List.<RecoverResult>of(new TestRecoverResult(first),
                new TestRecoverResult(failed, DefaultRecordOffset.of(0, 200, 0)),
                new TestRecoverResult(malformed, DefaultRecordOffset.of(0, 300, 0))).iterator(),
                ends, 100_000, logger, blocks::add);
            assertEquals(Map.of(42L, List.of(first)), blocks.get(0).records());
            assertEquals(Map.of(42L, 1L, 43L, 0L), ends);
            assertEquals(0, failedBuffer.refCnt());
            assertEquals(0, malformedBuffer.refCnt());
            verify(decoder, never()).decode(same(malformed), any());
            verify(logger).error(anyString(), eq(DefaultRecordOffset.of(0, 200, 0)), eq(43L), eq(0L), same(failure));
        } finally {
            blocks.forEach(block -> WALRecovery.releaseAllRecords(block.records().values()));
        }
    }

    /** A full block must not prefetch a suffix record that would be abandoned if this block is truncated. */
    @Test
    public void testDecodeFailureDoesNotLeakPrefetchedSuffix() {
        List<ByteBuf> allocated = new ArrayList<>();
        Iterator<RecoverResult> iterator = new Iterator<>() {
            private RecoverResult next;
            private int position;

            @Override
            public boolean hasNext() {
                if (next == null && position < 2) {
                    StreamRecordBatch record = position == 0 ? newLinkRecord(42, 0) : newRecord(42, 1);
                    allocated.add(record.encoded());
                    next = new TestRecoverResult(record);
                    position++;
                }
                return next != null;
            }

            @Override
            public RecoverResult next() {
                RecoverResult result = next;
                next = null;
                return result;
            }
        };
        LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
        when(decoder.decodedSize(any())).thenReturn(1);
        when(decoder.decode(any(), any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("missing")));
        try (MockedStatic<S3Storage> storage = mockStatic(S3Storage.class)) {
            storage.when(S3Storage::getLinkRecordDecoder).thenReturn(decoder);
            WALRecovery.recover(iterator, new HashMap<>(Map.of(42L, 0L)), 1, LOGGER,
                block -> assertTrue(block.records().isEmpty()));
        }
        assertEquals(1, allocated.size());
        assertEquals(0, allocated.get(0).refCnt());
    }

    /** Upload even a partial batch before replenishing prefetch; permit a single record above the soft budget. */
    @ParameterizedTest
    @ValueSource(ints = {1, 2000})
    public void testBatchUploadedBeforeNextPrefetch(int maxSize) {
        List<ByteBuf> allocated = new ArrayList<>();
        Iterator<RecoverResult> iterator = new Iterator<>() {
            private int position;

            @Override
            public boolean hasNext() {
                return position < 3;
            }

            @Override
            public RecoverResult next() {
                if (position == 2) {
                    // The first batch had one retained and one filtered record, so its block was not full.
                    allocated.forEach(buf -> assertEquals(0, buf.refCnt()));
                }
                StreamRecordBatch record = newRecord(position == 1 ? 43 : 42, position == 2 ? 1 : 0);
                allocated.add(record.encoded());
                return new TestRecoverResult(record, DefaultRecordOffset.of(0, ++position, 0));
            }
        };
        List<Long> uploadedOffsets = new ArrayList<>();
        Map<Long, Long> ends = new HashMap<>(Map.of(42L, 0L));
        WALRecovery.recover(iterator, ends, maxSize, LOGGER, block -> {
            try {
                assertEquals(1025, block.size());
                assertEquals(1, block.records().get(42L).size());
                uploadedOffsets.add(block.records().get(42L).get(0).getBaseOffset());
            } finally {
                WALRecovery.releaseAllRecords(block.records().values());
            }
        });
        assertEquals(List.of(0L, 1L), uploadedOffsets);
        assertEquals(2L, ends.get(42L));
        allocated.forEach(buf -> assertEquals(0, buf.refCnt()));
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
            when(decoded.getStreamId()).thenReturn(src.getStreamId());
            when(decoded.getBaseOffset()).thenReturn(src.getBaseOffset());
            when(decoded.getLastOffset()).thenReturn(src.getLastOffset());
            when(decoded.size()).thenReturn(size);
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
