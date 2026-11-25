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

import com.automq.stream.s3.Constants;
import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.LogCache.LogCacheBlock;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class LogCacheTest {

    /** Complete RAM coverage and disabled backends return immediately without disk work. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAsyncRamHit(boolean enabled) throws Exception {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        LogCache cache = new LogCache(1 << 20, 1 << 20, enabled ? diskCache : null);
        StreamRecordBatch record = sizedRecord(42, 0, 6);
        assertTrue(cache.put(record));
        assertEquals(List.of(record), cache.getAsync(TraceContext.DEFAULT, 42, 0, 1, 100).get(5, TimeUnit.SECONDS));
        verify(record).retain();
        verifyNoInteractions(diskCache);
        record.release();
    }

    /** Disk and RAM form coverage from the requested start; the byte budget starts with disk. */
    @ParameterizedTest
    @CsvSource({"1,100,3", "1,7,2", "1,6,1"})
    public void testAsyncMixedRead(long startOffset, int maxBytes, int count) throws Exception {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        LogCache cache = new LogCache(1 << 20, 1 << 20, diskCache);
        StreamRecordBatch disk = sizedRecord(42, 1, 6);
        StreamRecordBatch first = sizedRecord(42, 2, 6);
        StreamRecordBatch second = sizedRecord(42, 3, 6);
        assertTrue(cache.put(first));
        assertTrue(cache.put(second));
        when(diskCache.get(42, startOffset, 2, maxBytes)).thenReturn(CompletableFuture.completedFuture(List.of(disk)));

        List<StreamRecordBatch> records = cache.getAsync(TraceContext.DEFAULT, 42, startOffset, 4, maxBytes)
            .get(5, TimeUnit.SECONDS);
        assertEquals(List.of(disk, first, second).subList(0, count), records);
        verify(first, count == 1 ? times(1) : never()).release();
        verify(second, count == 2 ? times(1) : never()).release();
        records.forEach(StreamRecordBatch::release);
    }

    /** Disk-only coverage is returned directly; a suffix missing the requested start is released. */
    @Test
    public void testAsyncDiskOnlyRead() throws Exception {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        LogCache cache = new LogCache(1 << 20, 1 << 20, diskCache);
        StreamRecordBatch disk = sizedRecord(42, 0, 6);
        when(diskCache.get(42, 0, 1, 100)).thenReturn(CompletableFuture.completedFuture(List.of(disk)));
        assertEquals(List.of(disk), cache.getAsync(TraceContext.DEFAULT, 42, 0, 1, 100).get(5, TimeUnit.SECONDS));
        verify(disk, never()).release();
        disk.release();

        StreamRecordBatch suffix = sizedRecord(42, 1, 6);
        when(diskCache.get(42, 0, 2, 100)).thenReturn(CompletableFuture.completedFuture(List.of(suffix)));
        assertTrue(cache.getAsync(TraceContext.DEFAULT, 42, 0, 2, 100).get(5, TimeUnit.SECONDS).isEmpty());
        verify(suffix).release();
    }

    /** Misses, submission failures and overlapping boundaries retain the original RAM suffix. */
    @ParameterizedTest
    @ValueSource(strings = {"miss", "failed", "submission-failed", "overlap"})
    public void testAsyncDiskFallback(String outcome) throws Exception {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        LogCache cache = new LogCache(1 << 20, 1 << 20, diskCache);
        StreamRecordBatch ram = sizedRecord(42, 1, 6);
        when(ram.getLastOffset()).thenReturn(3L);
        StreamRecordBatch disk = sizedRecord(42, 0, 6);
        when(disk.getLastOffset()).thenReturn(2L);
        assertTrue(cache.put(ram));
        if ("submission-failed".equals(outcome)) {
            doThrow(new IllegalStateException("submission failed")).when(diskCache).get(42, 0, 1, 100);
        } else {
            CompletableFuture<List<StreamRecordBatch>> read = "failed".equals(outcome)
                ? CompletableFuture.failedFuture(new IllegalStateException("read failed"))
                : CompletableFuture.completedFuture("overlap".equals(outcome) ? List.of(disk) : List.of());
            when(diskCache.get(42, 0, 1, 100)).thenReturn(read);
        }
        assertEquals(List.of(ram), cache.getAsync(TraceContext.DEFAULT, 42, 0, 3, 100).get(5, TimeUnit.SECONDS));
        verify(ram, never()).release();
        if ("overlap".equals(outcome)) {
            verify(disk).release();
        }
        ram.release();
    }

    /** Cancelling a pending read releases retained RAM and any disk result that arrives later. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAsyncReadCancellation(boolean cancellable) {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        LogCache cache = new LogCache(1 << 20, 1 << 20, diskCache);
        StreamRecordBatch ram = sizedRecord(42, 1, 6);
        StreamRecordBatch disk = sizedRecord(42, 0, 6);
        CompletableFuture<List<StreamRecordBatch>> diskRead = spy(new CompletableFuture<>());
        if (!cancellable) {
            doReturn(false).when(diskRead).cancel(false);
        }
        when(diskCache.get(42, 0, 1, 100)).thenReturn(diskRead);
        assertTrue(cache.put(ram));
        CompletableFuture<List<StreamRecordBatch>> read = cache.getAsync(TraceContext.DEFAULT, 42, 0, 2, 100);
        assertFalse(read.isDone());
        verify(ram).retain();
        assertTrue(read.cancel(false));
        assertEquals(cancellable, diskRead.isCancelled());
        if (!cancellable) {
            assertTrue(diskRead.complete(List.of(disk)));
            verify(disk).release();
        }
        verify(ram).release();
    }

    /** Only disk coverage from the requested start counts as a hit; suffixes and failures are misses. */
    @ParameterizedTest
    @ValueSource(strings = {"hit", "miss", "error", "submission", "overlap", "suffix", "cancel"})
    public void testDiskReadMetrics(String outcome) throws Exception {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        LogCache cache = new LogCache(1 << 20, 1 << 20, diskCache);
        StreamRecordBatch ram = sizedRecord(42, 2, 1);
        assertTrue(cache.put(ram));
        CompletableFuture<List<StreamRecordBatch>> diskRead = new CompletableFuture<>();
        if ("submission".equals(outcome)) {
            doThrow(new IllegalStateException()).when(diskCache).get(42, 0, 2, 100);
        } else {
            when(diskCache.get(42, 0, 2, 100)).thenReturn(diskRead);
        }
        long hits = SnapshotReadCacheMetrics.DISK_HIT_LATENCY.cumulativeCount();
        long misses = SnapshotReadCacheMetrics.DISK_MISS_LATENCY.cumulativeCount();
        long totalNanos = SnapshotReadCacheMetrics.DISK_HIT_LATENCY.cumulativeSum()
            + SnapshotReadCacheMetrics.DISK_MISS_LATENCY.cumulativeSum();
        CompletableFuture<List<StreamRecordBatch>> read = cache.getAsync(TraceContext.DEFAULT, 42, 0, 3, 100);
        if ("cancel".equals(outcome)) {
            read.cancel(false);
        } else {
            if ("error".equals(outcome)) {
                diskRead.completeExceptionally(new IllegalStateException());
            } else if ("hit".equals(outcome)) {
                diskRead.complete(List.of(sizedRecord(42, 0, 1), sizedRecord(42, 1, 1)));
            } else if ("overlap".equals(outcome)) {
                diskRead.complete(List.of(sizedRecord(42, 0, 1)));
            } else if ("suffix".equals(outcome)) {
                StreamRecordBatch suffix = sizedRecord(42, 1, 1);
                diskRead.complete(List.of(suffix));
                verify(suffix).release();
            } else {
                diskRead.complete(List.of());
            }
            read.get(5, TimeUnit.SECONDS).forEach(StreamRecordBatch::release);
        }
        assertEquals(hits + ("hit".equals(outcome) ? 1 : 0),
            SnapshotReadCacheMetrics.DISK_HIT_LATENCY.cumulativeCount());
        assertEquals(misses + (List.of("miss", "error", "submission", "overlap", "suffix").contains(outcome) ? 1 : 0),
            SnapshotReadCacheMetrics.DISK_MISS_LATENCY.cumulativeCount());
        if (!"cancel".equals(outcome)) {
            assertTrue(SnapshotReadCacheMetrics.DISK_HIT_LATENCY.cumulativeSum()
                + SnapshotReadCacheMetrics.DISK_MISS_LATENCY.cumulativeSum() > totalNanos);
        }
        ram.release();
    }

    /** A disk deadline returns the original RAM suffix and frees late disk data without double release. */
    @Test
    public void testDiskReadDeadline() throws Exception {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        LogCache cache = new LogCache(1 << 20, 1 << 20, diskCache, 10);
        StreamRecordBatch ram = sizedRecord(42, 1, 6);
        StreamRecordBatch disk = sizedRecord(42, 0, 6);
        CompletableFuture<List<StreamRecordBatch>> diskRead = spy(new CompletableFuture<>());
        doReturn(false).when(diskRead).cancel(false);
        when(diskCache.get(42, 0, 1, 100)).thenReturn(diskRead);
        assertTrue(cache.put(ram));
        List<StreamRecordBatch> records = cache.getAsync(TraceContext.DEFAULT, 42, 0, 2, 100)
            .get(5, TimeUnit.SECONDS);
        assertEquals(List.of(ram), records);
        verify(diskRead, org.mockito.Mockito.timeout(5000).atLeastOnce()).cancel(false);
        diskRead.complete(List.of(disk));
        verify(disk).release();
        verify(ram, never()).release();
        records.forEach(StreamRecordBatch::release);
        verify(ram).release();
    }

    /** Supplying a backend enables spilling without delaying record release or replacing the free listener. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOptionalDiskSpill(boolean enabled) throws Exception {
        AsyncStreamRecordBatchDiskCache diskCache = mock(AsyncStreamRecordBatchDiskCache.class);
        StreamRecordBatch record = sizedRecord(42, 0, 1);
        CompletableFuture<Boolean> pendingWrite = new CompletableFuture<>();
        when(diskCache.put(record)).thenReturn(pendingWrite);
        CompletableFuture<LogCacheBlock> notification = new CompletableFuture<>();
        LogCache cache = enabled
            ? new LogCache(0, 1024, 10000, notification::complete, diskCache)
            : new LogCache(0, 1024, 10000, notification::complete);
        assertTrue(cache.put(record));
        LogCacheBlock block = cache.archiveCurrentBlock();
        cache.markFree(block).get(5, TimeUnit.SECONDS);

        assertSame(block, notification.get(5, TimeUnit.SECONDS));
        verify(record).release();
        assertTrue(block.records().isEmpty());
        assertFalse(pendingWrite.isDone());
        if (enabled) {
            verify(diskCache).put(record);
        } else {
            verifyNoInteractions(diskCache);
        }
    }

    /**
     * Each stream may reach the upload limit independently; the next record stays owned by the caller for retry.
     */
    @Test
    public void testStreamPayloadLimitAndRetry() {
        int halfLimit = (int) (Constants.MAX_UPLOAD_STREAM_SIZE / 2);
        LogCacheBlock block = new LogCacheBlock(Long.MAX_VALUE);
        StreamRecordBatch first = sizedRecord(42, 0, halfLimit);
        StreamRecordBatch second = sizedRecord(42, 1, halfLimit);
        StreamRecordBatch deferred = sizedRecord(42, 2, 1);
        assertTrue(block.put(first));
        assertTrue(block.put(second));
        assertTrue(block.put(sizedRecord(43, 0, (int) Constants.MAX_UPLOAD_STREAM_SIZE)));
        assertTrue(block.size() > Integer.MAX_VALUE);
        assertFalse(block.put(deferred));
        assertEquals(List.of(first, second), block.records().get(42L));
        assertEquals(2L, block.map.get(42L).endOffset());
        assertEquals(Constants.MAX_UPLOAD_STREAM_SIZE, block.map.get(42L).payloadBytes);
        verify(deferred, never()).release();

        LogCacheBlock next = new LogCacheBlock(Long.MAX_VALUE);
        assertTrue(next.put(deferred));
        assertEquals(List.of(deferred), next.records().get(42L));
    }

    /**
     * A conservative decoded size controls splitting even when the encoded link itself is tiny.
     */
    @Test
    public void testExplicitPayloadSizeControlsStreamLimit() {
        LogCacheBlock block = new LogCacheBlock(Long.MAX_VALUE);
        StreamRecordBatch first = sizedRecord(42, 0, 1);
        StreamRecordBatch next = sizedRecord(42, 1, 1);
        assertTrue(block.put(first, (int) Constants.MAX_UPLOAD_STREAM_SIZE));
        assertFalse(block.put(next, 1));
        assertEquals(Constants.MAX_UPLOAD_STREAM_SIZE + 1024, block.size());
        assertEquals(List.of(first), block.records().get(42L));
    }

    /**
     * An indivisible record above the hard limit is rejected without mutating or poisoning an empty block.
     */
    @Test
    public void testOversizedRecordDoesNotMutateBlock() {
        LogCacheBlock block = new LogCacheBlock(Long.MAX_VALUE);
        StreamRecordBatch oversized = sizedRecord(42, 0, (int) Constants.MAX_UPLOAD_STREAM_SIZE + 1);
        assertThrows(IllegalArgumentException.class, () -> block.put(oversized));
        assertTrue(block.records().isEmpty());
        assertEquals(0L, block.size());
        assertFalse(block.isFull());
        verify(oversized, never()).release();
        assertTrue(block.put(sizedRecord(42, 0, 1)));
    }

    /**
     * Copying and merging cached streams preserves the conservative payload accounting used for later insertions.
     */
    @Test
    public void testMergePreservesPayloadAccounting() {
        int halfLimit = (int) (Constants.MAX_UPLOAD_STREAM_SIZE / 2);
        LogCacheBlock first = new LogCacheBlock(Long.MAX_VALUE);
        LogCacheBlock second = new LogCacheBlock(Long.MAX_VALUE);
        LogCacheBlock merged = new LogCacheBlock(Long.MAX_VALUE);
        first.put(sizedRecord(42, 0, 1), halfLimit);
        second.put(sizedRecord(42, 1, 1), halfLimit);
        LogCache.mergeBlock(merged, first);
        LogCache.mergeBlock(merged, second);
        assertEquals(Constants.MAX_UPLOAD_STREAM_SIZE, merged.map.get(42L).payloadBytes);
        assertEquals(halfLimit, first.map.get(42L).payloadBytes);
        assertFalse(merged.put(sizedRecord(42, 2, 1)));
    }

    /**
     * A merged block exceeding int bytes must exhaust the soft read budget without reading the next block.
     * Record sizes are mocked and cache capacities are thresholds only; no GiB-sized payloads are allocated.
     */
    @Test
    public void testReadBudgetStopsAfterMergedBlockExceedsIntSize() {
        LogCache cache = new LogCache(8L << 30, 4L << 30);
        StreamRecordBatch first = sizedRecord(42, 0, (int) Constants.MAX_UPLOAD_STREAM_SIZE);
        StreamRecordBatch second = sizedRecord(42, 1, (int) Constants.MAX_UPLOAD_STREAM_SIZE);
        StreamRecordBatch next = sizedRecord(42, 2, 1);
        assertTrue(cache.put(first));
        LogCacheBlock archived = cache.archiveCurrentBlock();
        LogCacheBlock other = new LogCacheBlock(Long.MAX_VALUE);
        assertTrue(other.put(second));
        LogCache.mergeBlock(archived, other);
        assertTrue(cache.put(next));

        assertEquals(List.of(first, second), cache.get0(42L, 0, 3, Integer.MAX_VALUE));
        assertEquals(List.of(first), cache.get0(42L, 0, 3, (int) Constants.MAX_UPLOAD_STREAM_SIZE - 1));
    }

    /**
     * A soft budget includes the complete final record even across blocks, then stops before the next record.
     */
    @Test
    public void testReadBudgetAllowsFinalRecordToExceedHint() {
        LogCache cache = new LogCache(1024 * 1024, 1024 * 1024);
        StreamRecordBatch first = sizedRecord(42, 0, 80);
        StreamRecordBatch second = sizedRecord(42, 1, 80);
        StreamRecordBatch next = sizedRecord(42, 2, 1);
        assertTrue(cache.put(first));
        cache.archiveCurrentBlock();
        assertTrue(cache.put(second));
        assertTrue(cache.put(next));

        assertEquals(List.of(first, second), cache.get0(42L, 0, 3, 100));
        assertEquals(List.of(first), cache.get0(42L, 0, 3, 80));
        assertEquals(List.of(first), cache.get0(42L, 0, 3, 1));
    }

    private static StreamRecordBatch sizedRecord(long streamId, long offset, int size) {
        StreamRecordBatch record = mock(StreamRecordBatch.class);
        when(record.getStreamId()).thenReturn(streamId);
        when(record.getBaseOffset()).thenReturn(offset);
        when(record.getLastOffset()).thenReturn(offset + 1);
        when(record.size()).thenReturn(size);
        return record;
    }

    @Test
    public void testPutGet() {
        LogCache logCache = new LogCache(1024 * 1024, 1024 * 1024);

        logCache.put(StreamRecordBatch.of(233L, 0L, 10L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        logCache.put(StreamRecordBatch.of(233L, 0L, 11L, 2, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        logCache.archiveCurrentBlock();
        logCache.put(StreamRecordBatch.of(233L, 0L, 13L, 2, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        logCache.archiveCurrentBlock();
        logCache.put(StreamRecordBatch.of(233L, 0L, 20L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        logCache.put(StreamRecordBatch.of(233L, 0L, 21L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

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
            cache.put(StreamRecordBatch.of(233L, 0L, i, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
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

        logCache.put(StreamRecordBatch.of(233L, 0L, 10L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        logCache.put(StreamRecordBatch.of(233L, 0L, 11L, 2, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        logCache.archiveCurrentBlock();
        logCache.put(StreamRecordBatch.of(233L, 0L, 13L, 2, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        logCache.put(StreamRecordBatch.of(234L, 0L, 13L, 2, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

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
        left.put(StreamRecordBatch.of(233L, 0L, 10L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        LogCacheBlock right = new LogCacheBlock(1024L * 1024);
        right.put(StreamRecordBatch.of(233L, 0L, 13L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        assertTrue(LogCache.isDiscontinuous(left, right));

        left = new LogCacheBlock(1024L * 1024);
        left.put(StreamRecordBatch.of(233L, 0L, 10L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        left.put(StreamRecordBatch.of(234L, 0L, 10L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        right = new LogCacheBlock(1024L * 1024);
        right.put(StreamRecordBatch.of(233L, 0L, 11L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        assertFalse(LogCache.isDiscontinuous(left, right));
    }

    @Test
    public void testMergeBlock() {
        long size = 0;
        LogCacheBlock left = new LogCacheBlock(1024L * 1024);
        left.put(StreamRecordBatch.of(233L, 0L, 10L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        left.put(StreamRecordBatch.of(234L, 0L, 100L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        size += left.size();

        LogCacheBlock right = new LogCacheBlock(1024L * 1024);
        right.put(StreamRecordBatch.of(233L, 0L, 11L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
        right.put(StreamRecordBatch.of(235L, 0L, 200L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));
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
    public void testMergeBlockDoesNotAliasSourceStreamCaches() {
        LogCacheBlock left = new LogCacheBlock(1024L * 1024);
        left.put(StreamRecordBatch.of(233L, 0L, 10L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        LogCacheBlock right = new LogCacheBlock(1024L * 1024);
        right.put(StreamRecordBatch.of(233L, 0L, 11L, 1, TestUtils.random(20), DefaultByteBufSupplier.INSTANCE));

        LogCacheBlock merged = new LogCacheBlock(1024L * 1024);
        LogCache.mergeBlock(merged, left);
        LogCache.mergeBlock(merged, right);

        assertEquals(1, left.map.get(233L).records.size());
        assertEquals(11L, right.map.get(233L).records.get(0).getBaseOffset());
        assertEquals(2, merged.map.get(233L).records.size());
    }

    @Test
    public void testTryMergeLogic() throws ExecutionException, InterruptedException {
        LogCache logCache = new LogCache(Long.MAX_VALUE, 10_000L);
        final long streamId = 233L;
        final int blocksToCreate = LogCache.MERGE_BLOCK_THRESHOLD + 2;

        // create multiple blocks, each containing one record for the same stream with contiguous offsets
        for (int i = 0; i < blocksToCreate; i++) {
            logCache.put(StreamRecordBatch.of(streamId, 0L, i, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
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

    /**
     * Verify that freeOpsModCount is incremented when a stream is removed from a block,
     * and that tryMerge aborts the swap when the count changes between the two lock acquisitions.
     */
    @Test
    public void testMergeAbortedWhenBlockMutatedDuringMerge() throws ExecutionException, InterruptedException {
        LogCache logCache = new LogCache(Long.MAX_VALUE, 10_000L);
        final long streamId1 = 1L;
        final long streamId2 = 2L;

        // Two blocks, each with two streams so clearStreamRecords can mutate one while merge runs.
        logCache.put(StreamRecordBatch.of(streamId1, 0L, 0L, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
        logCache.put(StreamRecordBatch.of(streamId2, 0L, 0L, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
        LogCacheBlock block0 = logCache.archiveCurrentBlock();

        logCache.put(StreamRecordBatch.of(streamId1, 0L, 1L, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
        logCache.put(StreamRecordBatch.of(streamId2, 0L, 1L, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
        LogCacheBlock block1 = logCache.archiveCurrentBlock();

        assertEquals(0, block0.freeOpsModCount);
        assertEquals(0, block1.freeOpsModCount);

        // Simulate clearStreamRecords mutating blocks while tryMerge would be running.
        block0.free = true;
        block1.free = true;
        logCache.clearStreamRecords(streamId2);

        assertEquals(1, block0.freeOpsModCount);
        assertEquals(1, block1.freeOpsModCount);

        // tryMerge should detect the mutation and NOT replace the blocks.
        int blocksBefore = logCache.blocks.size();
        logCache.markFree(block0).get();
        // blocks should not have been merged since freeOpsModCount changed
        assertEquals(blocksBefore, logCache.blocks.size());
    }

    /**
     * Concurrent stress test: markFree (which triggers tryMerge) races with clearStreamRecords.
     * Without the freeOpsModCount guard this would cause IllegalReferenceCountException.
     */
    @Test
    public void testNoDoubleReleaseUnderConcurrentClearAndMerge() throws Exception {
        final int iterations = 200;
        for (int iter = 0; iter < iterations; iter++) {
            LogCache logCache = new LogCache(Long.MAX_VALUE, 10_000L);
            final long streamId1 = 1L;
            final long streamId2 = 2L;

            // Fill enough blocks to exceed MERGE_BLOCK_THRESHOLD so tryMerge actually runs.
            for (int i = 0; i < LogCache.MERGE_BLOCK_THRESHOLD + 2; i++) {
                logCache.put(StreamRecordBatch.of(streamId1, 0L, i, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
                logCache.put(StreamRecordBatch.of(streamId2, 0L, i, 1, TestUtils.random(1), DefaultByteBufSupplier.INSTANCE));
                LogCacheBlock b = logCache.archiveCurrentBlock();
                b.free = true;
            }

            CountDownLatch latch = new CountDownLatch(1);

            // Thread 1: trigger tryMerge via markFree
            CompletableFuture<Void> mergeFuture = CompletableFuture.runAsync(() -> {
                try {
                    latch.await();
                    logCache.markFree(logCache.blocks.get(0)).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Thread 2: concurrently clear streamId2 from all blocks
            CompletableFuture<Void> clearFuture = CompletableFuture.runAsync(() -> {
                try {
                    latch.await();
                    logCache.clearStreamRecords(streamId2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            latch.countDown();
            // Should complete without IllegalReferenceCountException
            CompletableFuture.allOf(mergeFuture, clearFuture).get();
        }
    }

}
