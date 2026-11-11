/*
 * Copyright 2026, AutoMQ HK Limited.
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

import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ReadProgressListener;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.impl.object.ObjectUtils;
import com.automq.stream.s3.wal.util.WALUtil;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Protects cache admission, shared fills and ownership independently of S3 or background writers. */
@Tag("S3Unit")
@Timeout(10)
class RouterWALReadCacheTest {
    /** The production block boundary is exclusive, and ranges crossing it keep their original read. */
    @Test
    void productionBlockBoundary() {
        int block = 8 * 1024 * 1024;
        assertEquals(block, RouterWALReadCache.BLOCK_SIZE);
        ObjectStorage storage = mock(ObjectStorage.class);
        when(storage.rangeRead(any(), eq("key"), eq(0L), eq((long) block)))
            .thenReturn(CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[block])));
        when(storage.rangeRead(any(), eq("key"), eq((long) block), eq(2L * block)))
            .thenReturn(CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[] {42})));
        when(storage.rangeRead(any(), eq("key"), eq((long) block - 1), eq((long) block + 1)))
            .thenReturn(CompletableFuture.completedFuture(bytes(2)));
        try (RouterWALReadCache cache = new RouterWALReadCache(2L * block)) {
            cache.read(storage, options(), "key", block - 1L, block).join().release();
            ByteBuf next = cache.read(storage, options(), "key", block, block + 1L).join();
            try {
                assertEquals(42, next.getByte(0));
            } finally {
                next.release();
            }
            assertTrue(cache.readIfPresent(storage, options(), "key", block - 1L, block + 1L).isEmpty());
            cache.read(storage, options(), "key", block - 1L, block + 1L).join().release();
            assertEquals(2L * block, cache.usedBytes());
            verify(storage).rangeRead(any(), eq("key"), eq(0L), eq((long) block));
            verify(storage).rangeRead(any(), eq("key"), eq((long) block), eq(2L * block));
            verify(storage).rangeRead(any(), eq("key"), eq((long) block - 1), eq((long) block + 1));
        }
    }

    /** A 1 GiB budget admits 128 full-block pending fills and bypasses the next miss. */
    @Test
    void productionBudgetReserves128PendingFills() {
        ObjectStorage storage = mock(ObjectStorage.class);
        List<CompletableFuture<ByteBuf>> fills = new ArrayList<>();
        when(storage.rangeRead(any(), anyString(), anyLong(), anyLong())).thenAnswer(invocation -> {
            String path = invocation.getArgument(1);
            if (path.equals("overflow")) {
                return CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER);
            }
            CompletableFuture<ByteBuf> fill = new CompletableFuture<>();
            fills.add(fill);
            return fill;
        });
        try (RouterWALReadCache cache = new RouterWALReadCache(1024L * 1024 * 1024)) {
            for (int i = 0; i < 128; i++) {
                cache.read(storage, options(), "key-" + i, 1, 2);
            }
            assertEquals(128, fills.size());
            assertEquals(1024L * 1024 * 1024, cache.usedBytes());
            cache.read(storage, options(), "overflow", 1, 2).join().release();
            verify(storage, times(128)).rangeRead(any(), anyString(), eq(0L), eq((long) RouterWALReadCache.BLOCK_SIZE));
            verify(storage).rangeRead(any(), eq("overflow"), eq(1L), eq(2L));
            fills.forEach(fill -> fill.completeExceptionally(new IOException("test failure")));
            assertEquals(0, cache.usedBytes());
        }
    }

    /** Concurrent readers share one fill, while mutation and cancellation remain private to each caller. */
    @Test
    void sharedFillReturnsIndependentCopies() {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        when(storage.rangeRead(any(), eq("key"), eq(0L), eq(16L))).thenReturn(download);
        ByteBuf data = bytes(16);
        try (RouterWALReadCache cache = new RouterWALReadCache(32, 16)) {
            ObjectStorage.ReadOptions firstOptions = options();
            CompletableFuture<ByteBuf> first = cache.read(storage, firstOptions, "key", 2, 6);
            firstOptions.bucket((short) 5);
            CompletableFuture<ByteBuf> second = cache.read(storage, options(), "key", 4, 8);
            CompletableFuture<ByteBuf> cancelled = cache.read(storage, options(), "key", 7, 9);
            assertTrue(cancelled.cancel(false));
            assertFalse(download.isCancelled());
            assertEquals(16, cache.usedBytes());
            download.complete(data);
            ByteBuf firstResult = first.join();
            ByteBuf secondResult = second.join();
            try {
                firstResult.setByte(2, 99);
                assertEquals(4, secondResult.getByte(0));
                assertEquals(4, data.getByte(4));
            } finally {
                firstResult.release();
                secondResult.release();
            }
            ByteBuf hit = cache.read(storage, options(), "key", 4, 8).join();
            assertEquals(4, hit.getByte(0));
            hit.release();
            verify(storage).rangeRead(argThat(o -> o.source() == S3RequestSource.ROUTER_WAL
                && o.bucket() == ObjectStorage.ReadOptions.UNSET_BUCKET), eq("key"), eq(0L), eq(16L));
        }
        assertEquals(0, data.refCnt());
    }

    /** A missing-layout lookup preserves the only cached block, and hits return independent writable copies. */
    @Test
    void completedLookupDoesNotLoadOrEvict() {
        ObjectStorage storage = mock(ObjectStorage.class);
        ByteBuf data = bytes(8);
        when(storage.rangeRead(any(), eq("fallback"), eq(0L), eq(16L))).thenReturn(CompletableFuture.completedFuture(data));
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            assertTrue(cache.readIfPresent(storage, options(), "fallback", 2, 4).isEmpty());
            cache.read(storage, options(), "fallback", 2, 4).join().release();
            assertTrue(cache.readIfPresent(storage, options(), "preferred", 2, 4).isEmpty());
            ByteBuf first = cache.readIfPresent(storage, options(), "fallback", 2, 4).orElseThrow().join();
            try {
                first.setByte(0, 99);
                ByteBuf replay = cache.readIfPresent(storage, options(), "fallback", 2, 12)
                    .orElseThrow().join();
                try {
                    assertEquals(2, replay.getByte(0));
                    assertEquals(6, replay.readableBytes());
                } finally {
                    replay.release();
                }
            } finally {
                first.release();
            }
            assertEquals(2, data.getByte(2));
            assertEquals(16, cache.usedBytes());
            verify(storage).rangeRead(any(), eq("fallback"), eq(0L), eq(16L));
            verifyNoMoreInteractions(storage);
        }
        assertEquals(0, data.refCnt());
    }

    /** Lookup cannot treat pending or failed fills as proof that an alternate object layout exists. */
    @Test
    void lookupIgnoresPendingAndFailedFills() {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        when(storage.rangeRead(any(), eq("key"), eq(0L), eq(16L))).thenReturn(download);
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            CompletableFuture<ByteBuf> caller = cache.read(storage, options(), "key", 2, 4);
            assertTrue(cache.readIfPresent(storage, options(), "key", 2, 4).isEmpty());
            assertEquals(16, cache.usedBytes());
            IOException failure = new IOException("read failed");
            download.completeExceptionally(failure);
            assertSame(failure, assertThrows(CompletionException.class, caller::join).getCause());
            assertTrue(cache.readIfPresent(storage, options(), "key", 2, 4).isEmpty());
            assertEquals(0, cache.usedBytes());
            verify(storage).rangeRead(any(), eq("key"), eq(0L), eq(16L));
            verifyNoMoreInteractions(storage);
        }
    }

    /** Ready blocks use LRU eviction, releasing the cache reference without invalidating caller copies. */
    @Test
    void evictsLeastRecentlyUsedIdleBlock() {
        ObjectStorage storage = mock(ObjectStorage.class);
        ByteBuf a = bytes(16);
        ByteBuf b = bytes(16);
        ByteBuf c = bytes(16);
        when(storage.rangeRead(any(), eq("a"), eq(0L), eq(16L))).thenReturn(CompletableFuture.completedFuture(a));
        when(storage.rangeRead(any(), eq("b"), eq(0L), eq(16L))).thenReturn(CompletableFuture.completedFuture(b));
        when(storage.rangeRead(any(), eq("c"), eq(0L), eq(16L))).thenReturn(CompletableFuture.completedFuture(c));
        try (RouterWALReadCache cache = new RouterWALReadCache(32, 16)) {
            cache.read(storage, options(), "a", 1, 2).join().release();
            ByteBuf bCopy = cache.read(storage, options(), "b", 1, 2).join();
            cache.read(storage, options(), "a", 1, 2).join().release();
            cache.read(storage, options(), "c", 1, 2).join().release();
            assertEquals(0, b.refCnt());
            assertEquals(1, bCopy.getByte(0));
            bCopy.release();
            assertEquals(1, a.refCnt());
            assertEquals(1, c.refCnt());
            assertEquals(32, cache.usedBytes());
        }
        assertEquals(0, a.refCnt());
        assertEquals(0, c.refCnt());
    }

    /** A pending fill reserves the full budget; an unrelated miss retains its original small range. */
    @Test
    void bypassesWhenBudgetIsPinned() {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        when(storage.rangeRead(any(), eq("a"), eq(0L), eq(16L))).thenReturn(download);
        ByteBuf bypass = bytes(4);
        when(storage.rangeRead(any(), eq("b"), eq(3L), eq(7L))).thenReturn(CompletableFuture.completedFuture(bypass));
        ByteBuf data = bytes(8);
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            CompletableFuture<ByteBuf> first = cache.read(storage, options(), "a", 0, 4);
            assertSame(bypass, cache.read(storage, options(), "b", 3, 7).join());
            bypass.release();
            assertEquals(16, cache.usedBytes());
            download.complete(data);
            first.join().release();
            // Short blocks retain their reservation, bounding metadata as well as payload memory.
            assertEquals(16, cache.usedBytes());
            verify(storage).rangeRead(any(), eq("b"), eq(3L), eq(7L));
        }
        assertEquals(0, data.refCnt());
    }

    /** Both asynchronous failures and synchronous exceptions release admission and allow a fresh fill. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedFillIsNotCached(boolean synchronous) {
        ObjectStorage storage = mock(ObjectStorage.class);
        RuntimeException failure = new RuntimeException(new IOException("read failed"));
        ByteBuf data = bytes(16);
        if (synchronous) {
            when(storage.rangeRead(any(), anyString(), anyLong(), anyLong()))
                .thenThrow(failure).thenReturn(CompletableFuture.completedFuture(data));
        } else {
            when(storage.rangeRead(any(), anyString(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(failure), CompletableFuture.completedFuture(data));
        }
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            CompletionException error = assertThrows(CompletionException.class,
                () -> cache.read(storage, options(), "key", 2, 4).join());
            assertSame(failure, error.getCause());
            assertEquals(0, cache.usedBytes());
            cache.read(storage, options(), "key", 2, 4).join().release();
            verify(storage, times(2)).rangeRead(any(), eq("key"), eq(0L), eq(16L));
        }
        assertEquals(0, data.refCnt());
    }

    /** Shutdown drains an admitted fill, including when its only caller has cancelled. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeReleasesLateFill(boolean cancel) {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        when(storage.rangeRead(any(), anyString(), anyLong(), anyLong())).thenReturn(download);
        RouterWALReadCache cache = new RouterWALReadCache(16, 16);
        CompletableFuture<ByteBuf> caller = cache.read(storage, options(), "key", 2, 4);
        if (cancel) {
            caller.cancel(false);
        }
        cache.close();
        assertEquals(16, cache.usedBytes());
        ByteBuf data = bytes(16);
        download.complete(data);
        if (!cancel) {
            ByteBuf copy = caller.join();
            assertEquals(2, copy.getByte(0));
            copy.release();
        }
        assertEquals(0, cache.usedBytes());
        assertEquals(0, data.refCnt());
        assertThrows(CompletionException.class, () -> cache.read(storage, options(), "key", 2, 4).join());
        assertThrows(CompletionException.class, () -> cache.readIfPresent(storage, options(), "key", 2, 4).orElseThrow().join());
        cache.close();
    }

    /** Client identity and aligned block offsets prevent collisions between buckets and object ranges. */
    @Test
    void separatesStorageClientsAndBlocks() {
        ObjectStorage first = mock(ObjectStorage.class);
        ObjectStorage second = mock(ObjectStorage.class);
        when(first.rangeRead(any(), eq("key"), anyLong(), anyLong()))
            .thenAnswer(invocation -> CompletableFuture.completedFuture(bytes(16)));
        when(second.rangeRead(any(), eq("key"), anyLong(), anyLong()))
            .thenAnswer(invocation -> CompletableFuture.completedFuture(bytes(16)));
        try (RouterWALReadCache cache = new RouterWALReadCache(48, 16)) {
            cache.read(first, options(), "key", 1, 3).join().release();
            ByteBuf nextBlock = cache.read(first, options(), "key", 17, 19).join();
            assertEquals(1, nextBlock.getByte(0));
            nextBlock.release();
            cache.read(second, options(), "key", 1, 3).join().release();
            assertTrue(cache.readIfPresent(second, options(), "key", 17, 19).isEmpty());
            assertEquals(48, cache.usedBytes());
            verify(first).rangeRead(any(), eq("key"), eq(0L), eq(16L));
            verify(first).rangeRead(any(), eq("key"), eq(16L), eq(32L));
            verify(second).rangeRead(any(), eq("key"), eq(0L), eq(16L));
        }
    }

    /** EOF truncation matches S3, and reads starting beyond EOF retain their original storage error. */
    @Test
    void handlesShortEofBlocks() {
        ObjectStorage storage = mock(ObjectStorage.class);
        ByteBuf data = bytes(8);
        data.readerIndex(2);
        when(storage.rangeRead(any(), eq("key"), eq(0L), eq(16L))).thenReturn(CompletableFuture.completedFuture(data));
        IOException failure = new IOException("range starts beyond EOF");
        when(storage.rangeRead(any(), eq("key"), eq(8L), eq(10L))).thenReturn(CompletableFuture.failedFuture(failure));
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            ByteBuf copy = cache.read(storage, options(), "key", 3, 12).join();
            assertEquals(3, copy.readableBytes());
            assertEquals(5, copy.getByte(0));
            copy.release();
            assertTrue(cache.readIfPresent(storage, options(), "key", 8, 10).isEmpty());
            CompletionException error = assertThrows(CompletionException.class,
                () -> cache.read(storage, options(), "key", 8, 10).join());
            assertSame(failure, error.getCause());
            verify(storage).rangeRead(any(), eq("key"), eq(8L), eq(10L));
        }
        assertEquals(0, data.refCnt());
    }

    /** Other sources, open-ended requests, empty requests and crossing ranges bypass the cache. */
    @Test
    void preservesUncacheableReads() {
        ObjectStorage storage = mock(ObjectStorage.class);
        when(storage.rangeRead(any(), anyString(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER));
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            cache.read(storage, options().source(S3RequestSource.DELTA_WAL), "key", 2, 4).join().release();
            cache.read(storage, options(), "key", 14, 18).join().release();
            cache.read(storage, options(), "key", 0, -1).join().release();
            cache.read(storage, options(), "key", 0, 0).join().release();
            assertTrue(cache.readIfPresent(storage, options().source(S3RequestSource.DELTA_WAL), "key", 2, 4).isEmpty());
            assertTrue(cache.readIfPresent(storage, options(), "key", 14, 18).isEmpty());
            assertTrue(cache.readIfPresent(storage, options(), "key", 0, -1).isEmpty());
            assertTrue(cache.readIfPresent(storage, options(), "key", 0, 0).isEmpty());
            assertEquals(0, cache.usedBytes());
            verify(storage).rangeRead(argThat(o -> o.source() == S3RequestSource.DELTA_WAL), eq("key"), eq(2L), eq(4L));
            verify(storage).rangeRead(any(), eq("key"), eq(14L), eq(18L));
            verify(storage).rangeRead(any(), eq("key"), eq(0L), eq(-1L));
            verify(storage).rangeRead(any(), eq("key"), eq(0L), eq(0L));
        }
    }

    /** Parallel miss admission cannot issue duplicate block fills or exceed the reservation budget. */
    @Test
    void concurrentAdmissionStartsOneFill() {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        when(storage.rangeRead(any(), anyString(), anyLong(), anyLong())).thenReturn(download);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            List<CompletableFuture<CompletableFuture<ByteBuf>>> submitted = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                submitted.add(CompletableFuture.supplyAsync(() -> cache.read(storage, options(), "key", 2, 4), executor));
            }
            List<CompletableFuture<ByteBuf>> callers = submitted.stream().map(CompletableFuture::join).toList();
            assertEquals(16, cache.usedBytes());
            download.complete(bytes(16));
            callers.forEach(caller -> caller.join().release());
            verify(storage).rangeRead(any(), eq("key"), eq(0L), eq(16L));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Storage must not return a buffer larger than the finite range reserved for a fill. */
    @Test
    void rejectsOversizedFill() {
        ObjectStorage storage = mock(ObjectStorage.class);
        ByteBuf data = bytes(17);
        when(storage.rangeRead(any(), anyString(), anyLong(), anyLong())).thenReturn(CompletableFuture.completedFuture(data));
        try (RouterWALReadCache cache = new RouterWALReadCache(16, 16)) {
            assertThrows(CompletionException.class, () -> cache.read(storage, options(), "key", 2, 4).join());
            assertEquals(0, cache.usedBytes());
            assertEquals(0, data.refCnt());
        }
    }

    /** Complete records and late joiners return early, while the fill stays pinned and owns its budget. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void progressiveReadsKeepFillPinnedUntilCompletion(boolean success) {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        AtomicReference<ObjectStorage.ReadOptions> fillOptions = new AtomicReference<>();
        when(storage.rangeRead(any(), eq("key"), eq(0L), eq(128L))).thenAnswer(call -> {
            fillOptions.set(call.getArgument(0));
            return download;
        });
        ByteBuf data = recordBlock();
        RouterWALReadCache cache = new RouterWALReadCache(128, 128, true);
        try {
            CompletableFuture<ByteBuf> first = cache.read(storage, options(), "key", 0, 32,
                buf -> ObjectUtils.isValidRecordBuf(buf, 0));
            CompletableFuture<ByteBuf> second = cache.read(storage, options(), "key", 32, 64,
                buf -> ObjectUtils.isValidRecordBuf(buf, 32));
            CompletableFuture<ByteBuf> cancelled = cache.read(storage, options(), "key", 64, 96,
                buf -> ObjectUtils.isValidRecordBuf(buf, 64));
            cancelled.cancel(false);
            CompletableFuture<ByteBuf> tail = cache.read(storage, options(), "key", 96, 128,
                buf -> ObjectUtils.isValidRecordBuf(buf, 96));
            ReadProgressListener progress = fillOptions.get().progressListener().get();
            progress.onProgress(data.slice(0, 31).asReadOnly());
            assertFalse(first.isDone());
            progress.onProgress(data.slice(0, 32).asReadOnly());
            ByteBuf early = first.join();
            early.setByte(24, 99);
            early.release();
            ByteBuf lateJoin = cache.read(storage, options(), "key", 0, 32,
                buf -> ObjectUtils.isValidRecordBuf(buf, 0)).join();
            assertEquals(0, lateJoin.getByte(24));
            lateJoin.release();
            assertFalse(second.isDone());
            assertFalse(download.isDone());
            assertTrue(cache.readIfPresent(storage, options(), "key", 0, 32).isEmpty());
            assertEquals(128, cache.usedBytes());
            cache.close();
            assertEquals(128, cache.usedBytes());
            progress.onProgress(data.slice(0, 64).asReadOnly());
            second.join().release();
            assertFalse(download.isDone());
            if (success) {
                download.complete(data.retainedDuplicate());
                tail.join().release();
            } else {
                progress.onFailure();
                download.completeExceptionally(new IOException("tail failed"));
                assertThrows(CompletionException.class, tail::join);
            }
            assertEquals(0, cache.usedBytes());
            assertTrue(cancelled.isCancelled());
            assertEquals(1, data.refCnt());
            verify(storage).rangeRead(any(), eq("key"), eq(0L), eq(128L));
        } finally {
            download.completeExceptionally(new IOException("test cleanup"));
            cache.close();
            data.release();
        }
    }

    /** A failed prefix cannot be stitched to a retry, and competing attempts cannot overwrite it. */
    @Test
    void progressiveAttemptsAreIsolated() {
        ByteBuf data = recordBlock();
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        try (RouterWALReadProgress progress = new RouterWALReadProgress(128, () -> { })) {
            progress.add(32, 64, result, buf -> ObjectUtils.isValidRecordBuf(buf, 32));
            ReadProgressListener original = progress.newAttempt();
            ReadProgressListener retry = progress.newAttempt();
            original.onProgress(data.slice(0, 40));
            retry.onProgress(data.slice(0, 64));
            assertFalse(result.isDone());
            original.onFailure();
            retry.onProgress(data.slice(0, 56));
            original.onProgress(data.slice(0, 64));
            assertFalse(result.isDone());
            retry.onProgress(data.slice(0, 64));
            assertTrue(ObjectUtils.isValidRecordBuf(result.join(), 32));
            result.join().release();
            progress.close();
            retry.onProgress(data);
        } finally {
            data.release();
        }
    }

    /** Truncation, corruption and a wrong record identity never qualify for early delivery. */
    @Test
    void progressiveReadsRequireIndependentIntegrity() {
        ByteBuf data = recordBlock();
        try {
            assertFalse(ObjectUtils.isValidRecordBuf(data.slice(0, 23), 0));
            assertFalse(ObjectUtils.isValidRecordBuf(data.slice(0, 31), 0));
            assertFalse(ObjectUtils.isValidRecordBuf(data.slice(0, 32), 1));
            for (int corruptOffset : new int[] {4, 20, 24}) {
                ByteBuf corrupt = data.copy();
                corrupt.setByte(corruptOffset, corrupt.getByte(corruptOffset) ^ 1);
                CompletableFuture<ByteBuf> result = new CompletableFuture<>();
                try (RouterWALReadProgress progress = new RouterWALReadProgress(128, () -> { })) {
                    progress.add(0, 32, result, buf -> ObjectUtils.isValidRecordBuf(buf, 0));
                    progress.newAttempt().onProgress(corrupt);
                    assertFalse(result.isDone());
                } finally {
                    corrupt.release();
                }
            }
        } finally {
            data.release();
        }
    }

    /** Disabled progress and validators that reject or throw preserve the normal full-response path. */
    @ParameterizedTest
    @ValueSource(strings = {"disabled", "reject", "throw"})
    void progressiveFallbackPreservesFullResponse(String mode) {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        AtomicReference<ObjectStorage.ReadOptions> fillOptions = new AtomicReference<>();
        when(storage.rangeRead(any(), eq("key"), eq(0L), eq(128L))).thenAnswer(call -> {
            fillOptions.set(call.getArgument(0));
            return download;
        });
        ByteBuf data = recordBlock();
        try (RouterWALReadCache cache = new RouterWALReadCache(128, 128, !mode.equals("disabled"))) {
            CompletableFuture<ByteBuf> result = cache.read(storage, options(), "key", 0, 32, buf -> {
                if (mode.equals("throw")) {
                    throw new IllegalArgumentException("validation failed");
                }
                return mode.equals("disabled");
            });
            if (mode.equals("disabled")) {
                assertNull(fillOptions.get().progressListener());
            } else {
                fillOptions.get().progressListener().get().onProgress(data);
            }
            assertFalse(result.isDone());
            download.complete(data.retainedDuplicate());
            ByteBuf copy = result.join();
            assertTrue(ObjectUtils.isValidRecordBuf(copy, 0));
            copy.release();
        } finally {
            download.completeExceptionally(new IOException("test cleanup"));
            data.release();
        }
    }

    /** If full completion wins during validation, the unused early copy is released exactly once. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fullCompletionWinsDuringEarlyValidation(boolean success) {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ByteBuf> download = new CompletableFuture<>();
        AtomicReference<ObjectStorage.ReadOptions> fillOptions = new AtomicReference<>();
        when(storage.rangeRead(any(), eq("key"), eq(0L), eq(128L))).thenAnswer(call -> {
            fillOptions.set(call.getArgument(0));
            return download;
        });
        ByteBuf data = recordBlock();
        AtomicReference<ByteBuf> earlyCopy = new AtomicReference<>();
        try (RouterWALReadCache cache = new RouterWALReadCache(128, 128, true)) {
            CompletableFuture<ByteBuf> result = cache.read(storage, options(), "key", 0, 32, buf -> {
                earlyCopy.set(buf);
                if (success) {
                    download.complete(data.retainedDuplicate());
                } else {
                    download.completeExceptionally(new IOException("full response failed"));
                }
                return true;
            });
            fillOptions.get().progressListener().get().onProgress(data.slice(0, 32));
            assertEquals(0, earlyCopy.get().refCnt());
            if (success) {
                ByteBuf copy = result.join();
                assertTrue(ObjectUtils.isValidRecordBuf(copy, 0));
                copy.release();
            } else {
                assertThrows(CompletionException.class, result::join);
                assertEquals(0, cache.usedBytes());
            }
        } finally {
            download.completeExceptionally(new IOException("test cleanup"));
            data.release();
        }
    }

    private static ByteBuf recordBlock() {
        ByteBuf data = Unpooled.buffer(128, 128);
        for (int offset = 0; offset < 128; offset += 32) {
            ByteBuf body = Unpooled.buffer(8, 8).writeLong(offset);
            ByteBuf header = WALUtil.generateHeader(body,
                Unpooled.buffer(RecordHeader.RECORD_HEADER_SIZE, RecordHeader.RECORD_HEADER_SIZE), 0, offset);
            try {
                data.writeBytes(header).writeBytes(body);
            } finally {
                header.release();
                body.release();
            }
        }
        return data;
    }

    private static ObjectStorage.ReadOptions options() {
        return new ObjectStorage.ReadOptions().source(S3RequestSource.ROUTER_WAL);
    }

    private static ByteBuf bytes(int size) {
        ByteBuf result = Unpooled.buffer(size, size);
        for (int i = 0; i < size; i++) {
            result.writeByte(i);
        }
        return result;
    }
}
