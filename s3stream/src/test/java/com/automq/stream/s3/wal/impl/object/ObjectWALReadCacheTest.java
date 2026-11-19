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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3ReadPurposeContext;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Time;
import com.google.common.base.Ticker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies consumable object ownership, miss coalescing, expiry, and reader layout integration. */
@Tag("S3Unit")
@Timeout(15)
public class ObjectWALReadCacheTest {
    private static final int HEADER = WALObjectHeader.WAL_HEADER_SIZE_V1;
    private static final int RECORD_SIZE = RecordHeader.RECORD_HEADER_SIZE + StreamRecordBatchCodec.HEADER_SIZE + 4;
    private static final int TRIM_SIZE = RecordHeader.RECORD_HEADER_SIZE + StreamRecordBatchCodec.HEADER_SIZE;

    /** Rounded-up heap units match the existing router cache and never allow zero concurrent loads. */
    @Test
    public void testHeapBudgets() {
        assertEquals(100L << 20, ObjectWALReadCache.cacheMaxWeight(1));
        assertEquals(2, ObjectWALReadCache.maxInflight(1));
        assertEquals(100L << 20, ObjectWALReadCache.cacheMaxWeight(6L << 30));
        assertEquals(200L << 20, ObjectWALReadCache.cacheMaxWeight((6L << 30) + 1));
        assertEquals(8, ObjectWALReadCache.maxInflight(24L << 30));
    }

    /** Disjoint delayed reads share one GET and zero-copy slices survive fully-read eviction. */
    @Test
    public void testCoalescedAndOutOfOrderConsumption() {
        Storage storage = new Storage();
        ByteBuf object = object(0, 3);
        try (ObjectWALReadCache cache = cache()) {
            CompletableFuture<ByteBuf> third = read(cache, storage, "a", 0, 2);
            CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
            assertEquals(1, storage.calls.size());
            assertEquals(0, storage.calls.get(0).start);
            assertEquals(-1, storage.calls.get(0).end);
            storage.complete(0, object);
            ByteBuf thirdBuf = third.join();
            ByteBuf firstBuf = first.join();
            firstBuf.setInt(RECORD_SIZE - 4, 99);
            assertEquals(99, object.getInt(HEADER + RECORD_SIZE - 4));
            ByteBuf secondBuf = read(cache, storage, "a", 0, 1).join();
            assertEquals(3, object.refCnt());
            cache.close();
            assertEquals(2, thirdBuf.getInt(RECORD_SIZE - 4));
            assertEquals(1, secondBuf.getInt(RECORD_SIZE - 4));
            firstBuf.release();
            secondBuf.release();
            thirdBuf.release();
            assertEquals(0, object.refCnt());
        }
    }

    /** Consumed and partially overlapping reads use fresh isolated responses, not modified cached bytes. */
    @Test
    public void testRepeatedAndOverlappingReads() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = cache()) {
            CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
            storage.complete(0, object(0, 3));
            ByteBuf firstBuf = first.join();
            firstBuf.setInt(RECORD_SIZE - 4, 99);
            CompletableFuture<ByteBuf> duplicate = read(cache, storage, "a", 0, 0);
            assertEquals(2, storage.calls.size());
            assertEquals(HEADER, storage.calls.get(1).start);
            ByteBuf response = Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE);
            response.setInt(RECORD_SIZE - 4, 7);
            storage.complete(1, response);
            ByteBuf duplicateBuf = duplicate.join();
            assertEquals(0, response.refCnt());
            assertEquals(7, duplicateBuf.getInt(RECORD_SIZE - 4));
            firstBuf.release();
            duplicateBuf.release();

            CompletableFuture<ByteBuf> overlap = cache.read(storage.storage, new ReadOptions(), "a",
                HEADER + RECORD_SIZE - 1, HEADER + RECORD_SIZE + 1);
            assertEquals(3, storage.calls.size());
            storage.complete(2, Unpooled.buffer(2).writeShort(1));
            overlap.join().release();
            read(cache, storage, "a", 0, 1).join().release();
            assertEquals(3, storage.calls.size());
        }
    }

    /** The load cap degrades new objects to ranges while still sharing an already-loading object. */
    @Test
    public void testSaturatedLoadsAndFailureRetry() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = new ObjectWALReadCache(1024, 1, Ticker.systemTicker())) {
            CompletableFuture<ByteBuf> a = read(cache, storage, "a", 0, 0);
            CompletableFuture<ByteBuf> a2 = read(cache, storage, "a", 0, 1);
            CompletableFuture<ByteBuf> b = read(cache, storage, "b", 0, 0);
            assertEquals(2, storage.calls.size());
            assertEquals(HEADER, storage.calls.get(1).start);
            assertEquals(HEADER + RECORD_SIZE, storage.calls.get(1).end);
            storage.complete(1, Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE));
            b.join().release();
            storage.calls.get(0).future.completeExceptionally(new IllegalStateException("failed"));
            assertEquals(4, storage.calls.size());
            for (int i = 2; i <= 3; i++) {
                assertTrue(storage.calls.get(i).start >= HEADER);
                assertEquals(RECORD_SIZE, storage.calls.get(i).end - storage.calls.get(i).start);
                storage.complete(i, Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE));
            }
            a.join().release();
            a2.join().release();
            CompletableFuture<ByteBuf> retry = read(cache, storage, "a", 0, 0);
            assertEquals(0, storage.calls.get(4).start);
            storage.complete(4, object(0, 2));
            retry.join().release();
        }
    }

    /** Multiple trim records are excluded from unread coverage so business consumption releases the object. */
    @Test
    public void testTrimPrefixAndTrimOnlyObjects() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = cache()) {
            ByteBuf object = object(2, 1);
            CompletableFuture<ByteBuf> record = read(cache, storage, "a", 2, 0);
            storage.complete(0, object);
            record.join().release();
            assertEquals(0, object.refCnt());

            ByteBuf trims = object(2, 0);
            CompletableFuture<ByteBuf> trimRead = cache.read(storage.storage, new ReadOptions(), "trim", HEADER, HEADER + TRIM_SIZE);
            storage.complete(1, trims);
            assertEquals(0, trims.refCnt());
            assertEquals(3, storage.calls.size());
            storage.complete(2, Unpooled.buffer(TRIM_SIZE).writeZero(TRIM_SIZE));
            trimRead.join().release();
        }
    }

    /** Access renews the two-second idle timeout, while an expired entry cannot serve another record. */
    @Test
    public void testIdleExpiryIsRenewedByAccess() {
        Storage storage = new Storage();
        AtomicLong nanos = new AtomicLong();
        Ticker ticker = new Ticker() {
            @Override
            public long read() {
                return nanos.get();
            }
        };
        try (ObjectWALReadCache cache = new ObjectWALReadCache(1024, 1, ticker)) {
            CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
            storage.complete(0, object(0, 4));
            first.join().release();
            nanos.set(TimeUnit.MILLISECONDS.toNanos(1500));
            read(cache, storage, "a", 0, 1).join().release();
            nanos.set(TimeUnit.MILLISECONDS.toNanos(3000));
            read(cache, storage, "a", 0, 2).join().release();
            assertEquals(1, storage.calls.size());
            nanos.set(TimeUnit.MILLISECONDS.toNanos(5100));
            CompletableFuture<ByteBuf> expired = read(cache, storage, "a", 0, 3);
            assertEquals(2, storage.calls.size());
            storage.complete(1, object(0, 4));
            expired.join().release();
        }
    }

    /** Capacity eviction drops only cache ownership and does not invalidate a caller's retained slice. */
    @Test
    public void testCapacityEvictionPreservesSlice() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = new ObjectWALReadCache(HEADER + 2L * RECORD_SIZE, 2, Ticker.systemTicker())) {
            ByteBuf object = object(0, 2);
            CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
            storage.complete(0, object);
            ByteBuf slice = first.join();
            CompletableFuture<ByteBuf> second = read(cache, storage, "b", 0, 0);
            storage.complete(1, object(0, 2));
            second.join().release();
            assertEquals(0, slice.getInt(RECORD_SIZE - 4));
            cache.close();
            assertEquals(1, object.refCnt());
            slice.release();
            assertEquals(0, object.refCnt());
        }
    }

    /** Closing releases late objects and both existing waiters and new reads fall back to finite ranges. */
    @Test
    public void testCloseDuringLoad() {
        Storage storage = new Storage();
        ObjectWALReadCache cache = cache();
        CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
        CompletableFuture<ByteBuf> second = read(cache, storage, "a", 0, 1);
        cache.close();
        assertFalse(first.isDone());
        assertFalse(second.isDone());
        ByteBuf object = object(0, 2);
        storage.complete(0, object);
        assertEquals(0, object.refCnt());
        assertEquals(3, storage.calls.size());
        for (int i = 1; i <= 2; i++) {
            assertTrue(storage.calls.get(i).start >= HEADER);
            storage.complete(i, Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE));
        }
        first.join().release();
        second.join().release();
        CompletableFuture<ByteBuf> bypass = read(cache, storage, "a", 0, 0);
        assertEquals(4, storage.calls.size());
        assertEquals(HEADER, storage.calls.get(3).start);
        storage.complete(3, Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE));
        bypass.join().release();
    }

    /** Malformed full objects release their buffers and degrade to finite reads without poisoning the cache. */
    @Test
    public void testMalformedLoadFallsBackToRange() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = cache()) {
            CompletableFuture<ByteBuf> invalid = read(cache, storage, "b", 0, 0);
            ByteBuf bad = object(0, 1);
            bad.setLong(12, 1);
            storage.complete(0, bad);
            assertEquals(0, bad.refCnt());
            assertEquals(HEADER, storage.calls.get(1).start);
            storage.complete(1, Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE));
            invalid.join().release();
            CompletableFuture<ByteBuf> retry = read(cache, storage, "b", 0, 0);
            storage.complete(2, object(0, 1));
            retry.join().release();
        }
    }

    /** Storage identity separates equal keys in different buckets or endpoints. */
    @Test
    public void testStorageNamespaces() {
        Storage a = new Storage();
        Storage b = new Storage();
        try (ObjectWALReadCache cache = cache()) {
            CompletableFuture<ByteBuf> first = read(cache, a, "same", 0, 0);
            CompletableFuture<ByteBuf> second = read(cache, b, "same", 0, 0);
            assertEquals(1, a.calls.size());
            assertEquals(1, b.calls.size());
            a.complete(0, object(0, 1));
            b.complete(0, object(0, 1));
            first.join().release();
            second.join().release();
        }
    }

    /** Simultaneous requests from different readers register only one full-object load. */
    @Test
    public void testConcurrentMisses() throws Exception {
        Storage storage = new Storage();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch ready = new CountDownLatch(8);
        CountDownLatch start = new CountDownLatch(1);
        try (ObjectWALReadCache cache = cache()) {
            List<CompletableFuture<CompletableFuture<ByteBuf>>> requests = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                int index = i;
                requests.add(CompletableFuture.supplyAsync(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(ex);
                    }
                    return read(cache, storage, "a", 0, index);
                }, executor));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            CompletableFuture.allOf(requests.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);
            assertEquals(1, storage.calls.size());
            ByteBuf object = object(0, 8);
            storage.complete(0, object);
            for (int i = 0; i < requests.size(); i++) {
                ByteBuf slice = requests.get(i).join().join();
                assertEquals(i, slice.getInt(RECORD_SIZE - 4));
                slice.release();
            }
            assertEquals(0, object.refCnt());
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    /** Cache and in-flight hits progress while another thread holds the global miss-registration monitor. */
    @Test
    public void testHitsBypassGlobalMonitor() throws Exception {
        Storage storage = new Storage();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ObjectWALReadCache cache = cache()) {
            CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
            CompletableFuture<ByteBuf> joined;
            synchronized (cache) {
                joined = CompletableFuture.supplyAsync(() -> read(cache, storage, "a", 0, 1), executor)
                    .get(5, TimeUnit.SECONDS);
            }
            assertEquals(1, storage.calls.size());
            storage.complete(0, object(0, 3));
            first.join().release();
            joined.join().release();
            synchronized (cache) {
                ByteBuf hit = CompletableFuture.supplyAsync(() -> read(cache, storage, "a", 0, 2).join(), executor)
                    .get(5, TimeUnit.SECONDS);
                hit.release();
            }
            assertEquals(1, storage.calls.size());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Readers racing load completion consistently retry through the published cache without losing requests. */
    @Test
    public void testCompletionRacesWithJoiningReaders() throws Exception {
        Storage storage = new Storage();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (ObjectWALReadCache cache = cache()) {
            for (int round = 0; round < 100; round++) {
                String path = "object-" + round;
                CompletableFuture<ByteBuf> first = read(cache, storage, path, 0, 0);
                CountDownLatch start = new CountDownLatch(1);
                List<CompletableFuture<ByteBuf>> readers = new ArrayList<>();
                for (int i = 1; i < 4; i++) {
                    int index = i;
                    readers.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            throw new CompletionException(ex);
                        }
                        return read(cache, storage, path, 0, index);
                    }, executor).thenCompose(future -> future));
                }
                ByteBuf object = object(0, 4);
                int requestIndex = round;
                CompletableFuture<Void> completion = CompletableFuture.runAsync(() -> {
                    start.countDown();
                    storage.complete(requestIndex, object);
                }, executor);
                first.get(5, TimeUnit.SECONDS).release();
                for (int i = 0; i < readers.size(); i++) {
                    ByteBuf result = readers.get(i).get(5, TimeUnit.SECONDS);
                    assertEquals(i + 1, result.getInt(RECORD_SIZE - 4));
                    result.release();
                }
                completion.get(5, TimeUnit.SECONDS);
                assertEquals(round + 1, storage.calls.size());
                assertEquals(0, object.refCnt());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** Objects are published and load slots released before any waiting reader is notified. */
    @Test
    public void testLoadPublishesBeforeNotifyingReaders() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = new ObjectWALReadCache(1024, 1, Ticker.systemTicker())) {
            CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
            CompletableFuture<Void> callback = first.thenAccept(buf -> {
                read(cache, storage, "a", 0, 1).join().release();
                CompletableFuture<ByteBuf> nextLoad = read(cache, storage, "b", 0, 0);
                assertEquals(2, storage.calls.size());
                assertEquals(0, storage.calls.get(1).start);
                storage.complete(1, object(0, 1));
                nextLoad.join().release();
            });
            ByteBuf object = object(0, 3);
            storage.complete(0, object);
            callback.join();
            first.join().release();
            read(cache, storage, "a", 0, 2).join().release();
            assertEquals(2, storage.calls.size());
            assertEquals(0, object.refCnt());
        }
    }

    /** Rejected admission falls back once instead of recursively loading the entire object again. */
    @Test
    public void testRetryAfterRejectedAdmission() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = new ObjectWALReadCache(0, 1, Ticker.systemTicker())) {
            CompletableFuture<ByteBuf> result = read(cache, storage, "a", 0, 0);
            ByteBuf object = object(0, 1);
            storage.complete(0, object);
            assertEquals(0, object.refCnt());
            assertEquals(2, storage.calls.size());
            assertEquals(HEADER, storage.calls.get(1).start);
            assertEquals(HEADER + RECORD_SIZE, storage.calls.get(1).end);
            storage.complete(1, Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE));
            result.join().release();
            assertEquals(2, storage.calls.size());
        }
    }

    /** Concurrent duplicate waiters claim a cached range only once regardless of callback notification order. */
    @Test
    public void testCoalescedDuplicateWaiters() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = cache()) {
            CompletableFuture<ByteBuf> first = read(cache, storage, "a", 0, 0);
            CompletableFuture<ByteBuf> second = read(cache, storage, "a", 0, 0);
            storage.complete(0, object(0, 2));
            assertTrue(first.isDone() ^ second.isDone());
            CompletableFuture<ByteBuf> claimed = first.isDone() ? first : second;
            CompletableFuture<ByteBuf> fallback = first.isDone() ? second : first;
            ByteBuf slice = claimed.join();
            slice.setInt(RECORD_SIZE - 4, 99);
            assertEquals(2, storage.calls.size());
            assertEquals(HEADER, storage.calls.get(1).start);
            storage.complete(1, Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE));
            ByteBuf duplicate = fallback.join();
            assertEquals(0, duplicate.getInt(RECORD_SIZE - 4));
            duplicate.release();
            slice.release();
        }
    }

    /** A failure callback can retry immediately without joining the failed load or losing its slot. */
    @Test
    public void testFailureCallbackCanRetry() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = new ObjectWALReadCache(1024, 1, Ticker.systemTicker())) {
            CompletableFuture<ByteBuf> retry = read(cache, storage, "a", 0, 0)
                .exceptionallyCompose(ex -> read(cache, storage, "a", 0, 0));
            storage.calls.get(0).future.completeExceptionally(new IllegalStateException("failed"));
            assertEquals(2, storage.calls.size());
            assertEquals(HEADER, storage.calls.get(1).start);
            storage.calls.get(1).future.completeExceptionally(new IllegalStateException("range failed"));
            assertEquals(3, storage.calls.size());
            assertEquals(0, storage.calls.get(2).start);
            storage.complete(2, object(0, 1));
            retry.join().release();
        }
    }

    /** A failed full load gets one range attempt, and callers receive the range failure if that also fails. */
    @Test
    public void testRangeFailureIsPropagated() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = cache()) {
            CompletableFuture<ByteBuf> result = read(cache, storage, "a", 0, 0);
            storage.calls.get(0).future.completeExceptionally(new IllegalStateException("full load failed"));
            assertEquals(2, storage.calls.size());
            assertEquals(HEADER, storage.calls.get(1).start);
            IllegalArgumentException rangeFailure = new IllegalArgumentException("range failed");
            storage.calls.get(1).future.completeExceptionally(rangeFailure);
            assertSame(rangeFailure, assertThrows(CompletionException.class, result::join).getCause());
            assertEquals(2, storage.calls.size());
        }
    }

    /** A synchronously throwing storage call releases its slot and allows a subsequent full load. */
    @Test
    public void testSynchronousLoadFailure() {
        ObjectStorage broken = mock(ObjectStorage.class);
        when(broken.rangeRead(any(), anyString(), anyLong(), anyLong())).thenThrow(new IllegalStateException("failed"));
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = new ObjectWALReadCache(1024, 1, Ticker.systemTicker())) {
            assertThrows(CompletionException.class, () -> cache.read(broken, new ReadOptions(), "a", HEADER, HEADER + RECORD_SIZE).join());
            CompletableFuture<ByteBuf> next = read(cache, storage, "b", 0, 0);
            assertEquals(0, storage.calls.get(0).start);
            storage.complete(0, object(0, 1));
            next.join().release();
        }
    }

    /** Saturated overlapping ranges are isolated even if storage merges them into shared response bytes. */
    @Test
    public void testSaturatedOverlapsAreIsolated() {
        Storage storage = new Storage();
        try (ObjectWALReadCache cache = new ObjectWALReadCache(1024, 1, Ticker.systemTicker())) {
            CompletableFuture<ByteBuf> loading = read(cache, storage, "a", 0, 0);
            CompletableFuture<ByteBuf> first = read(cache, storage, "b", 0, 0);
            CompletableFuture<ByteBuf> second = read(cache, storage, "b", 0, 0);
            ByteBuf shared = Unpooled.buffer(RECORD_SIZE).writeZero(RECORD_SIZE);
            storage.complete(1, shared.retainedDuplicate());
            storage.complete(2, shared.retainedDuplicate());
            ByteBuf firstBuf = first.join();
            ByteBuf secondBuf = second.join();
            firstBuf.setInt(RECORD_SIZE - 4, 99);
            assertEquals(0, secondBuf.getInt(RECORD_SIZE - 4));
            assertEquals(1, shared.refCnt());
            shared.release();
            firstBuf.release();
            secondBuf.release();

            storage.complete(0, object(0, 1));
            loading.join().release();
        }
    }

    /** Real readers share objects and preserve purpose across full/range reads and legacy layout fallback. */
    @Test
    public void testReaderIntegrationAndLayoutFallback() throws Exception {
        Storage storage = new Storage();
        ObjectWALConfig config = ObjectWALConfig.builder().withClusterId("cluster").withNodeId(7)
            .withEpoch(42).withType("rc").withVersion(1).withSource(S3RequestSource.ROUTER_WAL).build();
        try (ObjectWALReadCache cache = cache()) {
            DefaultReader firstReader = new DefaultReader(storage.storage, config, Time.SYSTEM, cache);
            DefaultReader secondReader = new DefaultReader(storage.storage, config, Time.SYSTEM, cache);
            CompletableFuture<StreamRecordBatch> first = S3ReadPurposeContext.withPurpose(S3ReadPurpose.ROUTER_PRODUCE,
                () -> firstReader.get(DefaultRecordOffset.of(42, 0, RECORD_SIZE)));
            Pending sharded = storage.next(0);
            assertTrue(sharded.path.contains("/s00/"));
            sharded.future.completeExceptionally(new ObjectNotExistException("missing"));
            Pending shardedRange = storage.next(1);
            assertTrue(shardedRange.path.contains("/s00/"));
            assertEquals(HEADER, shardedRange.start);
            shardedRange.future.completeExceptionally(new ObjectNotExistException("missing"));
            Pending legacy = storage.next(2);
            assertFalse(legacy.path.contains("/s00/"));
            storage.complete(2, object(0, 2));
            first.get(5, TimeUnit.SECONDS).release();
            storage.calls.subList(0, 3).forEach(call -> {
                assertEquals(S3RequestSource.ROUTER_WAL, call.options.source());
                assertEquals(S3ReadPurpose.ROUTER_PRODUCE, call.options.readPurpose());
            });
            CompletableFuture<StreamRecordBatch> second = S3ReadPurposeContext.withPurpose(S3ReadPurpose.SNAPSHOT_REPLAY,
                () -> secondReader.get(DefaultRecordOffset.of(42, RECORD_SIZE, RECORD_SIZE)));
            // The missing preferred path is retried before consulting the cached legacy path.
            storage.next(3).future.completeExceptionally(new ObjectNotExistException("missing"));
            storage.next(4).future.completeExceptionally(new ObjectNotExistException("missing"));
            StreamRecordBatch record = second.get(5, TimeUnit.SECONDS);
            assertEquals(1, record.getPayload().getInt(0));
            record.release();
            assertEquals(5, storage.calls.size());
            storage.calls.subList(3, 5).forEach(call -> {
                assertEquals(S3RequestSource.ROUTER_WAL, call.options.source());
                assertEquals(S3ReadPurpose.SNAPSHOT_REPLAY, call.options.readPurpose());
            });
        }
    }

    private static ObjectWALReadCache cache() {
        return new ObjectWALReadCache(1024 * 1024, 2, Ticker.systemTicker());
    }

    private static CompletableFuture<ByteBuf> read(ObjectWALReadCache cache, Storage storage, String path, int trims, int index) {
        long start = HEADER + (long) trims * TRIM_SIZE + (long) index * RECORD_SIZE;
        return cache.read(storage.storage, new ReadOptions(), path, start, start + RECORD_SIZE);
    }

    private static ByteBuf object(int trims, int records) {
        ByteBuf result = Unpooled.buffer(HEADER + trims * TRIM_SIZE + records * RECORD_SIZE);
        ByteBuf header = new WALObjectHeader(0, trims * TRIM_SIZE + records * RECORD_SIZE, 0, 7, 42, -1).marshal();
        result.writeBytes(header);
        header.release();
        for (int i = 0; i < trims + records; i++) {
            boolean trim = i < trims;
            StreamRecordBatch batch = StreamRecordBatch.of(trim ? -1 : 7, trim ? -1 : 0, trim ? 0 : i - trims,
                trim ? 0 : 1, trim ? Unpooled.EMPTY_BUFFER : Unpooled.buffer(4).writeInt(i - trims), DefaultByteBufSupplier.INSTANCE);
            ByteBuf recordHeader = WALUtil.generateHeader(batch.encoded(), Unpooled.buffer(RecordHeader.RECORD_HEADER_SIZE), 0,
                result.writerIndex() - HEADER);
            result.writeBytes(recordHeader);
            result.writeBytes(batch.encoded());
            recordHeader.release();
            batch.release();
        }
        return result;
    }

    private static class Storage {
        final ObjectStorage storage = mock(ObjectStorage.class);
        final List<Pending> calls = new ArrayList<>();

        Storage() {
            when(storage.rangeRead(any(), anyString(), anyLong(), anyLong())).thenAnswer(invocation -> {
                Pending pending = new Pending(invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2), invocation.getArgument(3));
                synchronized (calls) {
                    calls.add(pending);
                    calls.notifyAll();
                }
                return pending.future;
            });
        }

        Pending next(int index) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            synchronized (calls) {
                while (calls.size() <= index && System.nanoTime() < deadline) {
                    calls.wait(100);
                }
                assertTrue(calls.size() > index, "Expected storage request " + index);
                return calls.get(index);
            }
        }

        void complete(int index, ByteBuf buf) {
            calls.get(index).future.complete(buf);
        }
    }

    private static class Pending {
        final ReadOptions options;
        final String path;
        final long start;
        final long end;
        final CompletableFuture<ByteBuf> future = new CompletableFuture<>();

        Pending(ReadOptions options, String path, long start, long end) {
            this.options = options;
            this.path = path;
            this.start = start;
            this.end = end;
        }
    }
}
