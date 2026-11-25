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

import com.automq.stream.RecyclingByteBufSeqAlloc;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class AsyncStreamRecordBatchDiskCacheTest {
    @TempDir
    Path directory;
    private final List<AsyncStreamRecordBatchDiskCache> caches = new ArrayList<>();

    @AfterEach
    public void closeCaches() {
        caches.forEach(AsyncStreamRecordBatchDiskCache::close);
    }

    /**
     * Holds a write on a worker: the queued write keeps its own copy, so the caller's buffer is freed
     * as soon as the caller releases it. The index remains empty until the disk copy completes.
     * A later bulk read returns independent batches in order.
     */
    @Test
    public void testWritePublicationAndOwnership() throws Exception {
        FileCache fileCache = spy(fileCache(256));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicReference<Thread> writer = new AtomicReference<>();
        doAnswer(invocation -> {
            writer.set(Thread.currentThread());
            entered.countDown();
            await(proceed);
            return invocation.callRealMethod();
        }).when(fileCache).tryPut(anyLong(), anyLong(), any(ByteBuf.class));
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(10);
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, index, 4, 1024);
        StreamRecordBatch first = batch(0, 1);
        StreamRecordBatch second = batch(1, 2);
        ByteBuf firstBuffer = first.encoded();
        boolean callerReleased = false;
        byte[] expected = ByteBufUtil.getBytes(first.encoded());
        try {
            CompletableFuture<Boolean> write = cache.put(first);
            await(entered);
            assertNotSame(Thread.currentThread(), writer.get());
            assertFalse(write.isDone());
            assertEquals(0, index.size());
            CompletableFuture<List<StreamRecordBatch>> pendingRead = cache.get(17, 0, 1, 100);
            assertFalse(pendingRead.isDone());
            assertEquals(1, first.encoded().refCnt());
            first.release();
            callerReleased = true;
            assertEquals(0, firstBuffer.refCnt());
            proceed.countDown();
            assertTrue(result(write));
            result(pendingRead).forEach(StreamRecordBatch::release);
            assertEquals(1, index.size());
            assertTrue(result(cache.put(second)));
            List<StreamRecordBatch> read = result(cache.get(17, 0, 2, 100));
            try {
                assertEquals(2, read.size());
                assertEquals(0, read.get(0).getBaseOffset());
                assertEquals(1, read.get(1).getBaseOffset());
                assertArrayEquals(expected, ByteBufUtil.getBytes(read.get(0).encoded()));
                assertEquals(1, read.get(0).encoded().refCnt());
            } finally {
                read.forEach(StreamRecordBatch::release);
            }
        } finally {
            proceed.countDown();
            if (!callerReleased) {
                first.release();
            }
            second.release();
        }
    }

    /**
     * Disabled and undersized storage reject a write without publishing any index entries.
     */
    @Test
    public void testSkippedStorageIsNotIndexed() throws Exception {
        StreamRecordBatch source = batch(0, 1);
        try {
            for (long capacity : new long[] {0, 8}) {
                FileCache fileCache = fileCache(capacity);
                StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(10);
                AsyncStreamRecordBatchDiskCache cache = cache(fileCache, index, 1, 1024);
                assertFalse(result(cache.put(source)));
                assertEquals(0, index.size());
                assertEquals(1, source.encoded().refCnt());
                assertTrue(result(cache.get(17, 0, 1, 100)).isEmpty());
            }
        } finally {
            source.release();
        }
    }

    /**
     * A full task queue rejects without caller-thread I/O, and queued writes hold no reference to the
     * callers' buffers. Cancelling a queued write promptly frees its queue slot for a replacement task.
     */
    @Test
    public void testQueueRejectionAndQueuedCancellation() throws Exception {
        FileCache fileCache = mock(FileCache.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        when(fileCache.tryPut(anyLong(), anyLong(), any(ByteBuf.class))).thenAnswer(invocation -> {
            entered.countDown();
            await(proceed);
            return true;
        });
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, new StreamRecordBatchDiskIndex(10), 1, 1024);
        StreamRecordBatch first = batch(0, 1);
        StreamRecordBatch second = batch(1, 2);
        StreamRecordBatch third = batch(2, 3);
        try {
            CompletableFuture<Boolean> running = cache.put(first);
            await(entered);
            CompletableFuture<Boolean> queued = cache.put(second);
            assertEquals(1, second.encoded().refCnt());
            assertFalse(result(cache.put(third)));
            assertEquals(1, third.encoded().refCnt());
            verify(fileCache, times(1)).tryPut(anyLong(), anyLong(), any(ByteBuf.class));
            assertTrue(queued.cancel(false));
            assertEquals(1, second.encoded().refCnt());
            CompletableFuture<Boolean> replacement = cache.put(third);
            assertFalse(replacement.isDone());
            proceed.countDown();
            assertTrue(result(running));
            assertTrue(result(replacement));
            assertEquals(1, first.encoded().refCnt());
            assertEquals(1, third.encoded().refCnt());
            verify(fileCache, times(2)).tryPut(anyLong(), anyLong(), any(ByteBuf.class));
        } finally {
            proceed.countDown();
            first.release();
            second.release();
            third.release();
        }
    }

    /**
     * Queued and running writes both count against the encoded-byte budget. A rejected task
     * retains nothing, and completed writes make the budget available again.
     */
    @Test
    public void testByteBudget() throws Exception {
        FileCache fileCache = mock(FileCache.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        when(fileCache.tryPut(anyLong(), anyLong(), any(ByteBuf.class))).thenAnswer(invocation -> {
            entered.countDown();
            await(proceed);
            return true;
        });
        StreamRecordBatch first = batch(0, 1);
        StreamRecordBatch second = batch(1, 2);
        StreamRecordBatch third = batch(2, 3);
        long limit = 2L * first.encoded().readableBytes();
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, new StreamRecordBatchDiskIndex(10), 4, limit);
        try {
            CompletableFuture<Boolean> running = cache.put(first);
            await(entered);
            CompletableFuture<Boolean> queued = cache.put(second);
            assertFalse(result(cache.put(third)));
            assertEquals(1, third.encoded().refCnt());
            proceed.countDown();
            assertTrue(result(running));
            assertTrue(result(queued));
            assertTrue(result(cache.put(third)));
        } finally {
            proceed.countDown();
            first.release();
            second.release();
            third.release();
        }
    }

    /**
     * Read work shares the encoded-byte budget, preventing a bulk result from copying more
     * bytes than the asynchronous owner allows even when each write fits individually.
     */
    @Test
    public void testReadBudget() throws Exception {
        FileCache fileCache = spy(fileCache(256));
        StreamRecordBatch first = batch(0, 1);
        StreamRecordBatch second = batch(1, 2);
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, new StreamRecordBatchDiskIndex(10), 4,
            first.encoded().readableBytes());
        try {
            assertTrue(result(cache.put(first)));
            assertTrue(result(cache.put(second)));
            assertTrue(result(cache.get(17, 0, 2, 100)).isEmpty());
            verify(fileCache, times(0)).get(anyList());
            List<StreamRecordBatch> read = result(cache.get(17, 0, 1, 100));
            assertEquals(1, read.size());
            read.forEach(StreamRecordBatch::release);
        } finally {
            first.release();
            second.release();
        }
    }

    /**
     * A held index lock cannot block the caller or delay RAM fallback beyond the disk deadline.
     * After cancellation, the worker skips disk copying when the index lock becomes available.
     */
    @Test
    public void testDeadlineIncludesIndexLockWait() throws Exception {
        FileCache fileCache = spy(fileCache(256));
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(10);
        AsyncStreamRecordBatchDiskCache cache = spy(cache(fileCache, index, 4, 1024));
        AtomicReference<CompletableFuture<List<StreamRecordBatch>>> diskRead = new AtomicReference<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<List<StreamRecordBatch>> read =
                (CompletableFuture<List<StreamRecordBatch>>) invocation.callRealMethod();
            diskRead.set(read);
            return read;
        }).when(cache).get(17, 0, 1, 100);
        LogCache logCache = new LogCache(1024, 1024, cache, 20);
        StreamRecordBatch source = batch(0, 1);
        StreamRecordBatch ram = batch(1, 2);
        try {
            assertTrue(result(cache.put(source)));
            assertTrue(logCache.put(ram));
            synchronized (index) {
                List<StreamRecordBatch> records = logCache.getAsync(TraceContext.DEFAULT, 17, 0, 2, 100)
                    .get(5, TimeUnit.SECONDS);
                assertEquals(List.of(ram), records);
                records.forEach(StreamRecordBatch::release);
                assertThrows(CancellationException.class, () -> diskRead.get().get(5, TimeUnit.SECONDS));
                verify(fileCache, times(0)).get(anyList());
            }
            // A following task waits for the cancelled lookup's cleanup on the single worker.
            assertTrue(result(cache.get(17, 10, 11, 100)).isEmpty());
            verify(fileCache, times(0)).get(anyList());
            assertTrue(result(cache.put(source)));
            assertEquals(1, ram.encoded().refCnt());
        } finally {
            source.release();
            ram.release();
        }
    }

    /**
     * A stale bulk read removes every reported missing entry, while preserving unrelated live
     * entries. An uncached prefix misses without disk work, while the live range remains readable.
     */
    @Test
    public void testStaleRangeCleanup() throws Exception {
        FileCache fileCache = spy(fileCache(80));
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(10);
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, index, 4, 1024);
        List<StreamRecordBatch> sources = List.of(batch(0, 1), batch(1, 2), batch(2, 3), batch(3, 4));
        try {
            for (StreamRecordBatch source : sources) {
                assertTrue(result(cache.put(source)));
            }
            assertEquals(4, index.size());
            assertTrue(result(cache.get(17, 0, 4, 100)).isEmpty());
            assertEquals(2, index.size());
            assertTrue(result(cache.get(17, 0, 2, 100)).isEmpty());
            verify(fileCache, times(1)).get(anyList());
            assertTrue(result(cache.get(17, 0, 4, 100)).isEmpty());
            List<StreamRecordBatch> live = result(cache.get(17, 2, 4, 100));
            verify(fileCache, times(2)).get(anyList());
            try {
                assertEquals(2, live.size());
                assertEquals(2, live.get(0).getBaseOffset());
                assertEquals(3, live.get(1).getBaseOffset());
            } finally {
                live.forEach(StreamRecordBatch::release);
            }
        } finally {
            sources.forEach(StreamRecordBatch::release);
        }
    }

    /**
     * Runtime write and read failures become rejection or miss, release input ownership and
     * budget, and leave the owner available for subsequent successful cache operations.
     */
    @Test
    public void testRuntimeFailures() throws Exception {
        FileCache fileCache = spy(fileCache(128));
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(10);
        StreamRecordBatch source = batch(0, 1);
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, index, 1, source.encoded().readableBytes());
        doAnswer(invocation -> {
            throw new IllegalStateException("write failed");
        }).doCallRealMethod().when(fileCache).tryPut(anyLong(), anyLong(), any(ByteBuf.class));
        try {
            assertFalse(result(cache.put(source)));
            assertEquals(0, index.size());
            assertEquals(1, source.encoded().refCnt());
            assertTrue(result(cache.put(source)));
            doAnswer(invocation -> {
                throw new IllegalStateException("read failed");
            }).doCallRealMethod().when(fileCache).get(anyList());
            assertTrue(result(cache.get(17, 0, 1, 100)).isEmpty());
            List<StreamRecordBatch> read = result(cache.get(17, 0, 1, 100));
            assertEquals(1, read.size());
            read.forEach(StreamRecordBatch::release);
        } finally {
            source.release();
        }
    }

    /**
     * Cancelling a running read releases its eventual buffers; a following task proves the
     * cancelled task has finished its cleanup before the reference count is checked.
     */
    @Test
    public void testRunningReadCancellation() throws Exception {
        FileCache fileCache = spy(fileCache(128));
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, new StreamRecordBatchDiskIndex(10), 4, 1024);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicReference<ByteBuf> copied = new AtomicReference<>();
        StreamRecordBatch source = batch(0, 1);
        try {
            assertTrue(result(cache.put(source)));
            doAnswer(invocation -> {
                FileCache.BulkReadResult read = (FileCache.BulkReadResult) invocation.callRealMethod();
                copied.set(read.buffers().get(0));
                entered.countDown();
                await(proceed);
                return read;
            }).when(fileCache).get(anyList());
            CompletableFuture<List<StreamRecordBatch>> read = cache.get(17, 0, 1, 100);
            await(entered);
            assertTrue(read.cancel(false));
            proceed.countDown();
            assertTrue(result(cache.put(source)));
            assertEquals(0, copied.get().refCnt());
        } finally {
            proceed.countDown();
            source.release();
        }
    }

    /**
     * Shutdown discards queued reads and writes, releases retained write inputs, and rejects
     * subsequent operations. An interrupted running write releases its input on exit.
     */
    @Test
    public void testCloseDiscardsQueuedWork() throws Exception {
        FileCache fileCache = spy(fileCache(256));
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(10);
        AsyncStreamRecordBatchDiskCache cache = cache(fileCache, index, 4, 1024);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        StreamRecordBatch first = batch(0, 1);
        StreamRecordBatch second = batch(1, 2);
        try {
            assertTrue(result(cache.put(first)));
            doAnswer(invocation -> {
                entered.countDown();
                try {
                    new CountDownLatch(1).await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                exited.countDown();
                return invocation.callRealMethod();
            }).when(fileCache).tryPut(anyLong(), anyLong(), any(ByteBuf.class));
            CompletableFuture<Boolean> running = cache.put(first);
            await(entered);
            CompletableFuture<Boolean> queued = cache.put(second);
            CompletableFuture<List<StreamRecordBatch>> queuedRead = cache.get(17, 0, 1, 100);
            cache.close();
            assertFalse(result(queued));
            assertTrue(result(queuedRead).isEmpty());
            assertEquals(1, second.encoded().refCnt());
            await(exited);
            assertFalse(result(running));
            assertEquals(1, first.encoded().refCnt());
            assertEquals(1, index.size());
            assertFalse(result(cache.put(second)));
            assertTrue(result(cache.get(17, 0, 1, 100)).isEmpty());
            assertEquals(1, second.encoded().refCnt());
            cache.close();
        } finally {
            cache.close();
            first.release();
            second.release();
        }
    }

    /**
     * Given RAM batches from many streams interleaved in shared allocator slabs and a stalled writer,
     * when capacity eviction spills them under a small write budget, then queued writes pin no RAM
     * slabs after the cache releases its records, and admitted batches are still stored intact.
     */
    @Test
    public void testQueuedWritesDoNotPinRamSlabs() throws Exception {
        FileCache fileCache = spy(fileCache(1 << 20));
        CountDownLatch proceed = new CountDownLatch(1);
        doAnswer(invocation -> {
            await(proceed);
            return invocation.callRealMethod();
        }).when(fileCache).tryPut(anyLong(), anyLong(), any(ByteBuf.class));
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(100_000);
        long budget = 64 << 10;
        AsyncStreamRecordBatchDiskCache cache = spy(cache(fileCache, index, 1024, budget));
        List<CompletableFuture<Boolean>> writes = Collections.synchronizedList(new ArrayList<>());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<Boolean> write = (CompletableFuture<Boolean>) invocation.callRealMethod();
            writes.add(write);
            return write;
        }).when(cache).put(any());
        CompletableFuture<LogCache.LogCacheBlock> spilled = new CompletableFuture<>();
        LogCache logCache = new LogCache(0, Long.MAX_VALUE, 10_000, spilled::complete, cache);
        Set<ByteBuf> slabs = Collections.newSetFromMap(new IdentityHashMap<>());
        try (RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.SNAPSHOT_READ_CACHE, 1)) {
            // Replay order interleaves streams, so every stream has batches in every slab.
            for (long offset = 0; offset < 512; offset++) {
                for (long streamId = 0; streamId < 64; streamId++) {
                    StreamRecordBatch source = StreamRecordBatch.of(streamId, 0, offset, 1,
                        Unpooled.wrappedBuffer(payload(streamId, offset)), DefaultByteBufSupplier.INSTANCE);
                    StreamRecordBatch copy = StreamRecordBatch.parse(source.encoded(), true, alloc);
                    source.release();
                    slabs.add(copy.encoded().unwrap());
                    assertTrue(logCache.put(copy));
                }
            }
            assertTrue(slabs.size() > 1);
            logCache.markFree(logCache.archiveCurrentBlock()).get(5, TimeUnit.SECONDS);
            spilled.get(5, TimeUnit.SECONDS);

            long pinnedBytes = slabs.stream().filter(slab -> slab.refCnt() > 1).mapToLong(ByteBuf::capacity).sum();
            assertEquals(0, pinnedBytes, "queued writes pin RAM slabs beyond the " + budget + "-byte budget");

            proceed.countDown();
            long stored = 0;
            for (CompletableFuture<Boolean> write : writes) {
                stored += result(write) ? 1 : 0;
            }
            assertTrue(stored > 1 && stored < writes.size(), "the budget should admit some, not all, spilled writes");
            assertEquals(stored, index.size());
            // Spill visits streams in ascending ID order here, so stream 0 is admitted first.
            List<StreamRecordBatch> read = result(cache.get(0, 0, 1, 100));
            try {
                assertEquals(1, read.size());
                assertArrayEquals(payload(0, 0), ByteBufUtil.getBytes(read.get(0).getPayload()));
            } finally {
                read.forEach(StreamRecordBatch::release);
            }
        } finally {
            proceed.countDown();
        }
    }

    private static byte[] payload(long streamId, long offset) {
        byte[] payload = new byte[1024];
        java.util.Arrays.fill(payload, (byte) (streamId * 31 + offset));
        return payload;
    }

    private FileCache fileCache(long capacity) throws IOException {
        return new FileCache(directory.resolve("cache-" + capacity).toString(), capacity, 8, 16);
    }

    private AsyncStreamRecordBatchDiskCache cache(FileCache fileCache, StreamRecordBatchDiskIndex index,
        int queueCapacity, long maxPendingBytes) {
        AsyncStreamRecordBatchDiskCache cache = new AsyncStreamRecordBatchDiskCache(
            new StreamRecordBatchDiskCache(fileCache), index, 1, queueCapacity, maxPendingBytes);
        caches.add(cache);
        return cache;
    }

    private StreamRecordBatch batch(long offset, int value) {
        return StreamRecordBatch.of(17, 2, offset, 1, Unpooled.wrappedBuffer(new byte[] {(byte) value}),
            DefaultByteBufSupplier.INSTANCE);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for cache worker");
    }

    private static <T> T result(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }
}
