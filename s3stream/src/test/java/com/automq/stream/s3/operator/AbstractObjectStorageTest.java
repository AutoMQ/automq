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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.test.RecordTestNetworkBandwidthLimiter;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.operator.ObjectStorageTestUtils.spyStorage;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
@Timeout(10)
class AbstractObjectStorageTest {

    AbstractObjectStorage objectStorage;

    @BeforeEach
    public void setUp() {
        objectStorage = new MemoryObjectStorage(false);
    }

    @AfterEach
    public void tearDown() {
        objectStorage.close();
    }

    /** A fresh read bypasses manual batching and an older GET that has already delivered progress. */
    @Test
    void testFreshReadBypassesProgressiveInflightRead() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(true));
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        CompletableFuture<ReadProgressListener> listener = new CompletableFuture<>();
        AtomicInteger gets = new AtomicInteger();
        doAnswer(invocation -> {
            if (gets.incrementAndGet() == 1) {
                listener.complete(invocation.getArgument(4));
                return original;
            }
            return CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[] {2}));
        }).when(objectStorage).doRangeRead(any(), eq("mutable"), eq(0L), eq(1L), any());

        CompletableFuture<ByteBuf> first = objectStorage.rangeRead(new ReadOptions(), "mutable", 0, 1);
        objectStorage.readAccumulator.flush().join();
        ByteBuf oldData = Unpooled.wrappedBuffer(new byte[] {1});
        try {
            listener.get().onChange(List.of(oldData), 1);
            ByteBuf firstResult = first.get();
            assertEquals(1, firstResult.readByte());
            firstResult.release();
            assertFalse(original.isDone());

            // No flush: the fresh request must start its own GET and observe the overwritten value.
            ByteBuf fresh = objectStorage.rangeRead(new ReadOptions().freshRead(true), "mutable", 0, 1).get();
            assertEquals(2, fresh.readByte());
            fresh.release();
            assertEquals(2, gets.get());
        } finally {
            original.complete(oldData);
        }
        await().untilAsserted(() -> assertEquals(0, oldData.refCnt()));
    }

    @Test
    void testMergeRead() throws ExecutionException, InterruptedException {
        S3ObjectMetadata s3ObjectMetadata1 = new S3ObjectMetadata(1, 33554944, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata2 = new S3ObjectMetadata(2, 3072, S3ObjectType.STREAM);
        objectStorage = new MemoryObjectStorage(true);
        objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata1.key(), TestUtils.random((int) s3ObjectMetadata1.objectSize())).get();
        objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata2.key(), TestUtils.random((int) s3ObjectMetadata2.objectSize())).get();

        objectStorage = spyStorage(objectStorage);
        // obj0_0_1024 obj_1_1024_2048 obj_0_16776192_16777216 obj_0_2048_4096 obj_0_16777216_16778240
        CompletableFuture<ByteBuf> cf1 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 0, 1024);
        CompletableFuture<ByteBuf> cf2 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata2.key(), 1024, 3072);
        CompletableFuture<ByteBuf> cf3 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 31457280, 31461376);
        CompletableFuture<ByteBuf> cf4 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 2048, 4096);
        CompletableFuture<ByteBuf> cf5 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 33554432, 33554944);

        objectStorage.readAccumulator.flush().join();

        verify(objectStorage, timeout(1000L).times(1)).doRangeRead(any(), eq(s3ObjectMetadata1.key()), eq(0L), eq(4096L));
        verify(objectStorage, timeout(1000L).times(1)).doRangeRead(any(), eq(s3ObjectMetadata2.key()), eq(1024L), eq(3072L));
        verify(objectStorage, timeout(1000L).times(1)).doRangeRead(any(), eq(s3ObjectMetadata1.key()), eq(31457280L), eq(31461376L));
        verify(objectStorage, timeout(1000L).times(1)).doRangeRead(any(), eq(s3ObjectMetadata1.key()), eq(33554432L), eq(33554944L));

        ByteBuf buf = cf1.get();
        assertEquals(1024, buf.readableBytes());
        buf.release();
        buf = cf2.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
        buf = cf3.get();
        assertEquals(4096, buf.readableBytes());
        buf.release();
        buf = cf4.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
        buf = cf5.get();
        assertEquals(512, buf.readableBytes());
        buf.release();
    }

    @Test
    void testHandleReadCompleted() throws Throwable {
        ByteBuf data = TestUtils.random(4096);
        CompletableFuture<ByteBuf> readToEndCf = new CompletableFuture<>();
        CompletableFuture<ByteBuf> readRangeCf = new CompletableFuture<>();
        ReadRequestAccumulator.MergedReadTask.handleReadCompleted(List.of(
            new ReadRequestAccumulator.ReadTask(new ReadOptions(), "fake", 3000, -1, readToEndCf),
            new ReadRequestAccumulator.ReadTask(new ReadOptions(), "fake", 2000, 4096, readRangeCf)
        ), 2000, data.slice(2000, 4096 - 2000), null);
        assertEquals(data.slice(3000, 4096 - 3000), readToEndCf.get());
        assertEquals(data.slice(2000, 4096 - 2000), readRangeCf.get());
        readToEndCf.get().release();
        readRangeCf.get().release();
    }

    /** Given an outstanding physical GET, a covered range shares its response without another storage GET. */
    @Test
    void testInflightReadReuse() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(true));
        CompletableFuture<ByteBuf> physical = new CompletableFuture<>();
        doReturn(physical).when(objectStorage).doRangeRead(any(), eq("key"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> parent = objectStorage.rangeRead(new ReadOptions(), "key", 100, 200);
        objectStorage.readAccumulator.flush().join();
        awaitReadDispatch();
        CompletableFuture<ByteBuf> child = objectStorage.rangeRead(new ReadOptions(), "key", 120, 140);
        objectStorage.readAccumulator.flush().join();
        verify(objectStorage, times(1)).doRangeRead(any(), eq("key"), eq(100L), eq(200L));
        ByteBuf data = Unpooled.buffer(100).writeZero(100);
        physical.complete(data);
        assertEquals(100, parent.get().readableBytes());
        assertEquals(20, child.get().readableBytes());
        parent.get().release();
        child.get().release();
        // Caller delivery may precede the queued full-GET cleanup.
        await().untilAsserted(() -> assertEquals(0, data.refCnt()));
    }

    /** Given early progress, children return before the physical GET releases its concurrency permit. */
    @Test
    void testProgressKeepsPhysicalReadPermitUntilCompletion() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(1));
        AtomicReference<ReadProgressListener> progress = new AtomicReference<>();
        CompletableFuture<ByteBuf> physical = new CompletableFuture<>();
        doAnswer(invocation -> {
            progress.set(invocation.getArgument(4));
            return physical;
        }).when(objectStorage).doRangeRead(any(), eq("first"), anyLong(), anyLong(), any());
        CompletableFuture<ByteBuf> parent = objectStorage.rangeRead(new ReadOptions(), "first", 0, 100);
        objectStorage.readAccumulator.flush().join();
        awaitReadDispatch();
        CompletableFuture<ByteBuf> child = objectStorage.rangeRead(new ReadOptions(), "first", 10, 20);
        ReadRequestAccumulator.EVENT_LOOP.submit(() -> { }).join();
        ByteBuf prefix = Unpooled.buffer(20).writeZero(20);
        progress.get().onChange(List.of(prefix), prefix.readableBytes());
        child.get(1, TimeUnit.SECONDS).release();
        assertFalse(parent.isDone());
        CompletableFuture<ByteBuf> late = objectStorage.rangeRead(new ReadOptions(), "first", 5, 15);
        ReadRequestAccumulator.EVENT_LOOP.submit(() -> { }).join();
        assertFalse(late.isDone());
        progress.get().onChange(List.of(prefix), prefix.readableBytes());
        late.get(1, TimeUnit.SECONDS).release();
        Field limiter = AbstractObjectStorage.class.getDeclaredField("inflightReadLimiter");
        limiter.setAccessible(true);
        java.util.concurrent.Semaphore permits = (java.util.concurrent.Semaphore) limiter.get(objectStorage);
        assertEquals(0, permits.availablePermits());
        prefix.release();
        physical.complete(Unpooled.buffer(100).writeZero(100));
        parent.get(1, TimeUnit.SECONDS).release();
        await().untilAsserted(() -> assertEquals(1, permits.availablePermits()));
        await().untilAsserted(() -> assertEquals(0, prefix.refCnt()));
        verify(objectStorage, times(1)).doRangeRead(any(), eq("first"), eq(0L), eq(100L), any());
    }

    /** Given exhausted read quota, the storage reader returns immediately and resumes queued GETs in order. */
    @Test
    void testAsyncStorageReaderWaitsForQuotaWithoutBlockingCaller() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(1));
        CompletableFuture<ByteBuf> firstPhysical = new CompletableFuture<>();
        doReturn(firstPhysical).when(objectStorage).doRangeRead(any(), eq("first"), anyLong(), anyLong());
        doAnswer(invocation -> CompletableFuture.completedFuture(Unpooled.buffer(10).writeZero(10)))
            .when(objectStorage).doRangeRead(any(), eq("second"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> first = objectStorage.rangeRead0(new ReadOptions(), "first", 0, 10);
        verify(objectStorage, timeout(1000)).doRangeRead(any(), eq("first"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> second = objectStorage.rangeRead0(new ReadOptions(), "second", 0, 10);
        assertFalse(second.isDone());
        ReadRequestAccumulator.EVENT_LOOP.submit(() -> { }).get(1, TimeUnit.SECONDS);
        verify(objectStorage, times(0)).doRangeRead(any(), eq("second"), anyLong(), anyLong());
        firstPhysical.complete(Unpooled.buffer(10).writeZero(10));
        first.get(1, TimeUnit.SECONDS).release();
        second.get(1, TimeUnit.SECONDS).release();
    }

    /** Given a quota waiter and an unstarted read, close fails both without launching extra GETs or leaking quota. */
    @Test
    void testCloseSettlesQueuedStorageReads() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(1));
        CompletableFuture<ByteBuf> firstPhysical = new CompletableFuture<>();
        doReturn(firstPhysical).when(objectStorage).doRangeRead(any(), eq("first"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> first = objectStorage.rangeRead0(new ReadOptions(), "first", 0, 10);
        verify(objectStorage, timeout(1000)).doRangeRead(any(), eq("first"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> second = objectStorage.rangeRead0(new ReadOptions(), "second", 0, 10);
        Field pending = AbstractObjectStorage.class.getDeclaredField("pendingReads");
        pending.setAccessible(true);
        java.util.Set<?> queued = (java.util.Set<?>) pending.get(objectStorage);
        await().until(() -> !queued.contains(second));
        CompletableFuture<ByteBuf> third = objectStorage.rangeRead0(new ReadOptions(), "third", 0, 10);
        objectStorage.close();
        assertTrue(third.isCompletedExceptionally());
        await().until(second::isCompletedExceptionally);
        firstPhysical.complete(Unpooled.buffer(10).writeZero(10));
        first.get(1, TimeUnit.SECONDS).release();
        verify(objectStorage, times(0)).doRangeRead(any(), eq("second"), anyLong(), anyLong());
        verify(objectStorage, times(0)).doRangeRead(any(), eq("third"), anyLong(), anyLong());
        Field limiter = AbstractObjectStorage.class.getDeclaredField("inflightReadLimiter");
        limiter.setAccessible(true);
        java.util.concurrent.Semaphore permits = (java.util.concurrent.Semaphore) limiter.get(objectStorage);
        assertEquals(1, permits.availablePermits());
    }

    /** Given cancellation after a GET starts, its physical completion still releases the quota and unused response. */
    @Test
    void testCancelledStorageReadReleasesLateResponse() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(1));
        CompletableFuture<ByteBuf> physical = new CompletableFuture<>();
        doReturn(physical).when(objectStorage).doRangeRead(any(), eq("key"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> result = objectStorage.rangeRead0(new ReadOptions(), "key", 0, 10);
        awaitReadDispatch();
        assertTrue(result.cancel(false));
        ByteBuf response = Unpooled.buffer(10).writeZero(10);
        physical.complete(response);
        await().untilAsserted(() -> assertEquals(0, response.refCnt()));
        Field limiter = AbstractObjectStorage.class.getDeclaredField("inflightReadLimiter");
        limiter.setAccessible(true);
        assertEquals(1, ((java.util.concurrent.Semaphore) limiter.get(objectStorage)).availablePermits());
    }

    /** Given quota-one dispatch blocks on its second GET, the first GET's retry must run independently. */
    @Test
    void testBlockedDispatchDoesNotBlockStorageRetry() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(1));
        Field field = AbstractObjectStorage.class.getDeclaredField("readExecutor");
        field.setAccessible(true);
        ExecutorService batches = (ExecutorService) field.get(objectStorage);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        batches.execute(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        AtomicInteger firstAttempts = new AtomicInteger();
        doReturn(0).when(objectStorage).retryDelay(any(), anyInt());
        doAnswer(invocation -> {
            long start = invocation.getArgument(2);
            if (start == 0 && firstAttempts.getAndIncrement() == 0) {
                return CompletableFuture.failedFuture(new TimeoutException("retry first GET"));
            }
            return CompletableFuture.completedFuture(Unpooled.buffer(10).writeZero(10));
        }).when(objectStorage).doRangeRead(any(), eq("key"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> first = objectStorage.rangeRead(new ReadOptions(), "key", 0, 10);
        CompletableFuture<ByteBuf> second = objectStorage.rangeRead(new ReadOptions(), "key", 1024 * 1024, 1024 * 1024 + 10);
        release.countDown();
        first.get(2, TimeUnit.SECONDS).release();
        second.get(2, TimeUnit.SECONDS).release();
        verify(objectStorage, times(2)).doRangeRead(any(), eq("key"), eq(0L), eq(10L));
        verify(objectStorage, times(1)).doRangeRead(any(), eq("key"), eq(1024L * 1024), eq(1024L * 1024 + 10));
    }

    /** Given storage closes before the physical GET returns, late response completion still releases its buffer. */
    @Test
    void testLateResponseAfterCloseReleasesBuffer() {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(true));
        CompletableFuture<ByteBuf> physical = new CompletableFuture<>();
        doReturn(physical).when(objectStorage).doRangeRead(any(), eq("key"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> caller = objectStorage.rangeRead(new ReadOptions(), "key", 0, 100);
        objectStorage.readAccumulator.flush().join();
        awaitReadDispatch();
        objectStorage.close();
        assertTrue(caller.isCompletedExceptionally());
        ByteBuf response = Unpooled.buffer(100).writeZero(100);
        physical.complete(response);
        await().untilAsserted(() -> assertEquals(0, response.refCnt()));
    }

    /** Given a prepared response queued behind a blocked callback, close delivers it despite executor shutdown. */
    @Test
    void testQueuedResponseDuringCloseReleasesBuffer() throws Exception {
        objectStorage.close();
        objectStorage = spyStorage(new MemoryObjectStorage(true));
        java.util.concurrent.Executor callbacks = ReadRequestAccumulator.CALLBACK_EXECUTOR;
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        callbacks.execute(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        CompletableFuture<ByteBuf> physical = new CompletableFuture<>();
        doReturn(physical).when(objectStorage).doRangeRead(any(), eq("key"), anyLong(), anyLong());
        CompletableFuture<ByteBuf> caller = objectStorage.rangeRead(new ReadOptions(), "key", 0, 100);
        caller.whenComplete((data, error) -> objectStorage.close());
        objectStorage.readAccumulator.flush().join();
        awaitReadDispatch();
        ByteBuf response = Unpooled.buffer(100).writeZero(100);
        try {
            physical.complete(response);
            assertFalse(caller.isDone());
            CompletableFuture<Void> closed = CompletableFuture.runAsync(objectStorage::close);
            Field closing = AbstractObjectStorage.class.getDeclaredField("closed");
            closing.setAccessible(true);
            await().until(() -> closing.getBoolean(objectStorage));
            release.countDown();
            closed.get(2, TimeUnit.SECONDS);
            assertEquals(100, caller.join().readableBytes());
            caller.join().release();
            assertEquals(0, response.refCnt());
            verify(objectStorage, times(1)).doClose();
        } finally {
            release.countDown();
        }
    }

    private void awaitReadDispatch() {
        try {
            Field field = AbstractObjectStorage.class.getDeclaredField("readExecutor");
            field.setAccessible(true);
            ExecutorService executor = (ExecutorService) field.get(objectStorage);
            CompletableFuture.runAsync(() -> { }, executor).join();
            ReadRequestAccumulator.EVENT_LOOP.submit(() -> { }).join();
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    @Test
    void testByteBufRefCnt() throws ExecutionException, InterruptedException {
        objectStorage = new MemoryObjectStorage(false);
        S3ObjectMetadata s3ObjectMetadata1 = new S3ObjectMetadata(1, 100, S3ObjectType.STREAM);
        objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata1.key(), TestUtils.random((int) s3ObjectMetadata1.objectSize())).get();
        objectStorage = spyStorage(objectStorage);
        objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 0, 100)
            .thenCompose(buf -> {
                assertEquals(1, buf.refCnt());
                buf.release();
                return CompletableFuture.completedFuture(null);
            }).get();
    }

    private void prepareReadFastRetry() throws Exception {
        objectStorage = spyStorage(new MemoryObjectStorage());
        Field field = AbstractObjectStorage.class.getDeclaredField("readLatencyCalculator");
        field.setAccessible(true);
        S3LatencyCalculator calculator = mock(S3LatencyCalculator.class);
        when(calculator.valueAtPercentile(anyLong(), anyLong())).thenReturn(50L);
        field.set(objectStorage, calculator);
    }

    /** Given a stalled original read, the fast retry wins and the late original buffer is released. */
    @Test
    void testReadFastRetryWins() throws Exception {
        prepareReadFastRetry();
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        ByteBuf retryBuffer = TestUtils.randomPooled(1024);
        doReturn(original, CompletableFuture.completedFuture(retryBuffer))
            .when(objectStorage).doRangeRead(any(), anyString(), anyLong(), anyLong());
        ByteBuf result = objectStorage.rangeRead0(new ReadOptions(), "key", 0, 1024)
            .get(1, TimeUnit.SECONDS);
        assertSame(retryBuffer, result);
        result.release();
        ByteBuf lateBuffer = TestUtils.randomPooled(1024);
        original.complete(lateBuffer);
        assertEquals(0, lateBuffer.refCnt());
        verify(objectStorage, times(2)).doRangeRead(any(), eq("key"), eq(0L), eq(1024L));
    }

    /** Given a fallback progress observer throws, the completed response remains owned by the caller. */
    @Test
    void testDefaultProgressListenerFailureDoesNotFailRead() {
        objectStorage = spyStorage(objectStorage);
        ByteBuf response = Unpooled.buffer(10).writeZero(10);
        doReturn(CompletableFuture.completedFuture(response))
            .when(objectStorage).doRangeRead(any(), eq("key"), anyLong(), anyLong());
        ByteBuf result = objectStorage.doRangeRead(new ReadOptions(), "key", 0, 10, (chunks, size) -> {
            throw new IllegalStateException("observer failed");
        }).join();
        assertSame(response, result);
        assertEquals(1, result.refCnt());
        result.release();
    }

    /** Given ordinary and speculative retries, every physical GET receives the same progress listener. */
    @Test
    void testProgressListenerSurvivesNormalAndFastRetries() throws Exception {
        prepareReadFastRetry();
        List<ReadProgressListener> listeners = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        CompletableFuture<ByteBuf> stalled = new CompletableFuture<>();
        ByteBuf result = Unpooled.buffer(1024).writeZero(1024);
        doReturn(0).when(objectStorage).retryDelay(any(), anyInt());
        doAnswer(invocation -> {
            listeners.add(invocation.getArgument(4));
            int attempt = attempts.getAndIncrement();
            if (attempt == 0) {
                return original;
            }
            return attempt == 1 ? stalled : CompletableFuture.completedFuture(result);
        }).when(objectStorage).doRangeRead(any(), eq("key"), anyLong(), anyLong(), any());
        ReadProgressListener listener = (chunks, size) -> { };
        CompletableFuture<ByteBuf> read = objectStorage.rangeRead0(new ReadOptions(), "key", 0, 1024, listener);
        await().untilAsserted(() -> assertEquals(2, listeners.size()));
        original.completeExceptionally(new TimeoutException("ordinary retry"));
        assertSame(result, read.get(2, TimeUnit.SECONDS));
        assertEquals(3, listeners.size());
        listeners.forEach(actual -> assertSame(listener, actual));
        result.release();
        ByteBuf late = Unpooled.buffer(1024).writeZero(1024);
        stalled.complete(late);
        assertEquals(0, late.refCnt());
    }

    /** Given both reads in flight, the original wins and the late retry buffer is released. */
    @Test
    void testReadOriginalWinsAfterFastRetryStarts() throws Exception {
        prepareReadFastRetry();
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        CompletableFuture<ByteBuf> retry = new CompletableFuture<>();
        doReturn(original, retry).when(objectStorage).doRangeRead(any(), anyString(), anyLong(), anyLong());
        CompletableFuture<ByteBuf> result = objectStorage.rangeRead0(new ReadOptions(), "key", 0, 1024);
        verify(objectStorage, timeout(1000).times(2)).doRangeRead(any(), eq("key"), eq(0L), eq(1024L));
        ByteBuf originalBuffer = TestUtils.randomPooled(1024);
        original.complete(originalBuffer);
        assertSame(originalBuffer, result.get(1, TimeUnit.SECONDS));
        originalBuffer.release();
        ByteBuf lateBuffer = TestUtils.randomPooled(1024);
        retry.complete(lateBuffer);
        assertEquals(0, lateBuffer.refCnt());
    }

    /** A failed speculative read leaves the original read pending and does not start normal retries. */
    @Test
    void testReadFastRetryFailureDoesNotFailRequest() throws Exception {
        prepareReadFastRetry();
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        CompletableFuture<ByteBuf> retry = new CompletableFuture<>();
        doReturn(original, retry).when(objectStorage).doRangeRead(any(), anyString(), anyLong(), anyLong());
        CompletableFuture<ByteBuf> result = objectStorage.rangeRead0(new ReadOptions(), "key", 0, 1024);
        verify(objectStorage, timeout(1000).times(2)).doRangeRead(any(), eq("key"), eq(0L), eq(1024L));
        retry.completeExceptionally(new TimeoutException());
        assertFalse(result.isDone());
        ByteBuf originalBuffer = TestUtils.randomPooled(1024);
        original.complete(originalBuffer);
        assertSame(originalBuffer, result.get(1, TimeUnit.SECONDS));
        originalBuffer.release();
        verify(objectStorage, times(2)).doRangeRead(any(), eq("key"), eq(0L), eq(1024L));
    }

    /** A normal retry scheduled before speculative success must not issue another network request. */
    @Test
    void testReadFastRetryStopsScheduledNormalRetry() throws Exception {
        prepareReadFastRetry();
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        CompletableFuture<ByteBuf> retry = new CompletableFuture<>();
        doReturn(original, retry).when(objectStorage).doRangeRead(any(), anyString(), anyLong(), anyLong());
        doReturn(Pair.of(RetryStrategy.RETRY, new TimeoutException()))
            .when(objectStorage).toRetryStrategyAndCause(any(), eq(S3Operation.GET_OBJECT));
        doReturn(50).when(objectStorage).retryDelay(eq(S3Operation.GET_OBJECT), anyInt());
        CompletableFuture<ByteBuf> result = objectStorage.rangeRead0(new ReadOptions(), "key", 0, 1024);
        verify(objectStorage, timeout(1000).times(2)).doRangeRead(any(), eq("key"), eq(0L), eq(1024L));
        original.completeExceptionally(new TimeoutException());
        ByteBuf retryBuffer = TestUtils.randomPooled(1024);
        retry.complete(retryBuffer);
        assertSame(retryBuffer, result.get(1, TimeUnit.SECONDS));
        retryBuffer.release();
        objectStorage.scheduler.schedule(() -> { }, 100, TimeUnit.MILLISECONDS).get(1, TimeUnit.SECONDS);
        verify(objectStorage, times(2)).doRangeRead(any(), eq("key"), eq(0L), eq(1024L));
    }

    /** An unknown read-to-end size must not schedule speculative reads. */
    @Test
    void testReadToEndSkipsFastRetry() throws Exception {
        prepareReadFastRetry();
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        doReturn(original).when(objectStorage).doRangeRead(any(), anyString(), anyLong(), anyLong());
        CompletableFuture<ByteBuf> result = objectStorage.rangeRead0(new ReadOptions(), "key", 0, -1);
        Field field = AbstractObjectStorage.class.getDeclaredField("readLatencyCalculator");
        field.setAccessible(true);
        verify((S3LatencyCalculator) field.get(objectStorage), times(0)).valueAtPercentile(anyLong(), anyLong());
        ByteBuf buf = TestUtils.randomPooled(1024);
        original.complete(buf);
        result.get(1, TimeUnit.SECONDS).release();
        verify(objectStorage, times(1)).doRangeRead(any(), eq("key"), eq(0L), eq(-1L));
    }

    @Test
    void testFastRetry() throws Throwable {
        // Initialize memory storage and spy to track method calls
        objectStorage = new MemoryObjectStorage();
        objectStorage = spyStorage(objectStorage);

        // Configure write options: enable fast retry, disable normal retry
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .enableFastRetry(true)
            .retry(false);

        // Mock S3 latency calculator via reflection to force fast retry condition
        Field latencyCalculatorField = AbstractObjectStorage.class.getDeclaredField("s3LatencyCalculator");
        latencyCalculatorField.setAccessible(true);
        S3LatencyCalculator mockCalculator = mock(S3LatencyCalculator.class);
        when(mockCalculator.valueAtPercentile(anyLong(), anyLong())).thenReturn(100L); // Force low latency to trigger fast retry
        latencyCalculatorField.set(objectStorage, mockCalculator);

        // Track doWrite() calls: first call hangs, second completes immediately
        AtomicInteger callCount = new AtomicInteger();
        CompletableFuture<Void> firstFuture = new CompletableFuture<>();
        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            int count = callCount.getAndIncrement();
            return (count == 0) ? firstFuture : CompletableFuture.completedFuture(null);
        });

        // Execute write operation
        ByteBuf data = TestUtils.randomPooled(1024);
        assertEquals(1, data.refCnt()); // Verify initial ref count

        CompletableFuture<ObjectStorage.WriteResult> writeFuture = objectStorage.write(options, "testKey", data);
        writeFuture.get(1, TimeUnit.SECONDS); // Wait for write completion

        // Verify: two calls made (initial + retry), data ref count maintained during retry
        assertEquals(1, data.refCnt());
        assertEquals(2, callCount.get());

        // Complete initial future and verify data release
        firstFuture.complete(null);
        await().atMost(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(0, data.refCnt())); // Ensure buffer released
    }

    @Test
    void testWriteRetryTimeout() throws Throwable {
        // Setup storage with 100ms timeout (clearer time unit)
        objectStorage = spyStorage(new MemoryObjectStorage());
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .retry(true)
            .timeout(1000L);

        // Mock hanging write operation
        AtomicInteger callCount = new AtomicInteger();
        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            int count = callCount.getAndIncrement();
            if (count < 12) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                Executors.newSingleThreadScheduledExecutor().schedule(
                    () -> future.completeExceptionally(new TimeoutException("Simulated timeout")),
                    100, TimeUnit.MILLISECONDS
                );
                return future;
            }
            // Second call: immediate success
            return CompletableFuture.completedFuture(null);
        });

        // Execute test
        ByteBuf data = TestUtils.randomPooled(1024);
        CompletableFuture<ObjectStorage.WriteResult> writeFuture =
            objectStorage.write(options, "testKey", data);
        // Verify timeout exception
        assertThrows(TimeoutException.class,
            () -> writeFuture.get(1, TimeUnit.SECONDS));
        // Verify resource cleanup
        await().atMost(2, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(0, data.refCnt()));
        // Verify: no successful calls made
        assertTrue(callCount.get() < 12);
    }

    @Test
    void testWritePermit() throws Exception {
        final int maxConcurrency = 5;
        objectStorage = spyStorage(new MemoryObjectStorage(maxConcurrency));

        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .enableFastRetry(false)
            .retry(false);

        // Use completable future to block first 5 calls
        CompletableFuture<Void> barrierFuture = new CompletableFuture<>();
        AtomicInteger callCount = new AtomicInteger();

        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            int count = callCount.getAndIncrement();
            return (count < maxConcurrency)
                ? barrierFuture // Block first 5 calls
                : CompletableFuture.completedFuture(null); // Immediate success for 6th
        });

        // Phase 1: Submit max concurrency requests
        List<ByteBuf> buffers = new ArrayList<>();
        for (int i = 0; i < maxConcurrency; i++) {
            ByteBuf data = TestUtils.randomPooled(1024);
            buffers.add(data);
            objectStorage.write(options, "testKey", data);
        }

        // Verify initial calls reached max concurrency
        await().atMost(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(maxConcurrency, callCount.get()));

        // Phase 2: Submit 6th request beyond concurrency limit
        CompletableFuture<ObjectStorage.WriteResult> sixthWriteFuture =
            CompletableFuture.supplyAsync(() ->
                objectStorage.write(options, "testKey", TestUtils.random(1024))
            ).thenCompose(f -> f);

        // Release blocked calls and verify completion
        barrierFuture.complete(null);
        await().atMost(2, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                assertEquals(maxConcurrency + 1, callCount.get());
                assertTrue(sixthWriteFuture.isDone());

                // Verify: all buffers released
                for (ByteBuf buffer : buffers) {
                    assertEquals(0, buffer.refCnt());
                }
            });
    }

    @Test
    void testWaitWritePermit() throws Exception {
        final int maxConcurrency = 1;
        objectStorage = spyStorage(new MemoryObjectStorage(maxConcurrency));

        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .enableFastRetry(false)
            .retry(false);

        // Block first call using completable future
        CompletableFuture<Void> blockingFuture = new CompletableFuture<>();
        AtomicInteger callCount = new AtomicInteger();

        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            callCount.incrementAndGet();
            return blockingFuture; // Always return blocking future for first call
        });

        // Phase 1: Acquire the only permit
        ByteBuf firstBuffer = TestUtils.randomPooled(1024);
        objectStorage.write(options, "testKey", firstBuffer);

        // Verify permit acquisition
        await().until(() -> callCount.get() == 1);

        // Phase 2: Verify blocking behavior with interrupt
        Thread blockingThread = new Thread(() -> {
            ByteBuf byteBuf = TestUtils.randomPooled(1024);
            try {
                CompletableFuture<ObjectStorage.WriteResult> future =
                    objectStorage.write(options, "testKey", byteBuf);
                ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
                assertTrue(exception.getCause() instanceof InterruptedException);
            } catch (Exception e) {
                // Ignore
            } finally {
                await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertEquals(0, byteBuf.refCnt());
                });
            }
        });

        blockingThread.start();

        Thread.sleep(1000);

        // Interrupt and verify
        blockingThread.interrupt();
        blockingThread.join();

        // Verify resource cleanup
        assertEquals(1, firstBuffer.refCnt());

        // Cleanup
        blockingFuture.complete(null);
        await().atMost(2, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(0, firstBuffer.refCnt()));
    }

    @Test
    void testReadToEndOfObject() throws ExecutionException, InterruptedException {
        objectStorage = new MemoryObjectStorage(true);
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 4096, S3ObjectType.STREAM);

        objectStorage.writer(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata.key()).write(TestUtils.random(4096));
        objectStorage = spyStorage(objectStorage);

        CompletableFuture<ByteBuf> cf1 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0L, 1024L);
        CompletableFuture<ByteBuf> cf2 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 2048L, -1L);

        objectStorage.readAccumulator.flush().join();
        verify(objectStorage, timeout(1000L).times(1)).doRangeRead(any(), eq(s3ObjectMetadata.key()), eq(0L), eq(1024L));
        objectStorage.readAccumulator.flush().join();
        verify(objectStorage, timeout(1000L).times(1)).doRangeRead(any(), eq(s3ObjectMetadata.key()), eq(2048L), eq(-1L));

        ByteBuf buf = cf1.get();
        assertEquals(1024, buf.readableBytes());
        buf.release();
        buf = cf2.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
    }

    @Test
    void testReadToEndWithNetworkLimiter() throws InterruptedException {
        int objectSize = 4096;
        objectStorage = new MemoryObjectStorage(false);
        MemoryObjectStorage memoryObjectStorage = (MemoryObjectStorage) objectStorage;
        RecordTestNetworkBandwidthLimiter networkInboundBandwidthLimiter = (RecordTestNetworkBandwidthLimiter) memoryObjectStorage.getNetworkInboundBandwidthLimiter();

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, objectSize, S3ObjectType.STREAM);
        objectStorage.writer(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata.key()).write(TestUtils.random(objectSize));
        objectStorage = spyStorage(objectStorage);

        // check read to end range read will increase the inboundWidthLimiter
        CompletableFuture<ByteBuf> cf2 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0, -1L);

        cf2.join();
        Thread.sleep(100); // wait for async reacquire token
        assertEquals(objectSize, networkInboundBandwidthLimiter.getConsumed());
    }

    @Test
    void testRangeReadAllFailCanFinishTheReturnedCompletableFuture() {
        int objectSize = 4096;
        MemoryObjectStorage memoryObjectStorage = new MemoryObjectStorage(true);

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, objectSize, S3ObjectType.STREAM);
        memoryObjectStorage.writer(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata.key()).write(TestUtils.random(objectSize));
        memoryObjectStorage = spyStorage(memoryObjectStorage);

        // mock read fail can finish the return completeFuture
        when(memoryObjectStorage.doRangeRead(any(), anyString(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException("mock exception")));

        CompletableFuture<ByteBuf> cf4 = memoryObjectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0, -1L);
        memoryObjectStorage.readAccumulator.flush().join();

        try {
            cf4.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // expected.
        }

        assertTrue(cf4.isDone() && cf4.isCompletedExceptionally());
    }

    /**
     * Given LIST and COPY fail transiently, when they are invoked through AbstractObjectStorage, then the storage
     * boundary retries each operation until it succeeds.
     */
    @Test
    void testListAndCopyRetryTransientFailures() throws Exception {
        objectStorage = spyStorage(objectStorage);
        doReturn(0).when(objectStorage).retryDelay(any(), anyInt());
        ObjectStorage.ObjectInfo object = new ObjectStorage.ObjectInfo(objectStorage.bucketId(), "prefix/key", 0, 1);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("LIST unavailable")),
            CompletableFuture.completedFuture(List.of(object))).when(objectStorage).doList(any());
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("COPY unavailable")),
            CompletableFuture.completedFuture(null)).when(objectStorage).doCopy(any(), anyString(), anyString());

        assertEquals(List.of(object), objectStorage.list(new ObjectStorage.ListOptions("prefix/")).get());
        objectStorage.copy(objectStorage.bucketURI(objectStorage.bucketId()).bucket(), "source", "destination").get();

        verify(objectStorage, times(2)).doList(any());
        verify(objectStorage, times(2)).doCopy(any(), eq("source"), eq("destination"));
    }

    /**
     * Given LIST and COPY fail permanently, when they are invoked through AbstractObjectStorage, then the storage
     * boundary returns each failure without retrying.
     */
    @Test
    void testListAndCopyDoNotRetryPermanentFailures() {
        objectStorage = spyStorage(objectStorage);
        doReturn(CompletableFuture.failedFuture(new IllegalArgumentException("invalid LIST")))
            .when(objectStorage).doList(any());
        doReturn(CompletableFuture.failedFuture(new ObjectNotExistException("missing source")))
            .when(objectStorage).doCopy(any(), anyString(), anyString());

        assertThrows(ExecutionException.class,
            () -> objectStorage.list(new ObjectStorage.ListOptions("prefix/")).get());
        assertThrows(ExecutionException.class, () -> objectStorage.copy(
            objectStorage.bucketURI(objectStorage.bucketId()).bucket(), "source", "destination").get());

        verify(objectStorage).doList(any());
        verify(objectStorage).doCopy(any(), eq("source"), eq("destination"));
    }
}
