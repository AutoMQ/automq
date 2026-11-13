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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.operator.ReadRequestAccumulator.MergedReadTask;
import com.automq.stream.s3.operator.ReadRequestAccumulator.ReadTask;
import com.automq.stream.utils.threads.EventLoop;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class ReadRequestAccumulatorTest {
    private final AtomicLong now = new AtomicLong();
    private final List<Timer> timers = new ArrayList<>();
    private ReadRequestAccumulator accumulator;
    private ExecutorService callbackExecutor;
    private ReadRequestAccumulator.Reader reader;
    private ScheduledExecutorService scheduler;
    private EventLoop eventLoop;
    private final List<SubmittedRead> submitted = new ArrayList<>();

    @BeforeEach
    void setUp() {
        scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS))).thenAnswer(invocation -> {
            ScheduledFuture<?> future = mock(ScheduledFuture.class);
            timers.add(new Timer(now.get() + invocation.<Long>getArgument(1), invocation.getArgument(0), future));
            return future;
        });
        callbackExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(callbackExecutor).execute(any(Runnable.class));
        eventLoop = ReadRequestAccumulator.EVENT_LOOP;
        reader = mock(ReadRequestAccumulator.Reader.class);
        doAnswer(invocation -> {
            SubmittedRead read = new SubmittedRead(invocation.getArgument(4));
            submitted.add(read);
            return read.response;
        }).when(reader).read(any(), any(), anyLong(), anyLong(), any());
        accumulator = new ReadRequestAccumulator(scheduler, now::get, 0.5f, true,
            reader, callbackExecutor, mock(Logger.class));
    }

    @AfterEach
    void tearDown() {
        accumulator.close().join();
    }

    /** Given independent objects, each seals five milliseconds after its own last enqueue. */
    @Test
    void idleDeadlineIsPerObjectAndRefreshesOnEnqueue() {
        stallReads();
        accumulator.add(task("a", 0, 10));
        accumulator.add(task("b", 0, 10));
        advance(4);
        accumulator.add(task("a", 10, 20));
        advance(1);
        verify(reader).read(any(), eq("b"), eq(0L), eq(10L), any());
        verify(reader, never()).read(any(), eq("a"), anyLong(), anyLong(), any());
        advance(3);
        verify(reader, never()).read(any(), eq("a"), anyLong(), anyLong(), any());
        advance(1);
        verify(reader).read(any(), eq("a"), eq(0L), eq(20L), any());
        accumulator.close().join();
    }

    /** Given uninterrupted arrivals, the first caller's batch still seals at the 100ms hard deadline. */
    @Test
    void continuousArrivalsCannotExtendHardDeadline() {
        stallReads();
        accumulator.add(task("key", 0, 10));
        for (int i = 1; i < 25; i++) {
            advance(4);
            accumulator.add(task("key", i * 10, i * 10 + 10));
            verify(reader, never()).read(any(), any(), anyLong(), anyLong(), any());
        }
        advance(4);
        verify(reader).read(any(), eq("key"), eq(0L), eq(250L), any());
        accumulator.close().join();
    }

    /** Given a sealed GET waiting for quota, a covered caller reuses it without opening another batch. */
    @Test
    void sealedAndInflightReadsShareExactSlices() {
        CompletableFuture<ByteBuf> physical = new CompletableFuture<>();
        doReturn(physical).when(reader).read(any(), eq("key"), anyLong(), anyLong(), any());
        ReadTask parent = task("key", 100, 200);
        accumulator.add(parent);
        advance(5);
        ReadTask child = task("key", 120, 140);
        child.options.source(S3RequestSource.DELTA_WAL);
        accumulator.add(child);
        verify(reader).read(argThat(options -> options.source() == S3RequestSource.UNCLASSIFIED),
            eq("key"), eq(100L), eq(200L), any());
        ReadTask later = task("key", 150, 170);
        accumulator.add(later);
        accumulator.flush().join();
        verify(reader).read(any(), eq("key"), eq(100L), eq(200L), any());
        ByteBuf response = Unpooled.buffer(100).writeZero(100);
        response.setByte(20, 42);
        physical.complete(response);
        assertEquals(100, parent.cf.join().readableBytes());
        assertEquals(20, child.cf.join().readableBytes());
        assertEquals(42, child.cf.join().getByte(child.cf.join().readerIndex()));
        assertEquals(20, later.cf.join().readableBytes());
        parent.cf.join().release();
        child.cf.join().release();
        later.cf.join().release();
        assertEquals(0, response.refCnt());
        accumulator.close().join();
    }

    /** Given a partially overlapping range or another object, neither attaches to an unrelated in-flight GET. */
    @Test
    void partialOverlapAndDifferentObjectsAreNotAttached() {
        accumulator.add(task("a", 100, 200));
        flushReads();
        accumulator.add(task("a", 150, 250));
        accumulator.add(task("b", 120, 140));
        assertEquals(2, flushReads().size());
        accumulator.close().join();
    }

    /** Given a cancelled child, completion still delivers the parent and releases the unused child slice. */
    @Test
    void cancelledCallerDoesNotCancelSharedReadOrLeakBuffer() {
        ReadTask parent = task("key", 0, 100);
        accumulator.add(parent);
        SubmittedRead plan = flushReads().get(0);
        ReadTask child = task("key", 10, 20);
        accumulator.add(child);
        child.cf.cancel(false);
        awaitEventLoop();
        ByteBuf response = Unpooled.buffer(100).writeZero(100);
        plan.complete(response, null);
        awaitEventLoop();
        parent.cf.join().release();
        assertEquals(0, response.refCnt());
        accumulator.close().join();
    }

    /** Given a failed GET, every attached caller sees the same failure and a subsequent read starts anew. */
    @Test
    void failureRemovesInflightRead() {
        ReadTask parent = task("key", 0, 100);
        accumulator.add(parent);
        SubmittedRead plan = flushReads().get(0);
        ReadTask child = task("key", 10, 20);
        accumulator.add(child);
        awaitEventLoop();
        RuntimeException failure = new RuntimeException("GET failed");
        plan.complete(null, failure);
        awaitEventLoop();
        assertSame(failure, parent.cf.handle((v, ex) -> ex).join());
        assertSame(failure, child.cf.handle((v, ex) -> ex).join());
        accumulator.add(task("key", 10, 20));
        assertEquals(1, flushReads().size());
        accumulator.close().join();
    }

    /** Given pending and in-flight reads, close fails callers and releases any late physical response. */
    @Test
    void closeFailsCallersAndDisposesLateResponse() {
        ReadTask parent = task("a", 0, 100);
        accumulator.add(parent);
        SubmittedRead plan = flushReads().get(0);
        ReadTask pending = task("b", 0, 100);
        accumulator.add(pending);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("storage closed")))
            .when(reader).read(any(), eq("b"), anyLong(), anyLong(), any());
        accumulator.close().join();
        assertTrue(parent.cf.isCompletedExceptionally());
        assertTrue(pending.cf.isCompletedExceptionally());
        ByteBuf response = Unpooled.buffer(100).writeZero(100);
        plan.complete(response, null);
        awaitEventLoop();
        assertEquals(0, response.refCnt());
        advance(100);
        verify(reader).read(any(), eq("b"), eq(0L), eq(100L), any());
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("storage closed")))
            .when(reader).read(any(), eq("c"), anyLong(), anyLong(), any());
        ReadTask rejected = task("c", 0, 100);
        accumulator.add(rejected);
        assertTrue(rejected.cf.isCompletedExceptionally());
    }

    /** Given a short storage response, slicing failure fails all callers and releases already retained slices. */
    @Test
    void invalidResponseCannotLeakPartiallyCreatedSlices() {
        ReadTask first = task("key", 0, 10);
        ReadTask second = task("key", 20, 30);
        accumulator.add(first);
        accumulator.add(second);
        SubmittedRead plan = flushReads().get(0);
        ByteBuf response = Unpooled.buffer(15, 15).writeZero(15);
        plan.complete(response, null);
        awaitEventLoop();
        assertTrue(first.cf.isCompletedExceptionally());
        assertTrue(second.cf.isCompletedExceptionally());
        assertEquals(0, response.refCnt());
        accumulator.close().join();
    }

    /** Given cumulative chunks, covered requests return in end order while the full GET remains pending. */
    @Test
    void progressDeliversCrossComponentRangesAndLateReaders() {
        ReadTask parent = task("key", 100, 200);
        ReadTask first = task("key", 105, 115);
        ReadTask second = task("key", 120, 140);
        accumulator.add(parent);
        accumulator.add(second);
        accumulator.add(first);
        SubmittedRead plan = flushReads().get(0);
        CompositeByteBuf response = Unpooled.compositeBuffer(Integer.MAX_VALUE);
        ByteBuf chunk = Unpooled.buffer(10).writeZero(10);
        response.addComponent(true, chunk);
        plan.onProgress(response);
        awaitEventLoop();
        assertFalse(first.cf.isDone());
        response.addComponent(true, Unpooled.buffer(10).writeZero(10));
        response.setByte(14, 42);
        plan.onProgress(response);
        awaitEventLoop();
        assertEquals(42, first.cf.join().getByte(9));
        assertFalse(parent.cf.isDone());
        ReadTask late = task("key", 107, 117);
        accumulator.add(late);
        awaitEventLoop();
        assertFalse(late.cf.isDone());
        plan.onProgress(response);
        assertEquals(10, late.cf.join().readableBytes());
        assertEquals(42, late.cf.join().getByte(7));
        response.addComponent(true, Unpooled.buffer(80).writeZero(80));
        plan.onProgress(response);
        awaitEventLoop();
        assertEquals(100, parent.cf.join().readableBytes());
        plan.complete(response, null);
        awaitEventLoop();
        first.cf.join().release();
        second.cf.join().release();
        late.cf.join().release();
        parent.cf.join().release();
        assertEquals(0, chunk.refCnt());
        accumulator.close().join();
    }

    /** Given a shorter retry prefix, late callers wait until that attempt covers their range. */
    @Test
    void lateCallersWaitForCoverageInCurrentRetryPrefix() {
        ReadTask parent = task("key", 0, 100);
        accumulator.add(parent);
        SubmittedRead plan = flushReads().get(0);
        CompositeByteBuf original = Unpooled.compositeBuffer().addComponent(true, Unpooled.buffer(50).writeZero(50));
        plan.onProgress(original);
        awaitEventLoop();
        original.release();
        CompositeByteBuf retry = Unpooled.compositeBuffer().addComponent(true, Unpooled.buffer(20).writeZero(20));
        plan.onProgress(retry);
        awaitEventLoop();
        ReadTask late = task("key", 30, 40);
        accumulator.add(late);
        awaitEventLoop();
        assertFalse(late.cf.isDone());
        plan.onProgress(retry);
        assertFalse(late.cf.isDone());
        assertFalse(parent.cf.isDone());
        retry.addComponent(true, Unpooled.buffer(80).writeZero(80));
        plan.onProgress(retry);
        awaitEventLoop();
        assertEquals(10, late.cf.join().readableBytes());
        plan.complete(retry, null);
        awaitEventLoop();
        parent.cf.join().release();
        late.cf.join().release();
        assertEquals(0, original.refCnt());
        accumulator.close().join();
    }

    /** Given a read to EOF, progress cannot establish its end and it waits for the complete response. */
    @Test
    void readToEndWaitsForCompletion() {
        ReadTask eof = task("key", 0, ObjectStorage.RANGE_READ_TO_END);
        accumulator.add(eof);
        SubmittedRead plan = flushReads().get(0);
        ByteBuf chunk = Unpooled.buffer(10).writeZero(10);
        plan.onProgress(chunk);
        awaitEventLoop();
        assertFalse(eof.cf.isDone());
        plan.complete(chunk, null);
        awaitEventLoop();
        assertEquals(10, eof.cf.join().readableBytes());
        eof.cf.join().release();
        assertEquals(0, chunk.refCnt());
        accumulator.close().join();
    }

    /** Given an early child response, a late transport failure only fails the remaining callers. */
    @Test
    void lateFailureOnlyFailsRemainingCallers() {
        ReadTask parent = task("key", 0, 100);
        ReadTask child = task("key", 0, 10);
        accumulator.add(parent);
        accumulator.add(child);
        SubmittedRead plan = flushReads().get(0);
        ByteBuf chunk = Unpooled.buffer(10).writeZero(10);
        plan.onProgress(chunk);
        awaitEventLoop();
        chunk.release();
        assertFalse(parent.cf.isDone());
        plan.complete(null, new IllegalStateException("late failure"));
        awaitEventLoop();
        assertTrue(parent.cf.isCompletedExceptionally());
        assertEquals(0, child.cf.join().getByte(0));
        child.cf.join().release();
        assertEquals(0, chunk.refCnt());
        accumulator.close().join();
    }

    /** Given queued progress whose callback closes again, shutdown delivers it without waiting on its own close. */
    @Test
    void closeDeliversQueuedProgressAndIgnoresLateNotifications() {
        List<Runnable> queued = new ArrayList<>();
        doAnswer(invocation -> {
            queued.add(invocation.getArgument(0));
            return null;
        }).when(callbackExecutor).execute(any(Runnable.class));
        ReadTask parent = task("key", 0, 100);
        ReadTask child = task("key", 0, 10);
        accumulator.add(parent);
        accumulator.add(child);
        SubmittedRead plan = flushReads().get(0);
        ByteBuf chunk = Unpooled.buffer(10).writeZero(10);
        plan.onProgress(chunk);
        awaitEventLoop();
        assertFalse(child.cf.isDone());
        child.cf.whenComplete((data, error) -> accumulator.close());
        CompletableFuture<Void> closed = accumulator.close();
        assertSame(closed, accumulator.close());
        awaitEventLoop();
        awaitEventLoop();
        assertFalse(closed.isDone());
        assertFalse(parent.cf.isDone());
        queued.forEach(Runnable::run);
        closed.join();
        assertSame(closed, accumulator.close());
        verify(callbackExecutor, never()).shutdown();
        verify(callbackExecutor, never()).shutdownNow();
        assertEquals(10, child.cf.join().readableBytes());
        child.cf.join().release();
        assertTrue(parent.cf.isCompletedExceptionally());
        plan.onProgress(chunk);
        awaitEventLoop();
        chunk.release();
        assertEquals(0, chunk.refCnt());
    }

    /** Given concurrent retry prefixes, each covered caller is delivered once and all retained bytes are released. */
    @Test
    void concurrentProgressNotificationsShareTheWaitingList() throws Exception {
        ReadTask parent = task("key", 0, 100);
        ReadTask child = task("key", 10, 30);
        accumulator.add(parent);
        accumulator.add(child);
        SubmittedRead plan = flushReads().get(0);
        ByteBuf shorter = Unpooled.buffer(40).writeZero(40);
        ByteBuf longer = Unpooled.buffer(80).writeZero(80);
        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 100; i++) {
                plan.onProgress(shorter);
                awaitEventLoop();
            }
        });
        CompletableFuture<Void> second = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 100; i++) {
                plan.onProgress(longer);
                awaitEventLoop();
            }
        });
        CompletableFuture.allOf(first, second).get(5, TimeUnit.SECONDS);
        assertEquals(20, child.cf.join().readableBytes());
        assertFalse(parent.cf.isDone());
        accumulator.close().join();
        child.cf.join().release();
        shorter.release();
        longer.release();
        assertEquals(0, shorter.refCnt());
        assertEquals(0, longer.refCnt());
    }

    /** Given closed batching, new reads bypass accumulation and let the underlying reader decide their result. */
    @Test
    void closedAccumulatorRoutesNewRequestsToReader() {
        accumulator.close().join();
        ByteBuf response = Unpooled.buffer(10).writeZero(10);
        doReturn(CompletableFuture.completedFuture(response))
            .when(reader).read(any(), eq("key"), eq(10L), eq(20L), any());
        ReadTask caller = task("key", 10, 20);
        accumulator.add(caller);
        assertEquals(10, caller.cf.join().readableBytes());
        caller.cf.join().release();
        assertEquals(0, response.refCnt());
        verify(reader).read(any(), eq("key"), eq(10L), eq(20L), any());
    }

    /** Given a pending batch at close, sealing submits it once instead of locally failing its callers. */
    @Test
    void closeSubmitsPendingBatchToReader() {
        ByteBuf response = Unpooled.buffer(20).writeZero(20);
        doReturn(CompletableFuture.completedFuture(response))
            .when(reader).read(any(), eq("key"), eq(10L), eq(30L), any());
        ReadTask first = task("key", 10, 20);
        ReadTask second = task("key", 20, 30);
        accumulator.add(first);
        accumulator.add(second);
        accumulator.close().join();
        assertEquals(10, first.cf.join().readableBytes());
        assertEquals(10, second.cf.join().readableBytes());
        first.cf.join().release();
        second.cf.join().release();
        assertEquals(0, response.refCnt());
        verify(reader).read(any(), eq("key"), eq(10L), eq(30L), any());
    }

    /** Given flush seals a batch first, an already dequeued stale timer cannot submit its GET again. */
    @Test
    void flushAndTimerDispatchClaimEachPlanOnce() {
        stallReads();
        accumulator.add(task("key", 0, 10));
        awaitEventLoop();
        Timer stale = timers.remove(0);
        accumulator.flush().join();
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(5));
        stale.task.run();
        awaitEventLoop();
        verify(reader).read(any(), eq("key"), eq(0L), eq(10L), any());
        accumulator.close().join();
    }

    /** Given a synchronously completed GET, routing is removed before a later covered request arrives. */
    @Test
    void synchronousGetCompletionDoesNotLeaveStaleInflightRouting() throws Exception {
        List<ByteBuf> responses = new ArrayList<>();
        doAnswer(invocation -> {
            int length = Math.toIntExact(invocation.<Long>getArgument(3) - invocation.<Long>getArgument(2));
            ByteBuf response = Unpooled.buffer(length).writeZero(length);
            responses.add(response);
            return CompletableFuture.completedFuture(response);
        }).when(reader).read(any(), eq("key"), anyLong(), anyLong(), any());
        try {
            ReadTask first = task("key", 0, 100);
            accumulator.add(first);
            advance(5);
            assertEquals(100, first.cf.join().readableBytes());
            first.cf.join().release();
            ReadTask later = task("key", 10, 20);
            CompletableFuture.runAsync(() -> accumulator.add(later)).get(2, TimeUnit.SECONDS);
            advance(5);
            assertEquals(10, later.cf.join().readableBytes());
            later.cf.join().release();
            verify(reader).read(any(), eq("key"), eq(0L), eq(100L), any());
            verify(reader).read(any(), eq("key"), eq(10L), eq(20L), any());
            responses.forEach(response -> assertEquals(0, response.refCnt()));
        } finally {
            accumulator.close().join();
        }
    }

    /** Given a pending nonblocking read, timers can submit other objects and progress can complete children. */
    @Test
    void pendingReadDoesNotBlockTimersOrProgress() throws Exception {
        AtomicReference<ReadProgressListener> progress = new AtomicReference<>();
        CompletableFuture<ByteBuf> physical = new CompletableFuture<>();
        doAnswer(invocation -> {
            progress.set(invocation.getArgument(4));
            return physical;
        }).when(reader).read(any(), eq("a"), anyLong(), anyLong(), any());
        doReturn(CompletableFuture.completedFuture(Unpooled.buffer(10).writeZero(10)))
            .when(reader).read(any(), eq("b"), anyLong(), anyLong(), any());
        ReadTask first = task("a", 0, 100);
        ReadTask child = task("a", 10, 20);
        ReadTask second = task("b", 0, 10);
        accumulator.add(first);
        advance(5);
        accumulator.add(second);
        advance(5);
        second.cf.get(1, TimeUnit.SECONDS).release();
        accumulator.add(child);
        awaitEventLoop();
        ByteBuf prefix = Unpooled.buffer(20).writeZero(20);
        progress.get().onChange(List.of(prefix), prefix.readableBytes());
        prefix.release();
        child.cf.get(1, TimeUnit.SECONDS).release();
        assertFalse(first.cf.isDone());
        physical.complete(Unpooled.buffer(100).writeZero(100));
        first.cf.get(1, TimeUnit.SECONDS).release();
    }

    /** Given a reused chunk list, queued progress owns stable slices even after source references are released. */
    @Test
    void reusedChunkListDoesNotEscapeSynchronousListener() {
        List<Runnable> events = new ArrayList<>();
        ReadTask parent = task("key", 0, 8);
        ReadTask child = task("key", 1, 7);
        MergedReadTask plan = new MergedReadTask(List.of(parent, child), 0, reader, mock(Logger.class),
            events::add, ignored -> { });
        ByteBuf first = Unpooled.buffer().writeBytes(new byte[] {0, 1, 2, 3});
        first.readByte();
        ByteBuf second = Unpooled.buffer().writeBytes(new byte[] {4, 5, 6, 7, 8});
        List<ByteBuf> chunks = new ArrayList<>();
        chunks.add(first);
        plan.onProgress(chunks, 3);
        chunks.add(second);
        plan.onProgress(chunks, 8);
        // A controlled mutation demonstrates that the delivered view shares bytes rather than copying.
        first.setByte(2, 42);
        first.release();
        second.release();
        chunks.clear();
        events.forEach(Runnable::run);
        ByteBuf result = child.cf.join();
        assertEquals(6, result.readableBytes());
        for (int i = 0; i < 6; i++) {
            assertEquals(i == 0 ? 42 : i + 2, result.getByte(i));
        }
        plan.close();
        events.get(events.size() - 1).run();
        result.release();
        parent.cf.join().release();
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
    }

    /** Given sparse covered callers, progress retains only their span and holds no prefix without ready callers. */
    @Test
    void progressRetainsOnlySelectedCallerSpan() {
        List<Runnable> deliveries = new ArrayList<>();
        ReadTask parent = task("key", 100, 200);
        ReadTask first = task("key", 112, 118);
        ReadTask second = task("key", 120, 125);
        MergedReadTask plan = new MergedReadTask(List.of(parent), 0, reader, mock(Logger.class),
            deliveries::add, ignored -> { });
        ByteBuf prefix = Unpooled.buffer(10).writeZero(10);
        ByteBuf middle = Unpooled.buffer(20).writeZero(20);
        ByteBuf suffix = Unpooled.buffer(10).writeZero(10);
        middle.setByte(2, 42);
        middle.setByte(10, 43);
        List<ByteBuf> chunks = List.of(prefix, middle, suffix);
        plan.onProgress(chunks, 40);
        assertTrue(deliveries.isEmpty());
        assertEquals(1, middle.refCnt());
        assertTrue(plan.tryAdd(second));
        assertTrue(plan.tryAdd(first));
        plan.onProgress(chunks, 40);
        assertEquals(1, prefix.refCnt());
        assertEquals(2, middle.refCnt());
        assertEquals(1, suffix.refCnt());
        prefix.release();
        middle.release();
        suffix.release();
        deliveries.remove(0).run();
        assertEquals(6, first.cf.join().readableBytes());
        assertEquals(42, first.cf.join().getByte(0));
        assertEquals(5, second.cf.join().readableBytes());
        assertEquals(43, second.cf.join().getByte(0));
        first.cf.join().release();
        second.cf.join().release();
        assertEquals(0, middle.refCnt());
        plan.close();
        deliveries.remove(0).run();
        assertTrue(parent.cf.isCompletedExceptionally());
    }

    /** Given no further progress after attachment, a covered late caller waits for the complete response. */
    @Test
    void lateCallerWaitsForCompletionWithoutFurtherProgress() {
        List<Runnable> deliveries = new ArrayList<>();
        ReadTask parent = task("key", 0, 100);
        ReadTask late = task("key", 0, 10);
        MergedReadTask plan = new MergedReadTask(List.of(parent), 0, reader, mock(Logger.class),
            deliveries::add, ignored -> { });
        ByteBuf prefix = Unpooled.buffer(20).writeZero(20);
        plan.onProgress(List.of(prefix), 20);
        assertTrue(plan.tryAdd(late));
        assertTrue(deliveries.isEmpty());
        assertFalse(late.cf.isDone());
        ByteBuf response = Unpooled.buffer(100).writeZero(100);
        plan.complete(response, null);
        deliveries.forEach(Runnable::run);
        parent.cf.join().release();
        late.cf.join().release();
        prefix.release();
        assertEquals(0, prefix.refCnt());
        assertEquals(0, response.refCnt());
    }

    /** Given completion precedes routing cleanup, a finished read rejects attachment before delivery runs. */
    @Test
    void completedReadRejectsAttachmentBeforeDelivery() {
        List<Runnable> deliveries = new ArrayList<>();
        ReadTask parent = task("key", 0, 10);
        AtomicBoolean removed = new AtomicBoolean();
        MergedReadTask plan = new MergedReadTask(List.of(parent), 0, reader, mock(Logger.class),
            deliveries::add, ignored -> {
                assertEquals(1, deliveries.size());
                removed.set(true);
            });
        ByteBuf response = Unpooled.buffer(10).writeZero(10);
        plan.complete(response, null);
        assertTrue(removed.get());
        assertFalse(parent.cf.isDone());
        assertFalse(plan.tryAdd(task("key", 1, 2)));
        deliveries.get(0).run();
        parent.cf.join().release();
        assertEquals(0, response.refCnt());
    }

    /** Given concurrent progress, response and close, every caller terminates and all buffer references are released. */
    @Test
    void progressCompletionAndCloseShareOneMonitor() throws Exception {
        for (int i = 0; i < 30; i++) {
            java.util.Queue<Runnable> deliveries = new java.util.concurrent.ConcurrentLinkedQueue<>();
            ReadTask parent = task("key", 0, 100);
            ReadTask child = task("key", 0, 10);
            MergedReadTask plan = new MergedReadTask(List.of(parent, child), 0, reader, mock(Logger.class),
                deliveries::add, ignored -> { });
            ByteBuf prefix = Unpooled.buffer(10).writeZero(10);
            ByteBuf response = Unpooled.buffer(100).writeZero(100);
            CompletableFuture<Void> start = new CompletableFuture<>();
            CompletableFuture<Void> progress = start.thenRunAsync(() -> plan.onProgress(List.of(prefix), 10));
            CompletableFuture<Void> completion = start.thenRunAsync(() -> plan.complete(response, null));
            CompletableFuture<Void> close = start.thenRunAsync(plan::close);
            start.complete(null);
            CompletableFuture.allOf(progress, completion, close).get(5, TimeUnit.SECONDS);
            prefix.release();
            deliveries.forEach(Runnable::run);
            for (ReadTask caller : List.of(parent, child)) {
                assertTrue(caller.cf.isDone());
                caller.cf.handle((data, error) -> {
                    if (data != null) {
                        data.release();
                    }
                    return null;
                }).join();
            }
            assertFalse(plan.tryAdd(task("key", 0, 1)));
            assertEquals(0, prefix.refCnt());
            assertEquals(0, response.refCnt());
        }
    }

    /** Given EventLoop queueing delay, batching deadlines begin when the queued add is processed. */
    @Test
    void enqueueTimeIsCapturedOnEventLoop() throws Exception {
        accumulator.close().join();
        accumulator = new ReadRequestAccumulator(scheduler, () -> {
            assertSame(eventLoop, Thread.currentThread());
            return now.get();
        }, 0.5f, true, reader, callbackExecutor, mock(Logger.class));
        stallReads();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        eventLoop.execute(() -> {
            entered.countDown();
            try {
                assertTrue(release.await(2, TimeUnit.SECONDS));
            } catch (InterruptedException ex) {
                throw new AssertionError(ex);
            }
        });
        try {
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            accumulator.add(task("key", 0, 10));
            now.set(TimeUnit.MILLISECONDS.toNanos(120));
            release.countDown();
            advance(4);
            verify(reader, never()).read(any(), any(), anyLong(), anyLong(), any());
            advance(1);
            verify(reader).read(any(), eq("key"), eq(0L), eq(10L), any());
        } finally {
            release.countDown();
        }
    }

    /** Given two accumulators share the daemon EventLoop, closing one preserves the other's reads and late buffer cleanup. */
    @Test
    void closeLeavesSharedEventLoopAndOtherAccumulatorRunning() throws Exception {
        CompletableFuture<ByteBuf> firstPhysical = new CompletableFuture<>();
        CompletableFuture<ByteBuf> secondPhysical = new CompletableFuture<>();
        doReturn(firstPhysical).when(reader).read(any(), eq("first"), anyLong(), anyLong(), any());
        doReturn(secondPhysical).when(reader).read(any(), eq("second"), anyLong(), anyLong(), any());
        ReadRequestAccumulator other = new ReadRequestAccumulator(scheduler, now::get, 0.5f, false,
            reader, ReadRequestAccumulator.CALLBACK_EXECUTOR, mock(Logger.class));
        ReadTask first = task("first", 0, 100);
        ReadTask second = task("second", 0, 100);
        try {
            accumulator.add(first);
            accumulator.flush().join();
            other.add(second);
            other.flush().join();
            accumulator.close().join();
            assertTrue(first.cf.isCompletedExceptionally());
            assertTrue(eventLoop.isDaemon());
            assertTrue(eventLoop.isAlive());
            verify(scheduler, never()).shutdown();
            verify(scheduler, never()).shutdownNow();
            ByteBuf late = Unpooled.buffer(100).writeZero(100);
            firstPhysical.complete(late);
            awaitEventLoop();
            assertEquals(0, late.refCnt());
            assertFalse(second.cf.isDone());
            secondPhysical.complete(Unpooled.buffer(100).writeZero(100));
            second.cf.get(1, TimeUnit.SECONDS).release();
            doReturn(CompletableFuture.completedFuture(Unpooled.buffer(10).writeZero(10)))
                .when(reader).read(any(), eq("later"), anyLong(), anyLong(), any());
            ReadTask later = task("later", 0, 10);
            other.add(later);
            other.flush().join();
            later.cf.get(1, TimeUnit.SECONDS).release();
        } finally {
            other.close().join();
        }
    }

    /** Given a closed client's late callback, its continuation can flush and close another client off the shared loop. */
    @Test
    void closedCallbackDoesNotRunOnSharedEventLoop() throws Exception {
        accumulator.close().join();
        accumulator = new ReadRequestAccumulator(scheduler, now::get, 0.5f, false,
            reader, ReadRequestAccumulator.CALLBACK_EXECUTOR, mock(Logger.class));
        accumulator.close().join();
        ReadRequestAccumulator other = new ReadRequestAccumulator(scheduler, now::get, 0.5f, false,
            reader, ReadRequestAccumulator.CALLBACK_EXECUTOR, mock(Logger.class));
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("storage closed")))
            .when(reader).read(any(), eq("late"), anyLong(), anyLong(), any());
        ReadTask late = task("late", 0, 10);
        CompletableFuture<Void> callback = late.cf.handle((data, error) -> {
            assertFalse(Thread.currentThread() == eventLoop);
            return other.flush().thenCompose(ignored -> other.close());
        }).thenCompose(future -> future);
        try {
            eventLoop.submit(() -> accumulator.add(late)).get(1, TimeUnit.SECONDS);
            callback.get(2, TimeUnit.SECONDS);
            eventLoop.submit(() -> { }).get(1, TimeUnit.SECONDS);
        } finally {
            other.close().join();
        }
    }

    /** Given lifecycle calls on the shared loop, both return futures without waiting for their own queued events. */
    @Test
    void lifecycleCallsOnSharedEventLoopReturnFutures() throws Exception {
        ReadRequestAccumulator other = new ReadRequestAccumulator(scheduler, now::get, 0.5f, false,
            reader, ReadRequestAccumulator.CALLBACK_EXECUTOR, mock(Logger.class));
        AtomicReference<CompletableFuture<Void>> flushed = new AtomicReference<>();
        AtomicReference<CompletableFuture<Void>> closed = new AtomicReference<>();
        try {
            eventLoop.submit(() -> {
                flushed.set(other.flush().thenRun(() -> assertFalse(Thread.currentThread() == eventLoop)));
                closed.set(other.close());
            }).get(1, TimeUnit.SECONDS);
            flushed.get().get(1, TimeUnit.SECONDS);
            closed.get().get(2, TimeUnit.SECONDS);
            assertSame(closed.get(), other.close());
            eventLoop.submit(() -> { }).get(1, TimeUnit.SECONDS);
        } finally {
            other.close().join();
        }
    }

    /** Given a pending physical read, flush completes after submission without waiting for data. */
    @Test
    void flushFutureOnlyWaitsForSubmission() throws Exception {
        stallReads();
        ReadTask caller = task("key", 0, 10);
        accumulator.add(caller);
        accumulator.flush().get(1, TimeUnit.SECONDS);
        assertFalse(caller.cf.isDone());
        verify(reader).read(any(), eq("key"), eq(0L), eq(10L), any());
        accumulator.close().get(2, TimeUnit.SECONDS);
        assertTrue(caller.cf.isCompletedExceptionally());
    }

    private List<SubmittedRead> flushReads() {
        awaitEventLoop();
        int previous = submitted.size();
        accumulator.flush().join();
        return new ArrayList<>(submitted.subList(previous, submitted.size()));
    }

    private static final class SubmittedRead {
        private final ReadProgressListener listener;
        private final CompletableFuture<ByteBuf> response = new CompletableFuture<>();

        private SubmittedRead(ReadProgressListener listener) {
            this.listener = listener;
        }

        private void onProgress(ByteBuf data) {
            listener.onChange(data instanceof CompositeByteBuf
                ? ((CompositeByteBuf) data).decompose(data.readerIndex(), data.readableBytes()) : List.of(data),
                data.readableBytes());
        }

        private void complete(ByteBuf data, Throwable error) {
            if (error == null) {
                response.complete(data);
            } else {
                response.completeExceptionally(error);
            }
        }
    }

    private void awaitEventLoop() {
        eventLoop.submit(() -> { }).join();
    }

    private void stallReads() {
        doReturn(new CompletableFuture<ByteBuf>()).when(reader).read(any(), any(), anyLong(), anyLong(), any());
    }

    private ReadTask task(String path, long start, long end) {
        return new ReadTask(new ObjectStorage.ReadOptions(), path, start, end, new CompletableFuture<>());
    }

    private void advance(long millis) {
        awaitEventLoop();
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        for (;;) {
            Timer due = timers.stream().filter(t -> t.deadline <= now.get()).findFirst().orElse(null);
            if (due == null) {
                return;
            }
            timers.remove(due);
            due.task.run();
            awaitEventLoop();
        }
    }

    private static final class Timer {
        private final long deadline;
        private final Runnable task;

        private Timer(long deadline, Runnable task, ScheduledFuture<?> future) {
            this.deadline = deadline;
            this.task = task;
            when(future.cancel(false)).thenReturn(true);
        }
    }
}
