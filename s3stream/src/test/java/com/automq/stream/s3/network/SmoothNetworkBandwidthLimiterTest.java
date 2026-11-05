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

package com.automq.stream.s3.network;

import com.automq.stream.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
@Timeout(30)
class SmoothNetworkBandwidthLimiterTest {
    private final MockTime time = new MockTime(0, 0, 0);
    private final List<SmoothNetworkBandwidthLimiter> limiters = new ArrayList<>();

    @AfterEach
    void closeLimiters() {
        limiters.forEach(SmoothNetworkBandwidthLimiter::shutdown);
    }

    private SmoothNetworkBandwidthLimiter create(long baseline, long peak, long burstWindows, long quantum,
        Map<ThrottleStrategy, Integer> weights) {
        SmoothNetworkBandwidthLimiter limiter = new SmoothNetworkBandwidthLimiter(
            config(baseline, peak, burstWindows, quantum, weights), time, Runnable::run);
        limiters.add(limiter);
        return limiter;
    }

    private SmoothNetworkBandwidthLimiter.Configuration config(long baseline, long peak, long burstWindows,
        long quantum, Map<ThrottleStrategy, Integer> weights) {
        return new SmoothNetworkBandwidthLimiter.Configuration(baseline * 1000, peak * 1000,
            Duration.ofMillis(1), burstWindows, quantum, weights);
    }

    private void advance(SmoothNetworkBandwidthLimiter limiter, long windows) {
        time.sleep(windows);
        limiter.tick();
    }

    private List<CompletableFuture<Void>> enqueue(SmoothNetworkBandwidthLimiter limiter, ThrottleStrategy strategy,
        int count, long bytes) {
        List<CompletableFuture<Void>> requests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requests.add(limiter.consume(strategy, bytes));
        }
        return requests;
    }

    private long completed(List<CompletableFuture<Void>> requests) {
        return requests.stream().filter(CompletableFuture::isDone).count();
    }

    /** Given no burst capacity, idle windows cannot fund an additional grant in the current window. */
    @Test
    void testNoBurstAndNoEarlyRefill() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        CompletableFuture<Void> waiting = limiter.consume(ThrottleStrategy.TAIL, 64);
        limiter.tick();
        assertFalse(waiting.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        waiting.join();
        assertEquals(64, limiter.getAvailableTokens());
        advance(limiter, 100);
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        assertFalse(limiter.consume(ThrottleStrategy.TAIL, 1).isDone());
    }

    /** Given a full savings pool, peak grants last exactly three windows and then return to baseline. */
    @Test
    void testBurstPeakAndExhaustion() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 256, 3, 64, Map.of(ThrottleStrategy.TAIL, 1));
        assertEquals(128, limiter.getAvailableTokens());
        advance(limiter, 3);
        for (int window = 0; window < 3; window++) {
            if (window > 0) {
                advance(limiter, 1);
            }
            limiter.consume(ThrottleStrategy.TAIL, 128).join();
            limiter.consume(ThrottleStrategy.TAIL, 128).join();
            assertEquals(0, limiter.getAvailableTokens());
        }
        advance(limiter, 1);
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        assertFalse(limiter.consume(ThrottleStrategy.TAIL, 64).isDone());
    }

    /** Given unused baseline tokens, only their remainder enters savings; unused peak allowance does not. */
    @Test
    void testOnlyUnusedBaselineAccumulates() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 256, 3, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 96).join();
        advance(limiter, 1);
        assertEquals(160, limiter.getAvailableTokens());
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        limiter.consume(ThrottleStrategy.TAIL, 32).join();
        advance(limiter, 1);
        assertEquals(128, limiter.getAvailableTokens());
    }

    /** Given a delayed timer, savings may fill but only one current window's peak can be granted. */
    @Test
    void testLateRefillDoesNotReplayWindows() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 256, 3, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        List<CompletableFuture<Void>> pending = enqueue(limiter, ThrottleStrategy.TAIL, 20, 64);
        advance(limiter, 1000);
        assertEquals(4, completed(pending));
        limiter.tick();
        assertEquals(4, completed(pending));
    }

    /** WAL upload waits for budget and receives its larger configured share without starving tail reads. */
    @Test
    void testWalUploadIsThrottledWithHigherWeight() {
        SmoothNetworkBandwidthLimiter limiter = create(320, 320, 0, 64,
            Map.of(ThrottleStrategy.WAL_UPLOAD, 4, ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.BYPASS, 320).join();
        List<CompletableFuture<Void>> uploads = enqueue(limiter, ThrottleStrategy.WAL_UPLOAD, 10, 64);
        List<CompletableFuture<Void>> reads = enqueue(limiter, ThrottleStrategy.TAIL, 10, 64);
        assertEquals(0, completed(uploads));
        assertEquals(0, completed(reads));
        advance(limiter, 1);
        assertEquals(4, completed(uploads));
        assertEquals(1, completed(reads));
    }

    /** Given weights 4:2:1, continuously queued groups receive that byte ratio across refill boundaries. */
    @Test
    void testWeightedRoundSpansWindows() {
        SmoothNetworkBandwidthLimiter limiter = create(256, 256, 0, 64,
            Map.of(ThrottleStrategy.COMPACTION, 4, ThrottleStrategy.CATCH_UP, 2, ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 256).join();
        List<CompletableFuture<Void>> a = enqueue(limiter, ThrottleStrategy.COMPACTION, 16, 64);
        List<CompletableFuture<Void>> b = enqueue(limiter, ThrottleStrategy.CATCH_UP, 16, 64);
        List<CompletableFuture<Void>> c = enqueue(limiter, ThrottleStrategy.TAIL, 16, 64);
        // The preceding TAIL request leaves the cursor at CATCH_UP in the fixed group order.
        advance(limiter, 1);
        assertEquals(2, completed(a));
        assertEquals(2, completed(b));
        advance(limiter, 1);
        assertEquals(4, completed(a));
        assertEquals(3, completed(b));
        assertEquals(1, completed(c));
        advance(limiter, 1);
        assertEquals(7, completed(a));
        assertEquals(4, completed(b));
        assertEquals(1, completed(c));
        for (int i = 0; i < 4; i++) {
            advance(limiter, 1);
        }
        assertEquals(16, completed(a));
        assertEquals(8, completed(b));
        assertEquals(4, completed(c));
    }

    /** Given variable request sizes, partial allocations use the remaining deficit without reordering a FIFO. */
    @Test
    void testPartialAllocationsPreserveFifoAcrossRounds() {
        SmoothNetworkBandwidthLimiter limiter = create(320, 320, 0, 64,
            Map.of(ThrottleStrategy.COMPACTION, 2, ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 320).join();
        List<String> order = new ArrayList<>();
        limiter.consume(ThrottleStrategy.COMPACTION, 96).thenRun(() -> order.add("A1"));
        limiter.consume(ThrottleStrategy.COMPACTION, 96).thenRun(() -> order.add("A2"));
        limiter.consume(ThrottleStrategy.COMPACTION, 64).thenRun(() -> order.add("A3"));
        limiter.consume(ThrottleStrategy.TAIL, 64).thenRun(() -> order.add("B1"));
        limiter.consume(ThrottleStrategy.TAIL, 64).thenRun(() -> order.add("B2"));
        advance(limiter, 1);
        assertEquals(List.of("A1", "B1", "A2", "A3"), order);
        advance(limiter, 1);
        assertEquals(List.of("A1", "B1", "A2", "A3", "B2"), order);
    }

    /** Given a group that emptied with unused deficit, reactivation starts with only one fresh quantum. */
    @Test
    void testEmptyGroupLosesDeficit() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64,
            Map.of(ThrottleStrategy.COMPACTION, 2, ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.COMPACTION, 1).join();
        limiter.consume(ThrottleStrategy.TAIL, 127).join();
        List<String> order = new ArrayList<>();
        limiter.consume(ThrottleStrategy.COMPACTION, 128).thenRun(() -> order.add("A1"));
        limiter.consume(ThrottleStrategy.COMPACTION, 64).thenRun(() -> order.add("A2"));
        limiter.consume(ThrottleStrategy.TAIL, 64).thenRun(() -> order.add("B1"));
        advance(limiter, 1);
        advance(limiter, 1);
        assertEquals(List.of("A1", "B1", "A2"), order);
    }

    /** Given only one active group, its weight never leaves bandwidth reserved for idle groups. */
    @Test
    void testSingleGroupUsesAllTokensAcrossMultipleRounds() {
        SmoothNetworkBandwidthLimiter limiter = create(448, 448, 0, 32,
            Map.of(ThrottleStrategy.COMPACTION, 4, ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.COMPACTION, 448).join();
        List<CompletableFuture<Void>> tail = enqueue(limiter, ThrottleStrategy.TAIL, 8, 64);
        advance(limiter, 1);
        assertEquals(7, completed(tail));
    }

    /** Given pending demand, shutdown releases all futures successfully and repeated shutdown has no effect. */
    @Test
    void testShutdownReleasesPendingRequests() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        CompletableFuture<Void> first = limiter.consume(ThrottleStrategy.TAIL, 128);
        CompletableFuture<Void> second = limiter.consume(ThrottleStrategy.TAIL, 64);
        assertEquals(2, limiter.getQueueSize());
        limiter.shutdown();
        first.join();
        second.join();
        assertEquals(0, limiter.getQueueSize());
        assertEquals(0, limiter.getAvailableTokens());
        limiter.consume(ThrottleStrategy.TAIL, 1).join();
        limiter.shutdown();
    }

    /** Given an interrupted blocking waiter, only waiting stops; the limiter retains completion ownership. */
    @Test
    void testInterruptedWaiterDoesNotCancelRequest() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        AtomicReference<Throwable> interruption = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                limiter.consumeBlocking(ThrottleStrategy.TAIL, 64);
            } catch (Exception e) {
                interruption.set(e);
            }
        });
        waiter.start();
        waiter.join(5000);
        assertFalse(waiter.isAlive());
        assertTrue(interruption.get() instanceof InterruptedException);
        // Inspect the internal future to distinguish a stopped waiter from a cancelled request.
        Field groupsField = SmoothNetworkBandwidthLimiter.class.getDeclaredField("groups");
        groupsField.setAccessible(true);
        Object group = ((Map<?, ?>) groupsField.get(limiter)).get(ThrottleStrategy.TAIL);
        Field requestsField = group.getClass().getDeclaredField("requests");
        requestsField.setAccessible(true);
        Object request = ((ConcurrentLinkedQueue<?>) requestsField.get(group)).peek();
        Field futureField = request.getClass().getDeclaredField("future");
        futureField.setAccessible(true);
        CompletableFuture<?> future = (CompletableFuture<?>) futureField.get(request);
        assertFalse(future.isDone());
        advance(limiter, 1);
        future.join();
        assertEquals(0, limiter.getQueueSize());
        assertEquals(64, limiter.getAvailableTokens());
    }

    /** Given invalid or unconfigured ordinary requests, fail promptly; BYPASS needs no configured weight. */
    @Test
    void testRequestContract() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 256, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        assertEquals(128, limiter.getMaxTokens());
        assertTrue(limiter.consume(ThrottleStrategy.CATCH_UP, 1).isCompletedExceptionally());
        assertTrue(limiter.consume(ThrottleStrategy.BYPASS, -1).isCompletedExceptionally());
        assertTrue(limiter.consume(null, 1).isCompletedExceptionally());
        limiter.consume(ThrottleStrategy.BYPASS, 0).join();
        limiter.consume(ThrottleStrategy.BYPASS, 128).join();
        limiter.consume(ThrottleStrategy.BYPASS, 1).join();
        assertEquals(0, limiter.getAvailableTokens());
    }

    /** Given a request larger than baseline and peak, it completes only after three windows fund all bytes. */
    @Test
    void testLargeRequestSpansWindows() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        CompletableFuture<Void> large = limiter.consume(ThrottleStrategy.TAIL, 320);
        assertFalse(large.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        assertEquals(1, limiter.getQueueSize());
        advance(limiter, 1);
        assertFalse(large.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        large.join();
        assertEquals(64, limiter.getAvailableTokens());
        assertEquals(0, limiter.getQueueSize());
    }

    /** Given a partially funded large request, other groups still receive their weighted allocations. */
    @Test
    void testLargeRequestDoesNotMonopolizeOtherGroups() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64,
            Map.of(ThrottleStrategy.COMPACTION, 1, ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        CompletableFuture<Void> large = limiter.consume(ThrottleStrategy.COMPACTION, 320);
        List<CompletableFuture<Void>> small = enqueue(limiter, ThrottleStrategy.TAIL, 10, 64);
        for (int i = 1; i <= 5; i++) {
            advance(limiter, 1);
            assertEquals(i, completed(small));
            assertEquals(i == 5, large.isDone());
        }
    }

    /** Given a window tail smaller than the head request, it funds that request and is not discarded. */
    @Test
    void testWindowRemainderFundsNextRequest() {
        SmoothNetworkBandwidthLimiter limiter = create(100, 100, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        List<CompletableFuture<Void>> requests = enqueue(limiter, ThrottleStrategy.TAIL, 3, 64);
        assertEquals(1, completed(requests));
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        assertEquals(3, completed(requests));
        assertEquals(8, limiter.getAvailableTokens());
    }

    /** Given full savings, one large request uses peak allocations until savings run out, then baseline. */
    @Test
    void testLargeRequestRespectsBurstAllocationLimit() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 256, 3, 64, Map.of(ThrottleStrategy.TAIL, 1));
        advance(limiter, 3);
        CompletableFuture<Void> large = limiter.consume(ThrottleStrategy.TAIL, 1024);
        for (int i = 0; i < 3; i++) {
            assertFalse(large.isDone());
            assertEquals(0, limiter.getAvailableTokens());
            advance(limiter, 1);
        }
        assertFalse(large.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        large.join();
        assertEquals(0, limiter.getAvailableTokens());
    }

    /** Given weighted groups and large backlogs, repeated visits preserve the 1:3 byte allocation. */
    @Test
    @Timeout(5)
    void testWeightedRoundsForLargeRequests() {
        SmoothNetworkBandwidthLimiter limiter = create(1_000_000_000, 1_000_000_000, 0, 1_000_000,
            Map.of(ThrottleStrategy.COMPACTION, 1, ThrottleStrategy.TAIL, 3));
        limiter.consume(ThrottleStrategy.TAIL, 1_000_000_000).join();
        CompletableFuture<Void> a = limiter.consume(ThrottleStrategy.COMPACTION, 500_000_000);
        CompletableFuture<Void> b = limiter.consume(ThrottleStrategy.TAIL, 1_500_000_000);
        advance(limiter, 1);
        assertFalse(a.isDone());
        assertFalse(b.isDone());
        advance(limiter, 1);
        a.join();
        b.join();
        assertEquals(0, limiter.getAvailableTokens());
    }

    /** Given the largest positive request, partial arithmetic remains bounded and shutdown releases the remainder. */
    @Test
    void testLongMaxRequestCanWaitAndShutDown() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        CompletableFuture<Void> large = limiter.consume(ThrottleStrategy.TAIL, Long.MAX_VALUE);
        advance(limiter, 1);
        assertFalse(large.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        limiter.shutdown();
        large.join();
        assertEquals(0, limiter.getQueueSize());
    }

    /** Given a contended scheduling lock, producers enqueue without waiting and refill later makes progress. */
    @Test
    void testConsumeDoesNotWaitForSchedulingLock() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        Field field = SmoothNetworkBandwidthLimiter.class.getDeclaredField("lock");
        field.setAccessible(true);
        ReentrantLock schedulingLock = (ReentrantLock) field.get(limiter);
        AtomicReference<CompletableFuture<Void>> result = new AtomicReference<>();
        Thread producer = new Thread(() -> result.set(limiter.consume(ThrottleStrategy.TAIL, 64)));
        schedulingLock.lock();
        try {
            producer.start();
            producer.join(5000);
            assertFalse(producer.isAlive());
            assertFalse(result.get().isDone());
            assertEquals(1, limiter.getQueueSize());
        } finally {
            schedulingLock.unlock();
        }
        limiter.tick();
        result.get().join();
        assertEquals(64, limiter.getAvailableTokens());
    }

    /** Given enqueue during the last request's retirement, the scheduler revisits the group without losing the new request. */
    @Test
    void testEnqueueWhenGroupBecomesEmpty() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.TAIL, 128).join();
        CompletableFuture<Void> first = limiter.consume(ThrottleStrategy.TAIL, 64);
        AtomicReference<CompletableFuture<Void>> second = new AtomicReference<>();
        Field groupsField = SmoothNetworkBandwidthLimiter.class.getDeclaredField("groups");
        groupsField.setAccessible(true);
        Object group = ((Map<?, ?>) groupsField.get(limiter)).get(ThrottleStrategy.TAIL);
        Field requestsField = group.getClass().getDeclaredField("requests");
        requestsField.setAccessible(true);
        AtomicBoolean inject = new AtomicBoolean(true);
        // Force enqueue after the scheduler observes empty, but before it advances the cursor.
        ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>((Collection<?>) requestsField.get(group)) {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean isEmpty() {
                boolean empty = super.isEmpty();
                if (empty && inject.compareAndSet(true, false)) {
                    Thread producer = new Thread(() -> second.set(limiter.consume(ThrottleStrategy.TAIL, 64)));
                    producer.start();
                    try {
                        producer.join(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    assertFalse(producer.isAlive());
                }
                return empty;
            }
        };
        requestsField.set(group, queue);
        advance(limiter, 1);
        first.get(5, TimeUnit.SECONDS);
        second.get().get(5, TimeUnit.SECONDS);
        assertEquals(0, limiter.getQueueSize());
        assertEquals(0, limiter.getAvailableTokens());
    }

    /** Given a locked scheduler, BYPASS returns successfully without waiting for scheduling or consuming a queue slot. */
    @Test
    void testBypassDoesNotWaitForSchedulingLock() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        Field field = SmoothNetworkBandwidthLimiter.class.getDeclaredField("lock");
        field.setAccessible(true);
        ReentrantLock schedulingLock = (ReentrantLock) field.get(limiter);
        AtomicReference<CompletableFuture<Void>> result = new AtomicReference<>();
        Thread producer = new Thread(() -> result.set(limiter.consume(ThrottleStrategy.BYPASS, 256)));
        schedulingLock.lock();
        try {
            producer.start();
            producer.join(5000);
            assertFalse(producer.isAlive());
            assertTrue(result.get().isDone());
            result.get().join();
            assertEquals(0, limiter.getQueueSize());
        } finally {
            schedulingLock.unlock();
        }
        limiter.tick();
        assertEquals(0, limiter.getAvailableTokens());
    }

    /** Given unobserved bypass usage, delayed accounting does not spend expired idle windows retroactively. */
    @Test
    void testNewBypassUsageIsChargedWhenObserved() {
        SmoothNetworkBandwidthLimiter limiter = create(100, 100, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.BYPASS, 100).join();
        advance(limiter, 5);
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        assertEquals(100, limiter.getAvailableTokens());
    }

    /** Given bypass usage above baseline, it succeeds immediately and future windows repay debt before ordinary traffic. */
    @Test
    void testBypassDebtIsRepaidBeforeOrdinaryTraffic() {
        SmoothNetworkBandwidthLimiter limiter = create(100, 100, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        CompletableFuture<Void> bypass = limiter.consume(ThrottleStrategy.BYPASS, 250);
        assertTrue(bypass.isDone());
        bypass.join();
        assertEquals(0, limiter.getQueueSize());
        CompletableFuture<Void> ordinary = limiter.consume(ThrottleStrategy.TAIL, 50);
        assertFalse(ordinary.isDone());
        advance(limiter, 1);
        assertFalse(ordinary.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        ordinary.join();
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        assertEquals(100, limiter.getAvailableTokens());
    }

    /** Given a full burst pool, debt repayment consumes peak budget before normal requests may borrow savings. */
    @Test
    void testBypassRepaymentUsesBurstAndPeakBudget() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 256, 3, 64, Map.of(ThrottleStrategy.TAIL, 1));
        advance(limiter, 3);
        limiter.consume(ThrottleStrategy.BYPASS, 300).join();
        CompletableFuture<Void> ordinary = limiter.consume(ThrottleStrategy.TAIL, 64);
        assertFalse(ordinary.isDone());
        advance(limiter, 1);
        ordinary.join();
        assertEquals(148, limiter.getAvailableTokens());
    }

    /** Given known debt and delayed refill, missed baseline credits repay debt without replaying send budgets. */
    @Test
    void testMissedWindowsRepayKnownDebt() {
        SmoothNetworkBandwidthLimiter limiter = create(100, 200, 2, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.BYPASS, 350).join();
        limiter.tick();
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 3);
        assertEquals(50, limiter.getAvailableTokens());
    }

    /** Given multiple largest-long bypass calls, debt is capped at ten baseline windows without overflowing. */
    @Test
    void testBypassPendingUsageIsCapped() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.BYPASS, Long.MAX_VALUE).join();
        limiter.consume(ThrottleStrategy.BYPASS, Long.MAX_VALUE).join();
        CompletableFuture<Void> ordinary = limiter.consume(ThrottleStrategy.TAIL, 1);
        advance(limiter, 9);
        assertFalse(ordinary.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        assertTrue(ordinary.isDone());
        assertEquals(127, limiter.getAvailableTokens());
        limiter.shutdown();
        ordinary.join();
        limiter.consume(ThrottleStrategy.BYPASS, Long.MAX_VALUE).join();
    }

    /** Given existing debt and new usage, their combined debt is capped at ten baseline windows. */
    @Test
    void testBypassMergedDebtIsCapped() {
        SmoothNetworkBandwidthLimiter limiter = create(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1));
        limiter.consume(ThrottleStrategy.BYPASS, Long.MAX_VALUE).join();
        limiter.tick();
        limiter.consume(ThrottleStrategy.BYPASS, Long.MAX_VALUE).join();
        assertEquals(0, limiter.getAvailableTokens());
        CompletableFuture<Void> ordinary = limiter.consume(ThrottleStrategy.TAIL, 1);
        advance(limiter, 10);
        assertFalse(ordinary.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        advance(limiter, 1);
        assertTrue(ordinary.isDone());
        assertEquals(127, limiter.getAvailableTokens());
    }

    /** Given elapsed credits exceeding long range, refill saturates savings and retains only one peak send budget. */
    @Test
    void testMissedWindowCreditsSaturate() {
        SmoothNetworkBandwidthLimiter limiter = create(10_000_000, 20_000_000, 2, 64,
            Map.of(ThrottleStrategy.TAIL, 1));
        advance(limiter, 1_000_000_000_000L);
        assertEquals(20_000_000, limiter.getAvailableTokens());
        limiter.consume(ThrottleStrategy.TAIL, 20_000_000).join();
        assertEquals(0, limiter.getAvailableTokens());
        assertFalse(limiter.consume(ThrottleStrategy.TAIL, 1).isDone());
    }

    /** Given concurrent enqueue and close, every accepted request succeeds and no queued request is stranded. */
    @Test
    void testConcurrentEnqueueAndShutdown() throws Exception {
        for (int iteration = 0; iteration < 50; iteration++) {
            SmoothNetworkBandwidthLimiter limiter = create(1, 1, 0, 1, Map.of(ThrottleStrategy.TAIL, 1));
            CountDownLatch start = new CountDownLatch(1);
            List<CompletableFuture<Void>> requests = new ArrayList<>();
            Thread producer = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < 100; i++) {
                    requests.add(limiter.consume(ThrottleStrategy.TAIL, 64));
                }
            });
            producer.start();
            start.countDown();
            limiter.shutdown();
            producer.join(5000);
            assertFalse(producer.isAlive());
            assertEquals(100, requests.size());
            for (CompletableFuture<Void> request : requests) {
                request.get(5, TimeUnit.SECONDS);
            }
            assertEquals(0, limiter.getQueueSize());
        }
    }

    /** Given invalid rates, weights or overflowing arithmetic, reject the configuration before starting threads. */
    @Test
    void testConfigurationValidation() {
        assertThrows(IllegalArgumentException.class, () -> config(128, 64, 1, 64, Map.of(ThrottleStrategy.TAIL, 1)));
        assertThrows(IllegalArgumentException.class, () -> config(128, 128, -1, 64, Map.of(ThrottleStrategy.TAIL, 1)));
        assertThrows(IllegalArgumentException.class, () -> config(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 0)));
        assertThrows(IllegalArgumentException.class, () -> config(128, 128, 0, 64, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> config(128, 128, 0, 64, Map.of(ThrottleStrategy.BYPASS, 1)));
        assertThrows(ArithmeticException.class, () -> config(128, 256, Long.MAX_VALUE, 64, Map.of(ThrottleStrategy.TAIL, 1)));
        assertThrows(ArithmeticException.class, () -> config(128, 128, 0, Long.MAX_VALUE, Map.of(ThrottleStrategy.TAIL, 2)));
    }

    /** Given owned executors and queued demand, shutdown drains successful notifications and allows later requests. */
    @Test
    void testShutdownReleasesRequestsWithOwnedExecutors() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = new SmoothNetworkBandwidthLimiter(
            new SmoothNetworkBandwidthLimiter.Configuration(128, 128, Duration.ofSeconds(60), 0, 64,
                Map.of(ThrottleStrategy.TAIL, 1)));
        limiters.add(limiter);
        CompletableFuture<Void> first = limiter.consume(ThrottleStrategy.TAIL, Long.MAX_VALUE);
        CompletableFuture<Void> second = limiter.consume(ThrottleStrategy.TAIL, 64);
        assertFalse(first.isDone());
        assertFalse(second.isDone());
        limiter.shutdown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(0, limiter.getQueueSize());
        CompletableFuture<Void> afterClose = limiter.consume(ThrottleStrategy.TAIL, Long.MAX_VALUE);
        assertTrue(afterClose.isDone());
        afterClose.join();
        assertTrue(limiter.consume(ThrottleStrategy.TAIL, -1).isCompletedExceptionally());
        limiter.shutdown();
    }

    /** Given the real scheduler, queued requests complete and user callbacks do not hold the state lock. */
    @Test
    void testScheduledRefillAndCallbackReentry() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = new SmoothNetworkBandwidthLimiter(
            config(128, 128, 0, 64, Map.of(ThrottleStrategy.TAIL, 1)));
        limiters.add(limiter);
        CountDownLatch completed = new CountDownLatch(1);
        limiter.consume(ThrottleStrategy.TAIL, 128).thenRun(() -> {
            limiter.consume(ThrottleStrategy.TAIL, 128).thenRun(completed::countDown);
        });
        assertTrue(completed.await(10, TimeUnit.SECONDS));
    }
}
