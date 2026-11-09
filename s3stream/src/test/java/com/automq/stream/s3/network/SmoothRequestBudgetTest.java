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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class SmoothRequestBudgetTest {
    private static final long MIB = 1024 * 1024;
    private final MockTime time = new MockTime();

    private SmoothNetworkBandwidthLimiter limiter() {
        return new SmoothNetworkBandwidthLimiter(new SmoothNetworkBandwidthLimiter.Configuration(
            8 * MIB, 8 * MIB, Duration.ofSeconds(1), 0, 32768,
            Map.of(ThrottleStrategy.WAL_UPLOAD, 4, ThrottleStrategy.COMPACTION, 3,
                ThrottleStrategy.TAIL, 2, ThrottleStrategy.CATCH_UP, 1, ThrottleStrategy.ICEBERG_WRITE, 1)), time, Runnable::run);
    }

    private void nextWindow(SmoothNetworkBandwidthLimiter limiter) {
        time.sleep(1000);
        limiter.tick();
    }

    /** Grants spend global quota immediately; unclaimed payload does not stop the next FIFO request. */
    @Test
    void testProactiveFifoAndSingleAllocation() {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        limiter.reserve(ThrottleStrategy.WAL_UPLOAD, 4 * MIB);
        NetworkBandwidthLimiter.Handle a = limiter.register(ThrottleStrategy.WAL_UPLOAD, 8 * MIB);
        NetworkBandwidthLimiter.Handle b = limiter.register(ThrottleStrategy.WAL_UPLOAD, MIB);
        assertTrue(a.acquire(1).isDone());
        CompletableFuture<Void> startB = b.acquire(1);
        assertFalse(startB.isDone());
        assertEquals(4 * MIB, limiter.getAvailableTokens());
        nextWindow(limiter);
        assertFalse(startB.isDone());
        assertTrue(a.acquire(8 * MIB - 1).isDone());
        assertEquals(4 * MIB, limiter.getAvailableTokens());
        nextWindow(limiter);
        assertTrue(startB.isDone());
        assertEquals(7 * MIB, limiter.getAvailableTokens());
        assertTrue(b.acquire(MIB - 1).isDone());
        assertEquals(7 * MIB, limiter.getAvailableTokens());
        assertTrue(b.acquire(1).isCompletedExceptionally());
        limiter.shutdown();
    }

    /** A capped background group leaves global bandwidth available to other groups and one-shot requests. */
    @Test
    void testGroupCeilingAndGlobalCeiling() {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        NetworkBandwidthLimiter.RateReservation rate = limiter.reserve(ThrottleStrategy.WAL_UPLOAD, 4 * MIB);
        CompletableFuture<Void> wal = limiter.register(ThrottleStrategy.WAL_UPLOAD, 12 * MIB).acquire(12 * MIB);
        assertFalse(wal.isDone());
        assertTrue(limiter.consume(ThrottleStrategy.TAIL, 3 * MIB).isDone());
        assertEquals(MIB, limiter.getAvailableTokens());
        rate.burst();
        assertFalse(wal.isDone());
        assertEquals(0, limiter.getAvailableTokens());
        nextWindow(limiter);
        assertTrue(wal.isDone());
        assertEquals(MIB, limiter.getAvailableTokens());
        limiter.shutdown();
    }

    /** The floor is applied once to the sum, and closing a reservation removes only its contribution. */
    @Test
    void testAggregatedRateAndFloor() {
        for (ThrottleStrategy strategy : ThrottleStrategy.values()) {
            if (strategy == ThrottleStrategy.BYPASS) {
                continue;
            }
            SmoothNetworkBandwidthLimiter limiter = limiter();
            NetworkBandwidthLimiter.RateReservation first = limiter.reserve(strategy, MIB);
            NetworkBandwidthLimiter.RateReservation second = limiter.reserve(strategy, MIB);
            NetworkBandwidthLimiter.Handle request = limiter.register(strategy, 100 * MIB);
            assertTrue(request.acquire(4 * MIB).isDone());
            assertEquals(4 * MIB, limiter.getAvailableTokens());
            first.close();
            second.close();
            NetworkBandwidthLimiter.RateReservation third = limiter.reserve(strategy, 3 * MIB);
            limiter.reserve(strategy, 3 * MIB);
            nextWindow(limiter);
            assertEquals(2 * MIB, limiter.getAvailableTokens());
            third.close();
            nextWindow(limiter);
            assertEquals(4 * MIB, limiter.getAvailableTokens());
            limiter.shutdown();
        }
    }

    /** Missed windows cannot accumulate sending permission when the burst pool is disabled. */
    @Test
    void testLateTickDoesNotReplayExpiredWindows() {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        limiter.reserve(ThrottleStrategy.WAL_UPLOAD, 4 * MIB);
        NetworkBandwidthLimiter.Handle request = limiter.register(ThrottleStrategy.WAL_UPLOAD, 12 * MIB);
        CompletableFuture<Void> all = request.acquire(12 * MIB);
        time.sleep(10_000);
        limiter.tick();
        assertFalse(all.isDone());
        assertEquals(4 * MIB, limiter.getAvailableTokens());
        nextWindow(limiter);
        assertTrue(all.isDone());
        limiter.shutdown();
    }

    /** Global burst savings never override a group's current-window ceiling. */
    @Test
    void testGroupCeilingBoundsGlobalBurst() {
        SmoothNetworkBandwidthLimiter limiter = new SmoothNetworkBandwidthLimiter(
            new SmoothNetworkBandwidthLimiter.Configuration(8 * MIB, 16 * MIB, Duration.ofSeconds(1),
                10, 32768, Map.of(ThrottleStrategy.WAL_UPLOAD, 4)), time, Runnable::run);
        limiter.reserve(ThrottleStrategy.WAL_UPLOAD, 4 * MIB);
        time.sleep(10_000);
        NetworkBandwidthLimiter.Handle request = limiter.register(ThrottleStrategy.WAL_UPLOAD, 8 * MIB);
        assertTrue(request.acquire(4 * MIB).isDone());
        assertFalse(request.acquire(1).isDone());
        assertEquals(12 * MIB, limiter.getAvailableTokens());
        limiter.shutdown();
    }

    /** Closing releases pending acquires, skips remaining demand, and shutdown releases every waiter. */
    @Test
    void testCloseAndShutdown() {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        limiter.reserve(ThrottleStrategy.WAL_UPLOAD, 4 * MIB);
        NetworkBandwidthLimiter.Handle first = limiter.register(ThrottleStrategy.WAL_UPLOAD, 100 * MIB);
        NetworkBandwidthLimiter.Handle second = limiter.register(ThrottleStrategy.WAL_UPLOAD, 8 * MIB);
        CompletableFuture<Void> abandoned = first.acquire(100 * MIB);
        first.close();
        assertTrue(abandoned.isDone());
        CompletableFuture<Void> start = second.acquire(1);
        nextWindow(limiter);
        assertTrue(start.isDone());
        CompletableFuture<Void> rest = second.acquire(8 * MIB - 1);
        assertFalse(rest.isDone());
        limiter.shutdown();
        assertTrue(rest.isDone());
        assertTrue(second.acquire(MIB).isDone());
        assertTrue(limiter.register(ThrottleStrategy.WAL_UPLOAD, MIB).acquire(MIB).isDone());
    }

    /** Unknown-length reads charge each buffer on demand; BYPASS records debt without admission waits. */
    @Test
    void testUnknownLengthAndBypass() {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        NetworkBandwidthLimiter.Handle read = limiter.register(ThrottleStrategy.TAIL, -1);
        assertTrue(read.acquire(MIB).isDone());
        assertEquals(7 * MIB, limiter.getAvailableTokens());
        NetworkBandwidthLimiter.Handle bypass = limiter.register(ThrottleStrategy.BYPASS, 2 * MIB);
        assertTrue(bypass.acquire(2 * MIB).isDone());
        assertEquals(5 * MIB, limiter.getAvailableTokens());
        limiter.shutdown();
    }
    /** Hot-path registration and rate updates enqueue without blocking on the scheduling lock. */
    @Test
    void testRegisterDoesNotWaitForSchedulingLock() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        java.lang.reflect.Field field = SmoothNetworkBandwidthLimiter.class.getDeclaredField("lock");
        field.setAccessible(true);
        java.util.concurrent.locks.ReentrantLock lock = (java.util.concurrent.locks.ReentrantLock) field.get(limiter);
        java.util.concurrent.atomic.AtomicReference<CompletableFuture<Void>> result = new java.util.concurrent.atomic.AtomicReference<>();
        Thread producer = new Thread(() -> {
            limiter.reserve(ThrottleStrategy.WAL_UPLOAD, 4 * MIB);
            result.set(limiter.register(ThrottleStrategy.WAL_UPLOAD, 8 * MIB).acquire(4 * MIB));
        });
        lock.lock();
        try {
            producer.start();
            producer.join(5000);
            assertFalse(producer.isAlive());
            assertFalse(result.get().isDone());
        } finally {
            lock.unlock();
        }
        limiter.tick();
        assertTrue(result.get().isDone());
        assertEquals(4 * MIB, limiter.getAvailableTokens());
        limiter.shutdown();
    }

    /** Closing a queued request needs no allowance and never waits for an earlier request to finish. */
    @Test
    void testCloseBehindBlockedHeadWithoutSchedulingLock() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        limiter.reserve(ThrottleStrategy.WAL_UPLOAD, 4 * MIB);
        NetworkBandwidthLimiter.Handle head = limiter.register(ThrottleStrategy.WAL_UPLOAD, 100 * MIB);
        NetworkBandwidthLimiter.Handle tail = limiter.register(ThrottleStrategy.WAL_UPLOAD, 8 * MIB);
        CompletableFuture<Void> waiting = tail.acquire(8 * MIB);
        java.lang.reflect.Field field = SmoothNetworkBandwidthLimiter.class.getDeclaredField("lock");
        field.setAccessible(true);
        java.util.concurrent.locks.ReentrantLock lock = (java.util.concurrent.locks.ReentrantLock) field.get(limiter);
        Thread closer = new Thread(tail::close);
        lock.lock();
        try {
            closer.start();
            closer.join(5000);
            assertFalse(closer.isAlive());
            assertFalse(waiting.isDone());
        } finally {
            lock.unlock();
        }
        limiter.tick();
        assertTrue(waiting.isDone());
        assertEquals(1, limiter.getQueueSize());
        assertEquals(4 * MIB, limiter.getAvailableTokens());
        assertFalse(head.acquire(100 * MIB).isDone());
        limiter.shutdown();
    }

    /** Concurrent acquires on one handle claim each byte once and all wake after the remaining grant. */
    @Test
    void testConcurrentAcquiresOnOneRequest() throws Exception {
        SmoothNetworkBandwidthLimiter limiter = limiter();
        NetworkBandwidthLimiter.Handle request = limiter.register(ThrottleStrategy.TAIL, 16 * MIB);
        java.util.List<CompletableFuture<Void>> grants = new java.util.concurrent.CopyOnWriteArrayList<>();
        CompletableFuture<?>[] callers = new CompletableFuture<?>[16];
        for (int i = 0; i < callers.length; i++) {
            callers[i] = CompletableFuture.runAsync(() -> grants.add(request.acquire(MIB)));
        }
        CompletableFuture.allOf(callers).get(5, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(8, grants.stream().filter(CompletableFuture::isDone).count());
        assertTrue(request.acquire(1).isCompletedExceptionally());
        nextWindow(limiter);
        CompletableFuture.allOf(grants.toArray(CompletableFuture[]::new)).get(5, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(0, limiter.getAvailableTokens());
        limiter.shutdown();
    }

    /** Even a window smaller than one byte at the minimum rate still permits progress. */
    @Test
    void testSmallRateRoundsUpToOneBytePerWindow() {
        SmoothNetworkBandwidthLimiter limiter = new SmoothNetworkBandwidthLimiter(new SmoothNetworkBandwidthLimiter.Configuration(
            1_000_000_000, 1_000_000_000, Duration.ofNanos(1), 0, 1,
            Map.of(ThrottleStrategy.TAIL, 1)), time, Runnable::run);
        limiter.reserve(ThrottleStrategy.TAIL, 0.1);
        NetworkBandwidthLimiter.Handle request = limiter.register(ThrottleStrategy.TAIL, 2);
        assertTrue(request.acquire(1).isDone());
        CompletableFuture<Void> remaining = request.acquire(1);
        assertFalse(remaining.isDone());
        nextWindow(limiter);
        assertTrue(remaining.isDone());
        limiter.shutdown();
    }

}
