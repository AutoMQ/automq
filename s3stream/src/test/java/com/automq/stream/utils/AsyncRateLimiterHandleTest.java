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

package com.automq.stream.utils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class AsyncRateLimiterHandleTest {
    /** Admission advances by declared bytes/rate even when earlier handles remain alive. */
    @Test
    void testRateBasedAdmissionWithoutHandleCountLimit() throws Exception {
        MockTime time = new MockTime();
        AsyncRateLimiter limiter = new AsyncRateLimiter(100, time);
        try {
            for (int i = 0; i < 8; i++) {
                CompletableFuture<AsyncRateLimiter.Handle> next = limiter.register(100);
                if (i != 0) {
                    assertFalse(next.isDone());
                    time.sleep(1000);
                    limiter.tick();
                }
                next.get(1, TimeUnit.SECONDS);
            }
        } finally {
            limiter.close();
        }
    }

    /** Registration does not consume chunk permits; burst releases queued admission and acquisition. */
    @Test
    void testRegistrationAndActualBytesUseSeparateBudgets() throws Exception {
        AsyncRateLimiter limiter = new AsyncRateLimiter(1, new MockTime());
        try {
            AsyncRateLimiter.Handle first = limiter.register(1000).join();
            assertTrue(first.acquire(1).isDone());
            CompletableFuture<Void> chunk = first.acquire(1);
            CompletableFuture<AsyncRateLimiter.Handle> next = limiter.register(1000);
            assertFalse(chunk.isDone());
            assertFalse(next.isDone());
            limiter.burst();
            chunk.get(1, TimeUnit.SECONDS);
            next.get(1, TimeUnit.SECONDS);
        } finally {
            limiter.close();
        }
    }

    /** Idle limiters schedule nothing; an earlier byte deadline replaces admission wakeup safely. */
    @Test
    void testDeadlineSchedulingAndStaleWakeup() {
        MockTime time = new MockTime();
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        List<Runnable> callbacks = new ArrayList<>();
        List<Long> delays = new ArrayList<>();
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS))).thenAnswer(call -> {
            callbacks.add(call.getArgument(0));
            delays.add(call.getArgument(1));
            return mock(ScheduledFuture.class);
        });
        AsyncRateLimiter limiter = new AsyncRateLimiter(100, time, scheduler);
        try {
            verifyNoInteractions(scheduler);
            limiter.register(1000).join();
            CompletableFuture<AsyncRateLimiter.Handle> admission = limiter.register(1);
            limiter.acquire(100).join();
            CompletableFuture<Void> chunk = limiter.acquire(1);
            assertEquals(List.of(TimeUnit.SECONDS.toNanos(10), TimeUnit.SECONDS.toNanos(1)), delays);
            callbacks.get(0).run();
            assertFalse(admission.isDone());
            assertFalse(chunk.isDone());
            assertEquals(2, callbacks.size());
            time.sleep(1000);
            callbacks.get(1).run();
            assertTrue(chunk.isDone());
            assertFalse(admission.isDone());
            assertEquals(TimeUnit.SECONDS.toNanos(9), delays.get(2));
            time.sleep(9000);
            callbacks.get(2).run();
            assertTrue(admission.isDone());
            assertEquals(3, callbacks.size());
        } finally {
            limiter.close();
        }
    }

    /** Late wakeups catch up queued admissions and bytes, but idle time creates no new credit. */
    @Test
    void testLateWakeupCatchesUpWithoutAccumulatingIdleCredit() {
        MockTime time = new MockTime();
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS)))
            .thenAnswer(call -> mock(ScheduledFuture.class));
        AsyncRateLimiter limiter = new AsyncRateLimiter(100, time, scheduler);
        try {
            limiter.register(10).join();
            limiter.acquire(10).join();
            List<CompletableFuture<AsyncRateLimiter.Handle>> admissions = new ArrayList<>();
            List<CompletableFuture<Void>> chunks = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                admissions.add(limiter.register(10));
                chunks.add(limiter.acquire(10));
            }
            time.sleep(250);
            limiter.tick();
            for (int i = 0; i < 3; i++) {
                assertEquals(i < 2, admissions.get(i).isDone());
                assertEquals(i < 2, chunks.get(i).isDone());
            }
            time.sleep(50);
            limiter.tick();
            assertTrue(admissions.get(2).isDone());
            assertTrue(chunks.get(2).isDone());

            time.sleep(1000);
            assertTrue(limiter.register(10).isDone());
            assertTrue(limiter.acquire(10).isDone());
            CompletableFuture<AsyncRateLimiter.Handle> nextAdmission = limiter.register(10);
            CompletableFuture<Void> nextChunk = limiter.acquire(10);
            limiter.tick();
            assertFalse(nextAdmission.isDone());
            assertFalse(nextChunk.isDone());
            time.sleep(100);
            limiter.tick();
            assertTrue(nextAdmission.isDone());
            assertTrue(nextChunk.isDone());
        } finally {
            limiter.close();
        }
    }

    /** Closing a task grants queued and future requests, including acquisitions through existing handles. */
    @Test
    void testCloseCompletesWaiters() {
        AsyncRateLimiter limiter = new AsyncRateLimiter(1, new MockTime());
        AsyncRateLimiter.Handle first = limiter.register(1000).join();
        first.acquire(1).join();
        CompletableFuture<Void> chunk = first.acquire(1);
        CompletableFuture<AsyncRateLimiter.Handle> next = limiter.register(1000);
        limiter.close();
        chunk.join();
        next.join();
        assertTrue(first.acquire(1000).isDone());
        assertTrue(limiter.register(1000).join().acquire(1000).isDone());
    }
}
