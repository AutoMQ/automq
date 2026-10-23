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

package kafka.automq.partition.snapshot;

import org.apache.kafka.common.utils.MockTime;

import com.automq.stream.s3.ConfirmWAL;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class SnapshotReadCommitterTest {
    private final Deque<Runnable> tasks = new ArrayDeque<>();
    private final Deque<Long> delays = new ArrayDeque<>();
    private MockTime time;
    private ConfirmWAL wal;
    private SnapshotReadCommitter committer;

    @BeforeEach
    void setup() {
        time = new MockTime();
        wal = mock(ConfirmWAL.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS))).thenAnswer(invocation -> {
            tasks.add(invocation.getArgument(0));
            delays.add(invocation.getArgument(1));
            return mock(ScheduledFuture.class);
        });
        when(wal.commit(0, false)).thenReturn(CompletableFuture.completedFuture(null));
        committer = new SnapshotReadCommitter(time, executor, wal);
    }

    /** Given many proxy requests before dispatch, one urgent commit covers the batch and idle creates no work. */
    @Test
    void testCoalescesProxyRequestsAndStopsWhenIdle() {
        CompletableFuture<Void> first = committer.request();
        for (int i = 0; i < 1000; i++) {
            assertSame(first, committer.request());
        }
        assertEquals(1, tasks.size());
        assertEquals(0, delays.peekFirst());
        verifyNoInteractions(wal);
        runNext();
        assertTrue(first.isDone());
        verify(wal).commit(0, false);
        assertTrue(tasks.isEmpty());
    }

    /** Requests for newer data cannot be satisfied by an older upload; they share one bounded follow-up. */
    @Test
    void testInFlightRequestsRequireAnotherCommit() {
        CompletableFuture<Void> upload = new CompletableFuture<>();
        CompletableFuture<Void> nextUpload = new CompletableFuture<>();
        when(wal.commit(0, false)).thenReturn(upload, nextUpload);
        CompletableFuture<Void> first = committer.request();
        runNext();
        CompletableFuture<Void> second = committer.request();
        assertNotSame(first, second);
        for (int i = 0; i < 1000; i++) {
            assertSame(second, committer.request());
        }
        assertTrue(tasks.isEmpty());
        upload.complete(null);
        assertTrue(first.isDone());
        assertFalse(second.isDone());
        assertEquals(SnapshotReadCommitter.MIN_INTERVAL_NS, delays.peekFirst());
        runNext();
        assertFalse(second.isDone());
        nextUpload.complete(null);
        assertTrue(second.isDone());
        verify(wal, times(2)).commit(0, false);
        assertTrue(tasks.isEmpty());
    }

    /** Fast completions must not turn repeated cache-eviction requests into an unbounded upload loop. */
    @Test
    void testMinimumIntervalUsesMonotonicTime() {
        committer.request();
        runNext();
        time.sleep(250);
        committer.request();
        assertEquals(TimeUnit.MILLISECONDS.toNanos(750), delays.peekFirst());
        runNext();
        time.sleep(2000);
        committer.request();
        assertEquals(0, delays.peekFirst());
        runNext();
        verify(wal, times(3)).commit(0, false);
    }

    /** A failed commit propagates failure without stranding the newer-data batch or starting parallel uploads. */
    @Test
    void testFailedUploadDoesNotStrandFollowUp() {
        CompletableFuture<Void> upload = new CompletableFuture<>();
        when(wal.commit(0, false)).thenReturn(upload, CompletableFuture.completedFuture(null));
        CompletableFuture<Void> first = committer.request();
        runNext();
        CompletableFuture<Void> second = committer.request();
        time.sleep(2000);
        upload.completeExceptionally(new IllegalStateException("upload failed"));
        assertTrue(first.isCompletedExceptionally());
        assertFalse(second.isDone());
        assertEquals(0, delays.peekFirst());
        runNext();
        assertTrue(second.isDone());
        assertFalse(second.isCompletedExceptionally());
    }

    /** A synchronous WAL failure must release the running slot so a subsequent request can retry. */
    @Test
    void testSynchronousFailureDoesNotStrandRetry() {
        when(wal.commit(0, false)).thenThrow(new IllegalStateException("rejected"))
            .thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> first = committer.request();
        runNext();
        assertTrue(first.isCompletedExceptionally());
        CompletableFuture<Void> retry = committer.request();
        runNext();
        assertTrue(retry.isDone());
        assertFalse(retry.isCompletedExceptionally());
    }

    private void runNext() {
        time.sleep(TimeUnit.NANOSECONDS.toMillis(delays.removeFirst()));
        tasks.removeFirst().run();
    }
}
