/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Deterministic, no-sleep tests for response-delay admission, expiry and cancellation. */
@Tag("S3Unit")
class LeaderHintRecoveryTest {
    private final MockTime time = new MockTime();
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ArrayDeque<Runnable> polls = new ArrayDeque<>();
    private final List<Long> delays = new ArrayList<>();
    private LeaderHintRecovery recovery;

    @BeforeEach
    void setUp() {
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS))).thenAnswer(invocation -> {
            polls.add(invocation.getArgument(0));
            delays.add(invocation.getArgument(1));
            return mock(ScheduledFuture.class);
        });
        recovery = new LeaderHintRecovery(scheduler, time);
    }

    /** Given an already valid hint or no request budget, no timer or admission slot is used. */
    @Test
    void testImmediateCompletion() {
        assertTrue(recovery.await(() -> true, Long.MAX_VALUE).isDone());
        assertTrue(recovery.await(() -> false, 0).isDone());
        assertTrue(recovery.await(() -> false, -1).isDone());
        assertTrue(polls.isEmpty());
    }

    /** Given publication after the former 300ms expiry, the reply is released immediately with a usable hint. */
    @Test
    void testPublishedHintReleasesResponse() {
        AtomicBoolean ready = new AtomicBoolean();
        CompletableFuture<Void> result = recovery.await(ready::get, Long.MAX_VALUE);
        assertFalse(result.isDone());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(10), delays.get(0));
        time.sleep(10);
        polls.remove().run();
        assertFalse(result.isDone());
        time.sleep(340);
        ready.set(true);
        polls.remove().run();
        assertTrue(result.isDone());
        assertTrue(polls.isEmpty());
    }

    /** Given a leader that never appears, the one-second ceiling completes the unchanged-error path. */
    @Test
    void testDeadlineDoesNotExtend() {
        CompletableFuture<Void> result = recovery.await(() -> false, Long.MAX_VALUE);
        for (int i = 0; i < 99; i++) {
            time.sleep(10);
            polls.remove().run();
            assertFalse(result.isDone());
        }
        time.sleep(10);
        polls.remove().run();
        assertTrue(result.isDone());
        assertTrue(polls.isEmpty());
    }

    /** Given less request time than the poll interval, no timer exceeds the remaining budget. */
    @Test
    void testShortRequestBudget() {
        CompletableFuture<Void> result = recovery.await(() -> false, TimeUnit.MILLISECONDS.toNanos(7));
        assertEquals(TimeUnit.MILLISECONDS.toNanos(7), delays.get(0));
        time.sleep(7);
        polls.remove().run();
        assertTrue(result.isDone());
    }

    /** Given 64 pending responses, extra responses fall back immediately and cancellation returns capacity. */
    @Test
    void testBoundedAdmissionAndCancellation() {
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            pending.add(recovery.await(() -> false, Long.MAX_VALUE));
            assertFalse(pending.get(i).isDone());
        }
        assertTrue(recovery.await(() -> false, Long.MAX_VALUE).isDone());
        pending.get(0).cancel(false);
        polls.remove().run();
        assertEquals(63, polls.size());
        assertFalse(recovery.await(() -> false, Long.MAX_VALUE).isDone());
        assertEquals(64, polls.size());
    }

    /** Given executor rejection, the error response is not stranded and the admission slot is returned. */
    @Test
    void testSchedulingFailureFallsBack() {
        doThrow(new RejectedExecutionException("stopped")).when(scheduler)
            .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS));
        for (int i = 0; i < 100; i++) {
            assertTrue(recovery.await(() -> false, Long.MAX_VALUE).isDone());
        }
        assertTrue(polls.isEmpty());
    }

    /** Given a failed hint inspection during polling, the unchanged error still completes. */
    @Test
    void testSupplierFailureFallsBack() {
        AtomicBoolean fail = new AtomicBoolean();
        CompletableFuture<Void> result = recovery.await(() -> {
            if (fail.get()) {
                throw new IllegalStateException("metadata no longer available");
            }
            return false;
        }, Long.MAX_VALUE);
        fail.set(true);
        polls.remove().run();
        assertTrue(result.isDone());
        assertTrue(polls.isEmpty());
    }

    /** Given admitted failure replies, shutdown observes their snapshot without waiting on later traffic. */
    @Test
    void testShutdownSnapshotDoesNotGrow() {
        assertTrue(recovery.pendingResponses().isDone());
        CompletableFuture<Void> first = recovery.await(() -> false, Long.MAX_VALUE);
        CompletableFuture<Void> snapshot = recovery.pendingResponses();
        CompletableFuture<Void> later = recovery.await(() -> false, Long.MAX_VALUE);
        assertFalse(snapshot.isDone());
        first.cancel(false);
        assertTrue(snapshot.isDone());
        assertFalse(later.isDone());
        later.cancel(false);
        assertTrue(recovery.pendingResponses().isDone());
    }

    /** Given a published hint, shutdown still waits until its actual response-send callback completes. */
    @Test
    void testShutdownWaitsForSocketSendNotHintPublication() {
        AtomicBoolean ready = new AtomicBoolean();
        CompletableFuture<Void> sent = new CompletableFuture<>();
        CompletableFuture<Void> hint = recovery.await(ready::get, Long.MAX_VALUE, sent);
        CompletableFuture<Void> drained = recovery.pendingResponses();
        ready.set(true);
        time.sleep(10);
        polls.remove().run();
        assertTrue(hint.isDone());
        assertFalse(drained.isDone());
        sent.complete(null);
        assertTrue(drained.isDone());
        polls.remove().run();
        assertTrue(polls.isEmpty());
    }

    /** Given an abandoned socket, the same one-second deadline releases shutdown and admission capacity. */
    @Test
    void testAbandonedSocketHasBoundedRetention() {
        AtomicBoolean ready = new AtomicBoolean();
        CompletableFuture<Void> sent = new CompletableFuture<>();
        CompletableFuture<Void> hint = recovery.await(ready::get, Long.MAX_VALUE, sent);
        ready.set(true);
        time.sleep(100);
        polls.remove().run();
        assertTrue(hint.isDone());
        assertFalse(sent.isDone());
        time.sleep(900);
        polls.remove().run();
        assertTrue(sent.isDone());
        assertTrue(recovery.pendingResponses().isDone());
        assertTrue(polls.isEmpty());
    }
}
