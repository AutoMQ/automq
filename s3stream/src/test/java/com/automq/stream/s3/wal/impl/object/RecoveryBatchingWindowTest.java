/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.utils.MockTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers automatic expiry, bounded frequency, scope, and listener lifecycle. */
@Tag("S3Unit")
public class RecoveryBatchingWindowTest {
    /** Given a handoff, expire its two-thirds-second burst without extending an active burst. */
    @Test
    public void testWindowExpiresAndCannotBeExtended() {
        MockTime time = new MockTime(0, 0, 0);
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        long normal = TimeUnit.MILLISECONDS.toNanos(250);
        assertEquals(normal, window.batchIntervalNanos(normal));
        assertTrue(window.startRecovery());
        time.sleep(666);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(50), window.batchIntervalNanos(normal));
        assertFalse(window.startRecovery());
        time.sleep(1);
        assertEquals(normal, window.batchIntervalNanos(normal));
        assertTrue(window.startRecovery());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(50), window.batchIntervalNanos(normal));
    }

    /** Given three recovery events, a fourth must wait for the oldest start to leave the rolling minute. */
    @Test
    public void testThreeBurstsShareRollingMinuteBudget() {
        MockTime time = new MockTime(0, 0, 0);
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        assertTrue(window.startRecovery());
        time.sleep(1000);
        assertTrue(window.startRecovery());
        time.sleep(1000);
        assertTrue(window.startRecovery());
        time.sleep(1000);
        assertFalse(window.startRecovery());
        time.sleep(56999);
        assertFalse(window.startRecovery());
        time.sleep(1);
        assertTrue(window.startRecovery());
        time.sleep(999);
        assertFalse(window.startRecovery());
        time.sleep(1);
        assertTrue(window.startRecovery());
    }

    /** Given simultaneous topology triggers, exactly one may admit a burst and notify writers. */
    @Test
    public void testConcurrentStartsDoNotMultiplyBudget() throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        AtomicInteger notifications = new AtomicInteger();
        window.addListener(notifications::incrementAndGet);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<Boolean>> starts = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                starts.add(executor.submit(window::startRecovery));
            }
            int admitted = 0;
            for (Future<Boolean> start : starts) {
                if (start.get(5, TimeUnit.SECONDS)) admitted++;
            }
            assertEquals(1, admitted);
            assertEquals(1, notifications.get());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Negative nanoTime origins are valid and a faster configured batch must never be slowed. */
    @Test
    public void testNegativeClockOriginAndFasterSetting() {
        MockTime time = new MockTime(0, 0, -TimeUnit.SECONDS.toNanos(100));
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        assertTrue(window.startRecovery());
        long faster = TimeUnit.MILLISECONDS.toNanos(10);
        assertEquals(faster, window.batchIntervalNanos(faster));
        time.sleep(2000);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(250), window.batchIntervalNanos(TimeUnit.MILLISECONDS.toNanos(250)));
    }

    /** Writers can unsubscribe and one failed callback must not prevent the other writer's wakeup. */
    @Test
    public void testListenerLifecycleAndFailureIsolation() {
        MockTime time = new MockTime();
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        AtomicInteger calls = new AtomicInteger();
        Runnable listener = calls::incrementAndGet;
        window.addListener(() -> {
            throw new IllegalStateException("injected callback failure");
        });
        window.addListener(listener);
        assertTrue(window.startRecovery());
        assertEquals(1, calls.get());
        assertFalse(window.startRecovery());
        assertEquals(1, calls.get());
        window.removeListener(listener);
        time.sleep(60000);
        assertTrue(window.startRecovery());
        assertEquals(1, calls.get());
    }

    /** Router and data WALs share a policy only when they belong to the same process-local broker ID. */
    @Test
    public void testBrokerScope() {
        assertSame(RecoveryBatchingWindow.forNode(12001), RecoveryBatchingWindow.forNode(12001));
        assertNotSame(RecoveryBatchingWindow.forNode(12001), RecoveryBatchingWindow.forNode(12002));
    }
}
