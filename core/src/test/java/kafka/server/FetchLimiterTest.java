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

package kafka.server;

import kafka.server.Limiter.AcquireContext;
import kafka.server.Limiter.Permit;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers fetch memory admission, permit lifecycle, and slow-draining connection isolation. */
@Tag("S3Unit")
public class FetchLimiterTest {

    private static final long SOFT_THRESHOLD = 50;
    private static final long HARD_THRESHOLD = 100;
    private ExecutorService executor;

    @BeforeEach
    public void setUp() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    /** Given long-sized demand, admission oversells once at the hard edge and releases retained capacity exactly. */
    @Test
    public void testLongPermitsOversellAndReleaseLifecycle() throws Exception {
        long hardThreshold = (long) Integer.MAX_VALUE + 100;
        FetchLimiter limiter = newLimiter(hardThreshold / 2, hardThreshold, "test");
        Permit first = acquire(limiter, hardThreshold - 10, "first");
        Permit oversold = acquire(limiter, 20, "oversold");

        assertEquals(hardThreshold + 10, limiter.usedPermits());
        assertEquals(0, limiter.availablePermits());
        assertNull(tryAcquire(limiter, 1, "blocked", 5));

        assertTrue(oversold.releaseTo(5));
        assertEquals(hardThreshold - 5, limiter.usedPermits());
        assertEquals(5, limiter.availablePermits());
        oversold.close();
        first.close();
        oversold.close();

        assertEquals(0, limiter.usedPermits());
        assertEquals(hardThreshold, limiter.availablePermits());
    }

    /** Given hard pressure, a connection retaining a ready response for 10 ms is classified as slow-draining. */
    @Test
    public void testClassifiesConnectionAtSlowDrainThreshold() throws Exception {
        MockTime time = new MockTime();
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test", time);
        Permit permit = acquire(limiter, HARD_THRESHOLD, "slow");

        permit.markResponseReady();
        Future<Permit> waiting = submitAcquire(limiter, 1, "waiting");
        waitForWaitingThreads(limiter, 1);
        time.sleep(10);
        triggerGrantWaiterProbe(limiter);

        assertTrue(limiter.isSlowDraining("slow"));
        assertTrue(waiting.cancel(true));
        waitForWaitingThreads(limiter, 0);
        permit.close();
    }

    /** Given the first probe precedes response readiness, the same hard waiter probes again after it becomes eligible. */
    @Test
    public void testHardWaiterRepeatsProbeUntilResponseBecomesEligible() throws Exception {
        MockTime time = new MockTime();
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test", time);
        Permit permit = acquire(limiter, HARD_THRESHOLD, "slow");
        Future<Permit> waiting = submitAcquire(limiter, 1, "waiting");
        waitForWaitingThreads(limiter, 1);

        assertNull(tryAcquire(limiter, 1, "probe-observer", 20));
        assertFalse(limiter.isSlowDraining("slow"));

        permit.markResponseReady();
        time.sleep(10);
        triggerGrantWaiterProbe(limiter);
        assertTrue(limiter.isSlowDraining("slow"));

        assertTrue(waiting.cancel(true));
        waitForWaitingThreads(limiter, 0);
        permit.close();
    }

    /** Given a classified connection, its future fetches use soft capacity while normal fetches use hard capacity. */
    @Test
    public void testClassifiedConnectionUsesSoftCapacity() throws Exception {
        MockTime time = new MockTime();
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test", time);
        Permit classified = classifyConnection(limiter, time, "slow", HARD_THRESHOLD);
        classified.close();
        Permit normalLoad = acquire(limiter, 60, "normal-load");

        assertNull(tryAcquire(limiter, 1, "slow", 5));
        Permit normal = acquire(limiter, 1, "normal");
        assertEquals(61, limiter.usedPermits());

        normal.close();
        normalLoad.close();
    }

    /** Given normal and classified connections, execute routes their tasks to the corresponding executor. */
    @Test
    public void testExecuteIsolatesSlowDrainingConnection() throws Exception {
        MockTime time = new MockTime();
        AtomicInteger normalExecutions = new AtomicInteger();
        AtomicInteger slowDrainingExecutions = new AtomicInteger();
        FetchLimiter limiter = new FetchLimiter(
            SOFT_THRESHOLD,
            HARD_THRESHOLD,
            "test",
            time,
            task -> {
                normalExecutions.incrementAndGet();
                task.run();
            },
            task -> {
                slowDrainingExecutions.incrementAndGet();
                task.run();
            },
            new TestSlowDrainStrategy()
        );

        limiter.execute("normal", () -> { });
        Permit classified = classifyConnection(limiter, time, "slow", HARD_THRESHOLD);
        limiter.execute("slow", () -> { });

        assertEquals(1, normalExecutions.get());
        assertEquals(1, slowDrainingExecutions.get());
        classified.close();
    }

    /** Given queued soft and hard acquisitions, hard priority and FIFO order are preserved as capacity is released. */
    @Test
    public void testHardPriorityAndFifoOrdering() throws Exception {
        MockTime time = new MockTime();
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test", time);
        Permit classified = classifyConnection(limiter, time, "slow", HARD_THRESHOLD);
        classified.close();
        Permit blocker = acquire(limiter, HARD_THRESHOLD, "blocker");

        Future<Permit> soft = submitAcquire(limiter, HARD_THRESHOLD, "slow");
        waitForWaitingThreads(limiter, 1);
        Future<Permit> hard1 = submitAcquire(limiter, HARD_THRESHOLD, "hard-1");
        waitForWaitingThreads(limiter, 2);
        Future<Permit> hard2 = submitAcquire(limiter, HARD_THRESHOLD, "hard-2");
        waitForWaitingThreads(limiter, 3);

        blocker.close();
        Permit hardPermit1 = hard1.get(5, TimeUnit.SECONDS);
        assertFalse(hard2.isDone());
        assertFalse(soft.isDone());

        hardPermit1.close();
        Permit hardPermit2 = hard2.get(5, TimeUnit.SECONDS);
        assertFalse(soft.isDone());

        hardPermit2.close();
        soft.get(5, TimeUnit.SECONDS).close();
    }

    /** Given a request entered as hard, later classification does not change that queued request to soft. */
    @Test
    public void testQueuedRequestIsNotReclassified() throws Exception {
        MockTime time = new MockTime();
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test", time);
        Permit ready = acquire(limiter, 60, "target");
        ready.markResponseReady();
        Permit blocker = acquire(limiter, 40, "blocker");
        Future<Permit> queuedAsHard = submitAcquire(limiter, HARD_THRESHOLD, "target");
        waitForWaitingThreads(limiter, 1);
        time.sleep(10);
        triggerGrantWaiterProbe(limiter);
        waitForCondition(
            () -> limiter.isSlowDraining("target"),
            5000,
            "The queued hard acquisition did not classify its connection");

        blocker.close();
        Permit queuedPermit = queuedAsHard.get(5, TimeUnit.SECONDS);
        assertEquals(160, limiter.usedPermits());

        queuedPermit.close();
        ready.close();
    }

    /** Given a classified connection, a later response completed within 10 ms restores normal admission. */
    @Test
    public void testRecoversConnectionBelowSlowDrainThreshold() throws Exception {
        MockTime time = new MockTime();
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test", time);
        Permit slowPermit = acquire(limiter, HARD_THRESHOLD, "slow");
        slowPermit.markResponseReady();
        Future<Permit> waiting = submitAcquire(limiter, 1, "probe-waiter");
        waitForWaitingThreads(limiter, 1);
        time.sleep(10);
        triggerGrantWaiterProbe(limiter);
        assertTrue(waiting.cancel(true));
        waitForWaitingThreads(limiter, 0);
        slowPermit.close();

        Permit recoveredPermit = acquire(limiter, SOFT_THRESHOLD, "slow");
        recoveredPermit.markResponseReady();
        time.sleep(9);
        recoveredPermit.close();

        assertFalse(limiter.isSlowDraining("slow"));
    }

    /** Given a slow connection has no response activity for one minute, acquire-driven cleanup removes its state. */
    @Test
    public void testExpiresInactiveSlowDrainingConnection() throws Exception {
        MockTime time = new MockTime();
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test", time);
        Permit slowPermit = classifyConnection(limiter, time, "slow", HARD_THRESHOLD);
        slowPermit.close();

        time.sleep(TimeUnit.SECONDS.toMillis(59));
        acquire(limiter, 0, "observer").close();
        assertTrue(limiter.isSlowDraining("slow"));

        time.sleep(TimeUnit.SECONDS.toMillis(1));
        acquire(limiter, 0, "observer").close();
        assertFalse(limiter.isSlowDraining("slow"));
    }

    /** Given blocked acquisitions time out or are interrupted, their queue entries and permits do not leak. */
    @Test
    public void testTimedOutAndInterruptedWaitersAreRemoved() throws Exception {
        FetchLimiter limiter = newLimiter(SOFT_THRESHOLD, HARD_THRESHOLD, "test");
        Permit blocker = acquire(limiter, HARD_THRESHOLD, "blocker");

        assertNull(tryAcquire(limiter, 1, "timed-out", 5));
        assertEquals(0, limiter.waitingThreads());

        Future<Permit> interrupted = submitAcquire(limiter, 1, "interrupted");
        waitForWaitingThreads(limiter, 1);
        assertTrue(interrupted.cancel(true));
        waitForWaitingThreads(limiter, 0);
        assertEquals(HARD_THRESHOLD, limiter.usedPermits());

        blocker.close();
        acquire(limiter, HARD_THRESHOLD, "next").close();
        assertEquals(0, limiter.usedPermits());
    }

    private static Permit acquire(FetchLimiter limiter, long permits, String connectionId) throws Exception {
        return limiter.acquire(permits, new AcquireContext(50, connectionId));
    }

    private static FetchLimiter newLimiter(long softThreshold, long hardThreshold, String name) {
        return newLimiter(softThreshold, hardThreshold, name, Time.SYSTEM);
    }

    private static FetchLimiter newLimiter(long softThreshold, long hardThreshold, String name, Time time) {
        return new FetchLimiter(
            softThreshold,
            hardThreshold,
            name,
            time,
            Runnable::run,
            Runnable::run,
            new TestSlowDrainStrategy()
        );
    }

    private static Permit tryAcquire(FetchLimiter limiter, long permits, String connectionId, long timeoutMs)
        throws Exception {
        return limiter.acquire(permits, new AcquireContext(timeoutMs, connectionId));
    }

    private Future<Permit> submitAcquire(FetchLimiter limiter, long permits, String connectionId) {
        return executor.submit(() -> limiter.acquire(permits, new AcquireContext(0, connectionId)));
    }

    private Permit classifyConnection(
        FetchLimiter limiter,
        MockTime time,
        String connectionId,
        long permits
    ) throws Exception {
        Permit permit = acquire(limiter, permits, connectionId);
        permit.markResponseReady();
        Future<Permit> waiting = submitAcquire(limiter, 1, "probe-waiter");
        waitForWaitingThreads(limiter, 1);
        time.sleep(10);
        triggerGrantWaiterProbe(limiter);
        assertTrue(limiter.isSlowDraining(connectionId));
        assertTrue(waiting.cancel(true));
        waitForWaitingThreads(limiter, 0);
        return permit;
    }

    private static void waitForWaitingThreads(FetchLimiter limiter, int expected) throws InterruptedException {
        waitForCondition(
            () -> limiter.waitingThreads() == expected,
            5000,
            () -> "Expected " + expected + " waiting threads, but found " + limiter.waitingThreads());
    }

    /** Enqueues a zero-permit request to exercise the grant path while an aged hard waiter is queued. */
    private void triggerGrantWaiterProbe(FetchLimiter limiter) throws Exception {
        assertNull(limiter.acquire(0, new AcquireContext(1, "probe-trigger")));
    }

    private static void waitForCondition(BooleanSupplier condition, long timeoutMs, String failureMessage)
        throws InterruptedException {
        waitForCondition(condition, timeoutMs, () -> failureMessage);
    }

    private static void waitForCondition(BooleanSupplier condition, long timeoutMs, Supplier<String> failureMessage)
        throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean() && System.nanoTime() < deadlineNanos) {
            Thread.sleep(10);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError(failureMessage.get());
        }
    }

    private static final class TestSlowDrainStrategy implements FetchLimiterSlowDrainStrategy {
        @Override
        public List<String> select(FetchLimiter limiter, long nowNanos) {
            Set<String> selected = new LinkedHashSet<>();
            for (FetchLimiter.Permit permit : limiter.responseReadyPermits) {
                if (permit.connectionId != null
                    && nowNanos - permit.responseReadyNanos >= FetchLimiter.SLOW_DRAIN_THRESHOLD_NANOS) {
                    selected.add(permit.connectionId);
                }
            }
            return List.copyOf(selected);
        }
    }
}
