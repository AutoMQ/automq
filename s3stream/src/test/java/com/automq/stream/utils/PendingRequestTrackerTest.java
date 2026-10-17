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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
public class PendingRequestTrackerTest {
    private static final long THRESHOLD_NANOS = TimeUnit.SECONDS.toNanos(30);

    /**
     * Given a pending request younger than the threshold, the tracker should not report it as stalled.
     */
    @Test
    public void testPendingLatencyBeforeThreshold() {
        AtomicLong now = new AtomicLong();
        PendingRequestTracker tracker = new PendingRequestTracker(THRESHOLD_NANOS, now::get);

        tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(29));

        assertEquals(0, tracker.pendingLatencyNanos());
        tracker.close();
    }

    /**
     * Given multiple overdue requests, the tracker should report the oldest unfinished request age.
     */
    @Test
    public void testPendingLatencyReportsOldestOverdueRequest() {
        AtomicLong now = new AtomicLong();
        PendingRequestTracker tracker = new PendingRequestTracker(THRESHOLD_NANOS, now::get);

        tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(10));
        tracker.rotate();
        tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(45));
        tracker.rotate();

        assertEquals(TimeUnit.SECONDS.toNanos(45), tracker.pendingLatencyNanos());
        tracker.close();
    }

    /**
     * Given a request starts late in a rotation window, the tracker should keep it until it crosses the threshold.
     */
    @Test
    public void testRequestLateInWindowIsNotDroppedBeforeThreshold() {
        AtomicLong now = new AtomicLong(TimeUnit.SECONDS.toNanos(14));
        PendingRequestTracker tracker = new PendingRequestTracker(THRESHOLD_NANOS, now::get);

        tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(30));
        tracker.rotate();
        assertEquals(0, tracker.pendingLatencyNanos());

        now.set(TimeUnit.SECONDS.toNanos(45));
        tracker.rotate();
        assertEquals(TimeUnit.SECONDS.toNanos(31), tracker.pendingLatencyNanos());
        tracker.close();
    }

    /**
     * Given time passes without rotation, pending latency reads should not rotate buckets by themselves.
     */
    @Test
    public void testPendingLatencyReadDoesNotRotateBuckets() {
        AtomicLong now = new AtomicLong();
        PendingRequestTracker tracker = new PendingRequestTracker(THRESHOLD_NANOS, now::get);

        tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(45));

        assertEquals(0, tracker.pendingLatencyNanos());

        rotateTwice(tracker);

        assertEquals(TimeUnit.SECONDS.toNanos(45), tracker.pendingLatencyNanos());
        tracker.close();
    }

    /**
     * Given an overdue request completes, the tracker should remove it from future pending latency reads.
     */
    @Test
    public void testCompletedOverdueRequestIsRemoved() {
        AtomicLong now = new AtomicLong();
        PendingRequestTracker tracker = new PendingRequestTracker(THRESHOLD_NANOS, now::get);

        PendingRequestTracker.Handle handle = tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(45));
        rotateTwice(tracker);
        assertEquals(TimeUnit.SECONDS.toNanos(45), tracker.pendingLatencyNanos());

        handle.close();

        assertEquals(0, tracker.pendingLatencyNanos());
        tracker.close();
    }

    /**
     * Given requests complete out of order, the tracker should keep only unfinished overdue requests visible.
     */
    @Test
    public void testOutOfOrderCompletionDoesNotCreateGhostPendingLatency() {
        AtomicLong now = new AtomicLong();
        PendingRequestTracker tracker = new PendingRequestTracker(THRESHOLD_NANOS, now::get);

        PendingRequestTracker.Handle first = tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(10));
        tracker.rotate();
        PendingRequestTracker.Handle second = tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(45));
        tracker.rotate();

        second.close();

        assertEquals(TimeUnit.SECONDS.toNanos(45), tracker.pendingLatencyNanos());

        first.close();

        assertEquals(0, tracker.pendingLatencyNanos());
        tracker.close();
    }

    /**
     * Given a handle is closed repeatedly, the tracker should keep removal idempotent.
     */
    @Test
    public void testHandleCloseIsIdempotent() {
        AtomicLong now = new AtomicLong();
        PendingRequestTracker tracker = new PendingRequestTracker(THRESHOLD_NANOS, now::get);

        PendingRequestTracker.Handle handle = tracker.begin();
        now.set(TimeUnit.SECONDS.toNanos(45));
        rotateTwice(tracker);
        assertEquals(TimeUnit.SECONDS.toNanos(45), tracker.pendingLatencyNanos());

        handle.close();
        handle.close();

        assertEquals(0, tracker.pendingLatencyNanos());
        tracker.close();
    }

    private static void rotateTwice(PendingRequestTracker tracker) {
        tracker.rotate();
        tracker.rotate();
    }
}
