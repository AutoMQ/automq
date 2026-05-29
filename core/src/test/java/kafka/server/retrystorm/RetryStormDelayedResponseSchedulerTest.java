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

package kafka.server.retrystorm;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Covers delayed response scheduler timing, shutdown, and exactly-once send behavior. */
@Tag("S3Unit")
public class RetryStormDelayedResponseSchedulerTest {

    /** Given zero delay, scheduler executes the send callback synchronously. */
    @Test
    public void testDelayMsZeroSendsImmediately() {
        RetryStormDelayedResponseScheduler scheduler = new RetryStormDelayedResponseScheduler(10L);
        AtomicInteger sends = new AtomicInteger(0);
        try {
            scheduler.schedule(0L, sends::incrementAndGet);
            assertEquals(1, sends.get());
        } finally {
            scheduler.shutdown();
        }
    }

    /** Given positive delay, scheduler defers the callback until the timer fires. */
    @Test
    public void testPositiveDelaySendsLater() throws Exception {
        RetryStormDelayedResponseScheduler scheduler = new RetryStormDelayedResponseScheduler(10L);
        AtomicInteger sends = new AtomicInteger(0);
        try {
            scheduler.schedule(30L, sends::incrementAndGet);
            assertEquals(0, sends.get());
            eventually(1000L, () -> assertEquals(1, sends.get()));
        } finally {
            scheduler.shutdown();
        }
    }

    /** Given pending delayed responses at shutdown, scheduler sends each pending callback once. */
    @Test
    public void testShutdownSendsPendingResponsesOnce() throws Exception {
        RetryStormDelayedResponseScheduler scheduler = new RetryStormDelayedResponseScheduler(1000L);
        AtomicInteger sends = new AtomicInteger(0);
        scheduler.schedule(10000L, sends::incrementAndGet);

        scheduler.shutdown();
        assertEquals(1, sends.get());

        Thread.sleep(50L);
        assertEquals(1, sends.get());
    }

    /** Given scheduler is already shut down, later schedules fall back to immediate send. */
    @Test
    public void testScheduleAfterShutdownSendsImmediately() {
        RetryStormDelayedResponseScheduler scheduler = new RetryStormDelayedResponseScheduler(1000L);
        AtomicInteger sends = new AtomicInteger(0);

        scheduler.shutdown();
        scheduler.schedule(10000L, sends::incrementAndGet);

        assertEquals(1, sends.get());
    }

    private static void eventually(long timeoutMs, CheckedAssertion assertion) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        AssertionError lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                assertion.run();
                return;
            } catch (AssertionError e) {
                lastError = e;
                Thread.sleep(10L);
            }
        }
        if (lastError != null) {
            throw lastError;
        }
    }

    @FunctionalInterface
    private interface CheckedAssertion {
        void run() throws Exception;
    }
}
