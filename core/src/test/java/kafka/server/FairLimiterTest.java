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

package kafka.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class FairLimiterTest {

    /** Given exhausted permits, a timed acquisition waits for a release within its millisecond budget. */
    @Test
    void testAcquireSucceedsWhenPermitsAreReleasedWithinTimeout() throws Exception {
        FairLimiter limiter = new FairLimiter(1, "test");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Limiter.Handler> waiting;
            try (Limiter.Handler blocker = limiter.acquire(1)) {
                assertEquals(1, blocker.permitsHeld());
                waiting = executor.submit(() -> limiter.acquire(1, 5000));
                assertThrows(TimeoutException.class, () -> waiting.get(50, TimeUnit.MILLISECONDS));
            }
            try (Limiter.Handler acquired = waiting.get(5, TimeUnit.SECONDS)) {
                assertNotNull(acquired);
                assertEquals(1, acquired.permitsHeld());
                assertEquals(0, limiter.availablePermits());
            }
            assertEquals(1, limiter.availablePermits());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /** Given no permit release, a timed acquisition consumes its millisecond budget before returning null. */
    @Test
    void testAcquireTimesOutInMilliseconds() throws Exception {
        FairLimiter limiter = new FairLimiter(1, "test");
        try (Limiter.Handler blocker = limiter.acquire(1)) {
            assertEquals(1, blocker.permitsHeld());
            long startNanos = System.nanoTime();
            assertNull(limiter.acquire(1, 100));
            long elapsedNanos = System.nanoTime() - startNanos;
            assertTrue(elapsedNanos >= TimeUnit.MILLISECONDS.toNanos(100),
                "The acquisition must not time out before its millisecond budget expires");
            assertEquals(0, limiter.availablePermits());
            assertEquals(0, limiter.waitingThreads());
        }
        assertEquals(1, limiter.availablePermits());
    }
}
