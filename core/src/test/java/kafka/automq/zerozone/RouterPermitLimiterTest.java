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

package kafka.automq.zerozone;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RouterPermitLimiterTest {

    @Test
    public void testAcquireClampsToMaxPermits() {
        Time time = new MockTime();
        Semaphore semaphore = new Semaphore(10);
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireFailTimeNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 10, semaphore, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        int acquired = limiter.acquire(100);

        assertEquals(10, acquired);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testAcquireUpToUsesAvailablePermits() {
        Time time = new MockTime();
        Semaphore semaphore = new Semaphore(5);
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToFailTimeNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 10, semaphore, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        int acquired = limiter.acquireUpTo(8);

        assertEquals(5, acquired);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testAcquireUpToClampsToMaxPermits() {
        Time time = new MockTime();
        Semaphore semaphore = new Semaphore(10);
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToClampNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 3, semaphore, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        int acquired = limiter.acquireUpTo(8);

        assertEquals(3, acquired);
        assertEquals(7, semaphore.availablePermits());
    }

    @Test
    public void testAcquireUpToReturnsZeroWhenNoPermits() {
        Time time = new MockTime();
        Semaphore semaphore = new Semaphore(0);
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToNoPermitsNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 5, semaphore, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        int acquired = limiter.acquireUpTo(4);

        assertEquals(0, acquired);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testAcquireUpToIgnoresNonPositiveSize() {
        Time time = new MockTime();
        Semaphore semaphore = new Semaphore(5);
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToNonPositiveNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 5, semaphore, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        assertEquals(0, limiter.acquireUpTo(0));
        assertEquals(0, limiter.acquireUpTo(-1));
        assertEquals(5, semaphore.availablePermits());
    }

    @Test
    public void testAcquirePreservesInterrupt() throws Exception {
        Time time = new MockTime();
        Semaphore semaphore = new Semaphore(0);
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterInterruptAcquireFailTimeNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 1, semaphore, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        Thread releaser = new Thread(() -> {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }
            semaphore.release(1);
        });
        releaser.start();

        Thread.currentThread().interrupt();
        // ensure the acquire is blocked and then release the permit
        new Thread(latch::countDown).start();
        limiter.acquire(1);

        assertTrue(Thread.currentThread().isInterrupted());
        // clear the interrupted status
        assertTrue(Thread.interrupted());
        releaser.join();
    }
}
