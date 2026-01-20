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

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RouterPermitLimiterTest {

    @Test
    public void testAcquireClampsToMaxPermits() {
        Time time = new MockTime();
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireFailTimeNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 10, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        int acquired = limiter.acquire(100);

        assertEquals(10, acquired);
        // All permits consumed
        assertEquals(0, limiter.acquireUpTo(1));
    }

    @Test
    public void testAcquireUpToUsesAvailablePermits() {
        Time time = new MockTime();
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToFailTimeNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 10, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        // Consume 5 permits first
        limiter.acquire(5);
        int acquired = limiter.acquireUpTo(8);

        assertEquals(5, acquired);
        assertEquals(0, limiter.acquireUpTo(1));
    }

    @Test
    public void testAcquireUpToClampsToMaxPermits() {
        Time time = new MockTime();
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToClampNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 3, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        int acquired = limiter.acquireUpTo(8);

        assertEquals(3, acquired);
    }

    @Test
    public void testAcquireUpToReturnsZeroWhenNoPermits() {
        Time time = new MockTime();
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToNoPermitsNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 5, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        // Consume all permits
        limiter.acquire(5);
        int acquired = limiter.acquireUpTo(4);

        assertEquals(0, acquired);
    }

    @Test
    public void testAcquireUpToIgnoresNonPositiveSize() {
        Time time = new MockTime();
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterAcquireUpToNonPositiveNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 5, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        assertEquals(0, limiter.acquireUpTo(0));
        assertEquals(0, limiter.acquireUpTo(-1));
        // Permits should still be available
        assertEquals(5, limiter.acquireUpTo(10));
    }

    @Test
    public void testAcquirePreservesInterrupt() throws Exception {
        Time time = new MockTime();
        Histogram hist = new KafkaMetricsGroup(RouterPermitLimiterTest.class)
            .newHistogram("RouterPermitLimiterInterruptAcquireFailTimeNanos");
        RouterPermitLimiter limiter = new RouterPermitLimiter("[TEST]", time, 1, hist, LoggerFactory.getLogger(RouterPermitLimiterTest.class));

        // Consume the only permit
        limiter.acquire(1);

        CountDownLatch latch = new CountDownLatch(1);
        Thread releaser = new Thread(() -> {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }
            limiter.release(1);
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
