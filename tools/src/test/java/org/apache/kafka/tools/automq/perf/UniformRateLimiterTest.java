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

package org.apache.kafka.tools.automq.perf;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link UniformRateLimiter}.
 */
public class UniformRateLimiterTest {

    @Test
    void testAcquireReturnsIncreasingTimes() {
        double rate = 1000.0; // 1000 per second
        UniformRateLimiter limiter = new UniformRateLimiter(rate);
        
        long first = limiter.acquire();
        long second = limiter.acquire();
        long third = limiter.acquire();
        
        assertTrue(second > first, "Second acquire should return later time than first");
        assertTrue(third > second, "Third acquire should return later time than second");
    }

    @Test
    void testAcquireIntervalMatchesRate() {
        double rate = 1000.0; // 1000 per second = 1ms interval
        UniformRateLimiter limiter = new UniformRateLimiter(rate);
        
        long first = limiter.acquire();
        long second = limiter.acquire();
        
        long intervalNanos = second - first;
        long expectedIntervalNanos = TimeUnit.SECONDS.toNanos(1) / (long) rate; // 1ms = 1,000,000 ns
        
        // Allow 10% tolerance
        long tolerance = expectedIntervalNanos / 10;
        assertTrue(Math.abs(intervalNanos - expectedIntervalNanos) <= tolerance,
            "Interval should be approximately " + expectedIntervalNanos + " ns, but was " + intervalNanos + " ns");
    }

    @Test
    void testLowRate() {
        double rate = 10.0; // 10 per second = 100ms interval
        UniformRateLimiter limiter = new UniformRateLimiter(rate);
        
        long first = limiter.acquire();
        long second = limiter.acquire();
        
        long intervalNanos = second - first;
        long expectedIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);
        
        // Allow 10% tolerance
        long tolerance = expectedIntervalNanos / 10;
        assertTrue(Math.abs(intervalNanos - expectedIntervalNanos) <= tolerance,
            "Interval should be approximately 100ms, but was " + TimeUnit.NANOSECONDS.toMillis(intervalNanos) + "ms");
    }

    @Test
    void testHighRate() {
        double rate = 100000.0; // 100,000 per second = 10us interval
        UniformRateLimiter limiter = new UniformRateLimiter(rate);
        
        long first = limiter.acquire();
        long second = limiter.acquire();
        
        long intervalNanos = second - first;
        long expectedIntervalNanos = TimeUnit.MICROSECONDS.toNanos(10);
        
        // Allow 20% tolerance for high rates
        long tolerance = expectedIntervalNanos / 5;
        assertTrue(Math.abs(intervalNanos - expectedIntervalNanos) <= tolerance,
            "Interval should be approximately 10us, but was " + intervalNanos + "ns");
    }

    @Test
    void testMultipleAcquiresAccumulateCorrectly() {
        double rate = 1000.0; // 1000 per second
        UniformRateLimiter limiter = new UniformRateLimiter(rate);
        
        long first = limiter.acquire();
        
        // Acquire 100 times
        long last = first;
        for (int i = 0; i < 100; i++) {
            last = limiter.acquire();
        }
        
        // Total time should be approximately 100ms (100 acquires at 1000/s)
        long totalNanos = last - first;
        long expectedNanos = TimeUnit.MILLISECONDS.toNanos(100);
        
        // Allow 10% tolerance
        long tolerance = expectedNanos / 10;
        assertTrue(Math.abs(totalNanos - expectedNanos) <= tolerance,
            "Total time for 100 acquires should be approximately 100ms, but was " + 
            TimeUnit.NANOSECONDS.toMillis(totalNanos) + "ms");
    }

    @Test
    void testUninterruptibleSleepNs() {
        // Test that uninterruptibleSleepNs works correctly
        long targetTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10);
        
        long before = System.nanoTime();
        UniformRateLimiter.uninterruptibleSleepNs(targetTime);
        long after = System.nanoTime();
        
        assertTrue(after >= targetTime, 
            "Should sleep until at least the target time");
        
        // Should not overshoot by too much (allow 5ms tolerance)
        long overshoot = after - targetTime;
        assertTrue(overshoot < TimeUnit.MILLISECONDS.toNanos(5),
            "Should not overshoot by more than 5ms, but overshot by " + 
            TimeUnit.NANOSECONDS.toMillis(overshoot) + "ms");
    }

    @Test
    void testUninterruptibleSleepNsWithPastTime() {
        // If target time is in the past, should return immediately
        long pastTime = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(100);
        
        long before = System.nanoTime();
        UniformRateLimiter.uninterruptibleSleepNs(pastTime);
        long after = System.nanoTime();
        
        // Should return almost immediately (within 1ms)
        long elapsed = after - before;
        assertTrue(elapsed < TimeUnit.MILLISECONDS.toNanos(1),
            "Should return immediately for past time, but took " + 
            TimeUnit.NANOSECONDS.toMicros(elapsed) + "us");
    }
}
