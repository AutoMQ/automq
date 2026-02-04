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

import org.apache.kafka.tools.automq.perf.StatsCollector.StopCondition;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link StatsCollector}.
 */
public class StatsCollectorTest {

    @Test
    void testCurrentNanosReturnsEpochBasedTime() {
        // This test prevents regression of the time base mismatch bug.
        // currentNanos() should return epoch-based nanoseconds, not System.nanoTime().
        
        long currentNanos = StatsCollector.currentNanos();
        
        // Epoch time for year 2020+ should be > 1.5 * 10^18 nanoseconds
        // System.nanoTime() returns JVM-relative time which is typically much smaller
        assertTrue(currentNanos > 1_500_000_000_000_000_000L,
            "currentNanos() should return epoch-based time (> 1.5 * 10^18 for year 2020+), " +
            "but got: " + currentNanos);
    }

    @Test
    void testCurrentNanosMatchesInstantNow() {
        // Verify currentNanos() is consistent with Instant.now()
        Instant before = Instant.now();
        long currentNanos = StatsCollector.currentNanos();
        Instant after = Instant.now();
        
        long beforeNanos = TimeUnit.SECONDS.toNanos(before.getEpochSecond()) + before.getNano();
        long afterNanos = TimeUnit.SECONDS.toNanos(after.getEpochSecond()) + after.getNano();
        
        assertTrue(currentNanos >= beforeNanos, 
            "currentNanos should be >= before instant");
        assertTrue(currentNanos <= afterNanos + TimeUnit.MILLISECONDS.toNanos(10), 
            "currentNanos should be <= after instant (with small tolerance)");
    }

    @Test
    void testCurrentNanosIsMonotonicallyIncreasing() {
        long first = StatsCollector.currentNanos();
        
        // Small delay to ensure time passes
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        long second = StatsCollector.currentNanos();
        
        assertTrue(second >= first, 
            "currentNanos() should be monotonically increasing");
    }

    @Test
    void testCurrentNanosNotSystemNanoTime() {
        // System.nanoTime() is JVM-relative and typically much smaller than epoch time
        long systemNano = System.nanoTime();
        long currentNanos = StatsCollector.currentNanos();
        
        // The difference should be huge (epoch time is ~10^18, System.nanoTime is ~10^14 or less)
        // If they were the same base, the difference would be small
        long diff = Math.abs(currentNanos - systemNano);
        
        assertTrue(diff > 1_000_000_000_000_000_000L,
            "currentNanos() should use different time base than System.nanoTime(). " +
            "currentNanos=" + currentNanos + ", System.nanoTime()=" + systemNano);
    }

    @Test
    void testStopConditionWithDuration() {
        // Test duration-based stop condition
        Duration duration = Duration.ofSeconds(5);
        StopCondition condition = (startNanos, nowNanos) -> 
            Duration.ofNanos(nowNanos - startNanos).compareTo(duration) >= 0;
        
        long start = System.nanoTime();
        
        // Should not stop immediately
        assertFalse(condition.shouldStop(start, start));
        
        // Should not stop after 1 second
        long after1Sec = start + TimeUnit.SECONDS.toNanos(1);
        assertFalse(condition.shouldStop(start, after1Sec));
        
        // Should stop after 5 seconds
        long after5Sec = start + TimeUnit.SECONDS.toNanos(5);
        assertTrue(condition.shouldStop(start, after5Sec));
        
        // Should stop after 10 seconds
        long after10Sec = start + TimeUnit.SECONDS.toNanos(10);
        assertTrue(condition.shouldStop(start, after10Sec));
    }

    @Test
    void testStopConditionWithCatchUp() {
        // Test catch-up stop condition (used in backlog/cold-read scenario)
        // This is the condition that was broken by the time base mismatch bug
        Stats stats = new Stats();
        long targetTime = StatsCollector.currentNanos();
        
        StopCondition condition = (startNanos, nowNanos) -> 
            stats.maxSendTimeNanos.get() >= targetTime;
        
        // Should not stop initially (maxSendTimeNanos is 0)
        assertFalse(condition.shouldStop(0, 0));
        
        // Should not stop after receiving message with older timestamp
        stats.messageReceived(1, 100, targetTime - 1000);
        assertFalse(condition.shouldStop(0, 0));
        
        // Should stop after receiving message with target timestamp
        stats.messageReceived(1, 100, targetTime);
        assertTrue(condition.shouldStop(0, 0));
    }

    @Test
    void testStopConditionWithCatchUpUsingEpochTime() {
        // This test specifically validates the fix for the time base mismatch bug.
        // Both targetTime and sendTime must use the same time base (epoch).
        Stats stats = new Stats();
        
        // Simulate the backlog scenario:
        // 1. Record backlogEnd using epoch time (the fix)
        long backlogEnd = StatsCollector.currentNanos();
        
        // 2. Create stop condition
        StopCondition condition = (startNanos, nowNanos) -> 
            stats.maxSendTimeNanos.get() >= backlogEnd;
        
        // 3. Initially should not stop
        assertFalse(condition.shouldStop(0, 0));
        
        // 4. Receive message with send time before backlogEnd
        long sendTimeBeforeBacklog = backlogEnd - TimeUnit.SECONDS.toNanos(10);
        stats.messageReceived(1, 100, sendTimeBeforeBacklog);
        assertFalse(condition.shouldStop(0, 0), 
            "Should not stop when received message is from before backlog end");
        
        // 5. Receive message with send time at backlogEnd
        stats.messageReceived(1, 100, backlogEnd);
        assertTrue(condition.shouldStop(0, 0), 
            "Should stop when received message reaches backlog end time");
    }

    @Test
    void testStopConditionWithMismatchedTimeBaseWouldFail() {
        // This test demonstrates what happens with mismatched time bases.
        // It shows why the bug occurred and validates our understanding.
        Stats stats = new Stats();
        
        // BAD: Using System.nanoTime() for target (this was the bug)
        long badTargetTime = System.nanoTime(); // ~10^14 or less
        
        // Message send time uses epoch (StatsCollector.currentNanos())
        long sendTime = StatsCollector.currentNanos(); // ~10^18
        
        // The epoch time is always much larger than System.nanoTime()
        assertTrue(sendTime > badTargetTime,
            "Epoch time should always be > System.nanoTime(), " +
            "which would cause immediate stop condition satisfaction");
        
        // This demonstrates the bug: condition would be immediately true
        stats.messageReceived(1, 100, sendTime);
        assertTrue(stats.maxSendTimeNanos.get() >= badTargetTime,
            "With mismatched time bases, condition is always true");
    }
}
