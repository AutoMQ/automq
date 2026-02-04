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

import org.apache.kafka.tools.automq.perf.Stats.CumulativeStats;
import org.apache.kafka.tools.automq.perf.Stats.PeriodStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Stats}.
 */
public class StatsTest {

    private Stats stats;

    @BeforeEach
    void setUp() {
        stats = new Stats();
    }

    @Test
    void testMaxSendTimeNanosUpdatedOnMessageReceived() {
        // This test prevents regression of the time base mismatch bug.
        // maxSendTimeNanos should be updated when messageReceived is called.
        long sendTime = StatsCollector.currentNanos();
        
        stats.messageReceived(1, 100, sendTime);
        
        assertEquals(sendTime, stats.maxSendTimeNanos.get());
    }

    @Test
    void testMaxSendTimeNanosOnlyUpdatesWhenLarger() {
        long olderTime = 1000L;
        long newerTime = 2000L;
        
        // First update with newer time
        stats.messageReceived(1, 100, newerTime);
        assertEquals(newerTime, stats.maxSendTimeNanos.get());
        
        // Try to update with older time - should not change
        stats.messageReceived(1, 100, olderTime);
        assertEquals(newerTime, stats.maxSendTimeNanos.get());
        
        // Update with even newer time - should change
        long newestTime = 3000L;
        stats.messageReceived(1, 100, newestTime);
        assertEquals(newestTime, stats.maxSendTimeNanos.get());
    }

    @Test
    void testMaxSendTimeNanosUsesEpochTimeBase() {
        // Ensure maxSendTimeNanos uses the same time base as StatsCollector.currentNanos()
        // Epoch time for year 2020+ should be > 1.5 * 10^18 nanoseconds
        long sendTime = StatsCollector.currentNanos();
        assertTrue(sendTime > 1_500_000_000_000_000_000L, 
            "sendTime should be epoch-based (> 1.5 * 10^18 for year 2020+)");
        
        stats.messageReceived(1, 100, sendTime);
        
        assertTrue(stats.maxSendTimeNanos.get() > 1_500_000_000_000_000_000L,
            "maxSendTimeNanos should be epoch-based");
    }

    @Test
    void testMessageSentUpdatesCounters() {
        long sendTime = StatsCollector.currentNanos();
        
        stats.messageSent(100, sendTime);
        stats.messageSent(200, sendTime);
        
        PeriodStats period = stats.toPeriodStats();
        assertEquals(2, period.messagesSent);
        assertEquals(300, period.bytesSent);
    }

    @Test
    void testMessageReceivedUpdatesCounters() {
        long sendTime = StatsCollector.currentNanos();
        
        stats.messageReceived(5, 500, sendTime);
        stats.messageReceived(3, 300, sendTime);
        
        PeriodStats period = stats.toPeriodStats();
        assertEquals(8, period.messagesReceived);
        assertEquals(800, period.bytesReceived);
    }

    @Test
    void testMessageFailedUpdatesCounters() {
        stats.messageFailed();
        stats.messageFailed();
        stats.messageFailed();
        
        PeriodStats period = stats.toPeriodStats();
        assertEquals(3, period.messagesSendFailed);
    }

    @Test
    void testResetClearsAllCounters() {
        long sendTime = StatsCollector.currentNanos();
        
        // Add some data
        stats.messageSent(100, sendTime);
        stats.messageReceived(1, 100, sendTime);
        stats.messageFailed();
        
        // Reset
        CumulativeStats beforeReset = stats.reset();
        
        // Verify reset returned the cumulative stats
        assertEquals(1, beforeReset.totalMessagesSent);
        assertEquals(1, beforeReset.totalMessagesReceived);
        assertEquals(1, beforeReset.totalMessagesSendFailed);
        
        // Verify counters are cleared
        PeriodStats afterReset = stats.toPeriodStats();
        assertEquals(0, afterReset.messagesSent);
        assertEquals(0, afterReset.messagesReceived);
        assertEquals(0, afterReset.messagesSendFailed);
        assertEquals(0, afterReset.totalMessagesSent);
        assertEquals(0, afterReset.totalMessagesReceived);
    }

    @Test
    void testToPeriodStatsResetsIntervalCounters() {
        long sendTime = StatsCollector.currentNanos();
        
        stats.messageSent(100, sendTime);
        stats.messageReceived(1, 100, sendTime);
        
        // First call should return the data
        PeriodStats first = stats.toPeriodStats();
        assertEquals(1, first.messagesSent);
        assertEquals(1, first.messagesReceived);
        
        // Second call should return zeros (interval counters reset)
        PeriodStats second = stats.toPeriodStats();
        assertEquals(0, second.messagesSent);
        assertEquals(0, second.messagesReceived);
        
        // But total counters should still reflect the data
        assertEquals(1, second.totalMessagesSent);
        assertEquals(1, second.totalMessagesReceived);
    }

    @Test
    void testToCumulativeStatsDoesNotModifyCounters() {
        long sendTime = StatsCollector.currentNanos();
        
        stats.messageSent(100, sendTime);
        stats.messageReceived(1, 100, sendTime);
        
        // Call toCumulativeStats multiple times
        CumulativeStats first = stats.toCumulativeStats();
        CumulativeStats second = stats.toCumulativeStats();
        
        // Both should return the same values
        assertEquals(first.totalMessagesSent, second.totalMessagesSent);
        assertEquals(first.totalMessagesReceived, second.totalMessagesReceived);
        assertEquals(1, first.totalMessagesSent);
        assertEquals(1, first.totalMessagesReceived);
    }

    @Test
    void testConcurrentUpdates() throws InterruptedException {
        int numThreads = 10;
        int updatesPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < updatesPerThread; j++) {
                        long sendTime = StatsCollector.currentNanos();
                        stats.messageSent(100, sendTime);
                        stats.messageReceived(1, 100, sendTime);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        
        CumulativeStats cumulative = stats.toCumulativeStats();
        assertEquals(numThreads * updatesPerThread, cumulative.totalMessagesSent);
        assertEquals(numThreads * updatesPerThread, cumulative.totalMessagesReceived);
    }
}
