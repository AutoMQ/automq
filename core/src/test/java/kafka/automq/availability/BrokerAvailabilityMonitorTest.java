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

package kafka.automq.availability;

import com.automq.stream.s3.cache.blockcache.ColdReadInflightRegistry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.controller.availability.AvailabilitySignal;
import org.apache.kafka.controller.availability.AvailabilitySignalType;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class BrokerAvailabilityMonitorTest {
    private final AtomicLong nowNanos = new AtomicLong(0L);
    private final BrokerAvailabilityMonitor monitor = new BrokerAvailabilityMonitor(1, 10L,
        30L, 30L, 30L, 3, nowNanos::get);

    @Test
    public void testEmptySnapshotIsFreshCoverageInput() {
        BrokerAvailabilitySnapshot snapshot = monitor.snapshot(100L, 70L, 100L);

        assertEquals(1, snapshot.getBrokerId());
        assertEquals(10L, snapshot.getBrokerEpoch());
        assertTrue(snapshot.getSignals().isEmpty());
    }

    @Test
    public void testAppendAndColdReadStuckDependOnPendingFuture() {
        CompletableFuture<Void> appendFuture = new CompletableFuture<>();
        CompletableFuture<Void> readFuture = new CompletableFuture<>();
        monitor.recordAppendPending(appendFuture, 0L);
        monitor.recordColdReadPending(readFuture, 0L);

        nowNanos.set(31L);
        assertSignalTypes(monitor.snapshot(100L, 70L, 100L),
            AvailabilitySignalType.APPEND_STUCK, AvailabilitySignalType.COLD_READ_STUCK);

        monitor.clearWindow();
        appendFuture.complete(null);
        readFuture.complete(null);
        assertTrue(monitor.snapshot(101L, 71L, 101L).getSignals().isEmpty());
    }

    @Test
    public void testColdReadStuckUsesS3StreamInflightRegistry() {
        CompletableFuture<Void> readFuture = new CompletableFuture<>();
        BrokerAvailabilityMonitor monitor = new BrokerAvailabilityMonitor(1, 10L,
            30L, 0L, 30L, 3, nowNanos::get);
        ColdReadInflightRegistry.track(readFuture, System.nanoTime());

        try {
            assertSignalTypes(monitor.snapshot(100L, 70L, 100L), AvailabilitySignalType.COLD_READ_STUCK);

            monitor.clearWindow();
            ColdReadInflightRegistry.untrack(readFuture);
            assertTrue(monitor.snapshot(101L, 71L, 101L).getSignals().isEmpty());
        } finally {
            ColdReadInflightRegistry.clear();
        }
    }

    @Test
    public void testPartitionCloseHandleStopsCloseStuckSignal() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        BrokerAvailabilityMonitor.OperationHandle handle = monitor.recordPartitionCloseStarted(topicPartition);
        nowNanos.set(31L);

        assertSignalTypes(monitor.snapshot(100L, 70L, 100L), AvailabilitySignalType.PARTITION_CLOSE_STUCK);

        monitor.clearWindow();
        handle.close();
        assertTrue(monitor.snapshot(101L, 71L, 101L).getSignals().isEmpty());
    }

    @Test
    public void testRecoverAndWriteFailuresRecordPartitionSignals() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);

        monitor.recordPartitionRecoverFailed(topicPartition, "load", 2, "ctx");
        monitor.recordLogWriteFailed(topicPartition);

        BrokerAvailabilitySnapshot snapshot = monitor.snapshot(100L, 70L, 100L);
        assertSignalTypes(snapshot, AvailabilitySignalType.PARTITION_RECOVER_FAILED, AvailabilitySignalType.LOG_WRITE_FAIL);
        assertTrue(snapshot.getSignals().stream().anyMatch(signal ->
            "load".equals(signal.getAttributes().get("phase"))));
    }

    @Test
    public void testLogReadFailureFiresOnThirdSameOffsetFailureAndSuccessResets() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);

        monitor.recordLogReadFailed(topicPartition, 42L);
        monitor.recordLogReadFailed(topicPartition, 42L);
        assertTrue(monitor.snapshot(100L, 70L, 100L).getSignals().isEmpty());
        monitor.recordLogReadFailed(topicPartition, 42L);
        BrokerAvailabilitySnapshot snapshot = monitor.snapshot(101L, 71L, 101L);
        assertSignalTypes(snapshot, AvailabilitySignalType.LOG_READ_FAIL);
        AvailabilitySignal signal = snapshot.getSignals().get(0);
        assertEquals(42L, signal.getTarget().getOffset());
        assertTrue(signal.getAttributes().isEmpty());

        monitor.clearWindow();
        monitor.recordLogReadSucceeded(topicPartition, 42L);
        monitor.recordLogReadFailed(topicPartition, 42L);
        assertFalse(monitor.snapshot(102L, 72L, 102L).hasSignal(AvailabilitySignalType.LOG_READ_FAIL));
    }

    @Test
    public void testReadFailuresAreBoundedAndClearedWithPublishWindow() {
        BrokerAvailabilityMonitor boundedMonitor = new BrokerAvailabilityMonitor(1, 10L,
            30L, 30L, 30L, 3, 2, nowNanos::get);
        TopicPartition topicPartition = new TopicPartition("topic", 0);

        boundedMonitor.recordLogReadFailed(topicPartition, 1L);
        boundedMonitor.recordLogReadFailed(topicPartition, 2L);
        boundedMonitor.recordLogReadFailed(topicPartition, 3L);
        boundedMonitor.recordLogReadFailed(topicPartition, 1L);
        boundedMonitor.recordLogReadFailed(topicPartition, 1L);
        assertFalse(boundedMonitor.snapshot(100L, 70L, 100L).hasSignal(AvailabilitySignalType.LOG_READ_FAIL));

        boundedMonitor.recordLogReadFailed(topicPartition, 4L);
        boundedMonitor.recordLogReadFailed(topicPartition, 4L);
        boundedMonitor.recordLogReadFailed(topicPartition, 4L);
        assertTrue(boundedMonitor.snapshot(101L, 71L, 101L).hasSignal(AvailabilitySignalType.LOG_READ_FAIL));

        boundedMonitor.clearWindow();
        boundedMonitor.recordLogReadFailed(topicPartition, 4L);
        assertFalse(boundedMonitor.snapshot(102L, 72L, 102L).hasSignal(AvailabilitySignalType.LOG_READ_FAIL));
    }

    @Test
    public void testRuntimeHooksForwardSignalsToRegisteredMonitor() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        AvailabilityRuntimeHooks.register(monitor);
        try {
            BrokerAvailabilityMonitor.OperationHandle handle =
                AvailabilityRuntimeHooks.recordPartitionCloseStarted(topicPartition);
            nowNanos.set(31L);
            AvailabilityRuntimeHooks.recordPartitionRecoverFailed(topicPartition, "recoverLog", 1, "ctx");
            AvailabilityRuntimeHooks.recordLogWriteFailed(topicPartition);
            AvailabilityRuntimeHooks.recordLogReadFailed(topicPartition, 42L);
            AvailabilityRuntimeHooks.recordLogReadFailed(topicPartition, 42L);
            AvailabilityRuntimeHooks.recordLogReadFailed(topicPartition, 42L);

            assertSignalTypes(monitor.snapshot(100L, 70L, 100L),
                AvailabilitySignalType.PARTITION_CLOSE_STUCK,
                AvailabilitySignalType.PARTITION_RECOVER_FAILED,
                AvailabilitySignalType.LOG_WRITE_FAIL,
                AvailabilitySignalType.LOG_READ_FAIL);

            monitor.clearWindow();
            handle.close();
            AvailabilityRuntimeHooks.recordLogReadSucceeded(topicPartition, 42L);
            AvailabilityRuntimeHooks.unregister(monitor);
            AvailabilityRuntimeHooks.recordLogWriteFailed(topicPartition);
            assertTrue(monitor.snapshot(101L, 71L, 101L).getSignals().isEmpty());
        } finally {
            AvailabilityRuntimeHooks.unregister(monitor);
        }
    }

    private void assertSignalTypes(BrokerAvailabilitySnapshot snapshot, AvailabilitySignalType... expectedTypes) {
        Set<AvailabilitySignalType> actualTypes = snapshot.getSignals().stream()
            .map(AvailabilitySignal::getType)
            .collect(Collectors.toSet());
        assertEquals(Set.of(expectedTypes), actualTypes);
    }
}
