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

import kafka.automq.availability.action.SkippedReadRangeRegistry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Static bridge used by Scala data-path classes until BrokerServer wires a broker-owned monitor instance.
 */
public final class AvailabilityRuntimeHooks {
    private static final AtomicReference<BrokerAvailabilityMonitor> MONITOR = new AtomicReference<>();
    private static final AtomicReference<SkippedReadRangeRegistry> SKIPPED_READ_RANGES =
        new AtomicReference<>(new SkippedReadRangeRegistry());
    private static final AtomicReference<OpenRecoverActionConsumer> OPEN_RECOVER_ACTION_CONSUMER =
        new AtomicReference<>();

    private AvailabilityRuntimeHooks() {
    }

    public static void register(BrokerAvailabilityMonitor monitor) {
        MONITOR.set(monitor);
    }

    public static void unregister(BrokerAvailabilityMonitor monitor) {
        MONITOR.compareAndSet(monitor, null);
    }

    public static Optional<BrokerAvailabilityMonitor> monitor() {
        return Optional.ofNullable(MONITOR.get());
    }

    public static SkippedReadRangeRegistry skippedReadRanges() {
        return SKIPPED_READ_RANGES.get();
    }

    public static void registerSkippedReadRanges(SkippedReadRangeRegistry registry) {
        SKIPPED_READ_RANGES.set(registry);
    }

    public static void registerOpenRecoverActionConsumer(OpenRecoverActionConsumer consumer) {
        OPEN_RECOVER_ACTION_CONSUMER.set(consumer);
    }

    public static void unregisterOpenRecoverActionConsumer(OpenRecoverActionConsumer consumer) {
        OPEN_RECOVER_ACTION_CONSUMER.compareAndSet(consumer, null);
    }

    public static BrokerAvailabilityMonitor.OperationHandle recordPartitionCloseStarted(TopicPartition topicPartition) {
        BrokerAvailabilityMonitor monitor = MONITOR.get();
        if (monitor == null) {
            return () -> { };
        }
        return monitor.recordPartitionCloseStarted(topicPartition);
    }

    public static void recordAppendPending(CompletableFuture<?> appendFuture, long startNanos) {
        BrokerAvailabilityMonitor monitor = MONITOR.get();
        if (monitor != null) {
            monitor.recordAppendPending(appendFuture, startNanos);
        }
    }

    public static void recordPartitionRecoverFailed(TopicPartition topicPartition, String phase, int retryCount,
                                                    String context) {
        BrokerAvailabilityMonitor monitor = MONITOR.get();
        if (monitor != null) {
            monitor.recordPartitionRecoverFailed(topicPartition, phase, retryCount, context);
        }
    }

    public static void recordLogWriteFailed(TopicPartition topicPartition) {
        BrokerAvailabilityMonitor monitor = MONITOR.get();
        if (monitor != null) {
            monitor.recordLogWriteFailed(topicPartition);
        }
    }

    public static void recordLogReadFailed(TopicPartition topicPartition, long offset) {
        BrokerAvailabilityMonitor monitor = MONITOR.get();
        if (monitor != null) {
            monitor.recordLogReadFailed(topicPartition, offset);
        }
    }

    public static void recordLogReadSucceeded(TopicPartition topicPartition, long offset) {
        BrokerAvailabilityMonitor monitor = MONITOR.get();
        if (monitor != null) {
            monitor.recordLogReadSucceeded(topicPartition, offset);
        }
    }

    public static long adjustReadStartOffset(TopicPartition topicPartition, long startOffset) {
        return SKIPPED_READ_RANGES.get().adjustStartOffset(topicPartition, startOffset);
    }

    public static boolean consumeOpenRecoverAction(AvailabilityActionType actionType,
                                                   TopicPartition topicPartition,
                                                   OpenRecoverActionOperation operation) {
        OpenRecoverActionConsumer consumer = OPEN_RECOVER_ACTION_CONSUMER.get();
        return consumer != null && consumer.consume(actionType, topicPartition, operation);
    }

    public interface OpenRecoverActionConsumer {
        boolean consume(AvailabilityActionType actionType, TopicPartition topicPartition,
                        OpenRecoverActionOperation operation);
    }

    public interface OpenRecoverActionOperation {
        void run(RecoveryAction action) throws Exception;
    }
}
