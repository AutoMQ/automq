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
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Collects local Broker availability facts and exposes immutable snapshots for Controller attribution.
 */
public class BrokerAvailabilityMonitor {
    private final int brokerId;
    private final long brokerEpoch;
    private final long appendStuckThresholdNanos;
    private final long coldReadStuckThresholdNanos;
    private final long partitionCloseStuckThresholdNanos;
    private final int logReadFailureThreshold;
    private final int maxReadFailureEntries;
    private final LongSupplier nanoTimeSupplier;

    private final PendingOperations appendPendings = new PendingOperations();
    private final PendingOperations coldReadPendings = new PendingOperations();
    private final Map<TopicPartition, Long> partitionCloseStarts = new ConcurrentHashMap<>();
    private final Map<SignalKey, AvailabilitySignal> windowSignals = new ConcurrentHashMap<>();
    private final Map<TopicPartitionOffset, Integer> readFailures = new ConcurrentHashMap<>();

    public BrokerAvailabilityMonitor(int brokerId, long brokerEpoch, long appendStuckThresholdNanos,
                                     long coldReadStuckThresholdNanos, long partitionCloseStuckThresholdNanos,
                                     int logReadFailureThreshold, LongSupplier nanoTimeSupplier) {
        this(brokerId, brokerEpoch, appendStuckThresholdNanos, coldReadStuckThresholdNanos,
            partitionCloseStuckThresholdNanos, logReadFailureThreshold, 10000, nanoTimeSupplier);
    }

    public BrokerAvailabilityMonitor(int brokerId, long brokerEpoch, long appendStuckThresholdNanos,
                                     long coldReadStuckThresholdNanos, long partitionCloseStuckThresholdNanos,
                                     int logReadFailureThreshold, int maxReadFailureEntries,
                                     LongSupplier nanoTimeSupplier) {
        this.brokerId = brokerId;
        this.brokerEpoch = brokerEpoch;
        this.appendStuckThresholdNanos = appendStuckThresholdNanos;
        this.coldReadStuckThresholdNanos = coldReadStuckThresholdNanos;
        this.partitionCloseStuckThresholdNanos = partitionCloseStuckThresholdNanos;
        this.logReadFailureThreshold = logReadFailureThreshold;
        this.maxReadFailureEntries = maxReadFailureEntries;
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    public void recordAppendPending(CompletableFuture<?> appendFuture, long startNanos) {
        appendPendings.track(appendFuture, startNanos);
        appendFuture.whenComplete((ignored, throwable) -> appendPendings.untrack(appendFuture));
    }

    public void recordColdReadPending(CompletableFuture<?> readFuture, long startNanos) {
        coldReadPendings.track(readFuture, startNanos);
        readFuture.whenComplete((ignored, throwable) -> coldReadPendings.untrack(readFuture));
    }

    public OperationHandle recordPartitionCloseStarted(TopicPartition topicPartition) {
        partitionCloseStarts.put(topicPartition, nanoTimeSupplier.getAsLong());
        return () -> partitionCloseStarts.remove(topicPartition);
    }

    public void recordPartitionRecoverFailed(TopicPartition topicPartition, String phase, int retryCount,
                                             String context) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("phase", phase);
        attributes.put("retryCount", Integer.toString(retryCount));
        attributes.put("failed", "true");
        attributes.put("context", context == null ? "" : context);
        recordSignal(AvailabilitySignalType.PARTITION_RECOVER_FAILED, target(topicPartition), attributes);
    }

    public void recordLogWriteFailed(TopicPartition topicPartition) {
        recordSignal(AvailabilitySignalType.LOG_WRITE_FAIL, target(topicPartition));
    }

    public void recordLogReadFailed(TopicPartition topicPartition, long offset) {
        TopicPartitionOffset key = new TopicPartitionOffset(topicPartition, offset);
        if (!readFailures.containsKey(key) && readFailures.size() >= maxReadFailureEntries) {
            readFailures.clear();
        }
        int failures = readFailures.merge(key, 1, Integer::sum);
        if (failures >= logReadFailureThreshold) {
            recordSignal(AvailabilitySignalType.LOG_READ_FAIL,
                AvailabilityTarget.topicPartitionOffset(topicPartition.topic(), topicPartition.partition(), offset));
        }
    }

    public void recordLogReadSucceeded(TopicPartition topicPartition, long offset) {
        readFailures.remove(new TopicPartitionOffset(topicPartition, offset));
    }

    public BrokerAvailabilitySnapshot snapshot(long nowMs, long windowStartMs, long windowEndMs) {
        long nowNanos = nanoTimeSupplier.getAsLong();
        if (appendPendings.hasPendingOlderThan(nowNanos, appendStuckThresholdNanos)) {
            recordSignal(AvailabilitySignalType.APPEND_STUCK, AvailabilityTarget.broker(brokerId, brokerEpoch));
        }
        if (coldReadPendings.hasPendingOlderThan(nowNanos, coldReadStuckThresholdNanos)
            || ColdReadInflightRegistry.hasPendingOlderThan(coldReadStuckThresholdNanos)) {
            recordSignal(AvailabilitySignalType.COLD_READ_STUCK, AvailabilityTarget.broker(brokerId, brokerEpoch));
        }
        partitionCloseStarts.forEach((topicPartition, startNanos) -> {
            if (nowNanos - startNanos >= partitionCloseStuckThresholdNanos) {
                recordSignal(AvailabilitySignalType.PARTITION_CLOSE_STUCK, target(topicPartition));
            }
        });
        List<AvailabilitySignal> signals = new ArrayList<>(new LinkedHashMap<>(windowSignals).values());
        return new BrokerAvailabilitySnapshot(brokerId, brokerEpoch, nowMs, windowStartMs, windowEndMs, signals);
    }

    public void clearWindow() {
        windowSignals.clear();
        readFailures.clear();
    }

    private void recordSignal(AvailabilitySignalType type, AvailabilityTarget target, Map<String, String> attributes) {
        AvailabilitySignal signal = new AvailabilitySignal(type, target);
        signal.setAttributes(new HashMap<>(attributes));
        windowSignals.put(new SignalKey(type, target), signal);
    }

    private void recordSignal(AvailabilitySignalType type, AvailabilityTarget target) {
        windowSignals.put(new SignalKey(type, target), new AvailabilitySignal(type, target));
    }

    private AvailabilityTarget target(TopicPartition topicPartition) {
        return AvailabilityTarget.topicPartition(topicPartition.topic(), topicPartition.partition());
    }

    public interface OperationHandle extends AutoCloseable {
        @Override
        void close();
    }

    private static final class PendingOperations {
        private final Map<CompletableFuture<?>, Long> starts = new ConcurrentHashMap<>();

        private void track(CompletableFuture<?> future, long startNanos) {
            starts.put(future, startNanos);
        }

        private void untrack(CompletableFuture<?> future) {
            starts.remove(future);
        }

        private boolean hasPendingOlderThan(long nowNanos, long thresholdNanos) {
            return starts.values().stream().anyMatch(startNanos -> nowNanos - startNanos >= thresholdNanos);
        }
    }

    private static final class SignalKey {
        private final AvailabilitySignalType type;
        private final AvailabilityTarget target;

        private SignalKey(AvailabilitySignalType type, AvailabilityTarget target) {
            this.type = type;
            this.target = target;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SignalKey)) {
                return false;
            }
            SignalKey signalKey = (SignalKey) o;
            return type == signalKey.type && Objects.equals(target, signalKey.target);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, target);
        }
    }

    private static final class TopicPartitionOffset {
        private final TopicPartition topicPartition;
        private final long offset;

        private TopicPartitionOffset(TopicPartition topicPartition, long offset) {
            this.topicPartition = topicPartition;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TopicPartitionOffset)) {
                return false;
            }
            TopicPartitionOffset that = (TopicPartitionOffset) o;
            return offset == that.offset && Objects.equals(topicPartition, that.topicPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicPartition, offset);
        }
    }
}
