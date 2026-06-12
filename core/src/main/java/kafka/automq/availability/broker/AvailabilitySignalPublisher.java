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

package kafka.automq.availability.broker;

import kafka.automq.availability.BrokerAvailabilityMonitor;
import kafka.automq.availability.transport.AvailabilityKvRequestSender;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.availability.AvailabilitySignal;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 * Publishes fresh Broker availability snapshots, including empty signal sets for coverage.
 */
public class AvailabilitySignalPublisher {
    private static final Logger LOGGER = new LogContext("[AVAILABILITY_FALLBACK] ").logger(AvailabilitySignalPublisher.class);

    private final BrokerAvailabilityMonitor monitor;
    private final AvailabilityKvRequestSender sender;
    private final Time time;
    private long lastWindowStartMs = -1L;

    public AvailabilitySignalPublisher(BrokerAvailabilityMonitor monitor, AvailabilityKvRequestSender sender,
                                       Time time) {
        this.monitor = monitor;
        this.sender = sender;
        this.time = time;
    }

    public CompletableFuture<Void> publishOnceAsync() {
        long nowMs = time.milliseconds();
        long windowStartMs = lastWindowStartMs < 0 ? nowMs : lastWindowStartMs;
        BrokerAvailabilitySnapshot snapshot = monitor.snapshot(nowMs, windowStartMs, nowMs);
        if (!snapshot.getSignals().isEmpty()) {
            LOGGER.info("Publish availability signals brokerId={} brokerEpoch={} signalCount={} signals={}",
                snapshot.getBrokerId(), snapshot.getBrokerEpoch(), snapshot.getSignals().size(),
                describeSignals(snapshot.getSignals()));
        }
        if (sender == null) {
            monitor.clearWindow();
            lastWindowStartMs = nowMs;
            return CompletableFuture.completedFuture(null);
        }
        return sender.putSignal(snapshot).thenRun(() -> {
            monitor.clearWindow();
            lastWindowStartMs = nowMs;
        });
    }

    private List<String> describeSignals(List<AvailabilitySignal> signals) {
        return signals.stream()
            .map(signal -> "type=" + signal.getType() + ",target=" + signal.getTarget())
            .collect(Collectors.toList());
    }
}
