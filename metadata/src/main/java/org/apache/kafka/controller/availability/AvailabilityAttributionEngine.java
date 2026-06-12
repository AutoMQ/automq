/*
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

package org.apache.kafka.controller.availability;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class AvailabilityAttributionEngine {
    private static final Set<AvailabilitySignalType> NODE_SIGNALS = Set.of(
        AvailabilitySignalType.APPEND_STUCK,
        AvailabilitySignalType.COLD_READ_STUCK,
        AvailabilitySignalType.PARTITION_CLOSE_STUCK
    );

    private final double globalAffectedRatio;
    private final int smallClusterAffectedBrokers;

    public AvailabilityAttributionEngine(double globalAffectedRatio, int smallClusterAffectedBrokers) {
        this.globalAffectedRatio = globalAffectedRatio;
        this.smallClusterAffectedBrokers = smallClusterAffectedBrokers;
    }

    public FaultAttribution attribute(ClusterAvailabilitySnapshot snapshot) {
        Optional<FaultAttribution> coverageFailure = validateCoverage(snapshot);
        if (coverageFailure.isPresent()) {
            return coverageFailure.get();
        }

        List<BrokerAvailabilitySnapshot> coveredSnapshots = snapshot.coverageBrokers().stream()
            .map(broker -> snapshot.brokerSnapshots().get(broker.brokerId()))
            .collect(Collectors.toList());
        int coverageBaseSize = coveredSnapshots.size();
        long globalAffected = coveredSnapshots.stream()
            .filter(brokerSnapshot -> brokerSnapshot.hasSignal(AvailabilitySignalType.APPEND_STUCK) ||
                brokerSnapshot.hasSignal(AvailabilitySignalType.COLD_READ_STUCK))
            .count();
        if (globalAffected > 0 &&
            ((coverageBaseSize == 3 && globalAffected >= smallClusterAffectedBrokers) ||
                ((double) globalAffected / (double) coverageBaseSize) >= globalAffectedRatio)) {
            return new FaultAttribution(FaultAttributionType.GLOBAL_SHARED_STORAGE_SUSPECT,
                AvailabilityTarget.cluster(), null, "node-level storage signal reached global threshold");
        }

        List<BrokerAvailabilitySnapshot> nodeAffected = coveredSnapshots.stream()
            .filter(brokerSnapshot -> brokerSnapshot.hasSignal(AvailabilitySignalType.APPEND_STUCK) ||
                brokerSnapshot.hasSignal(AvailabilitySignalType.COLD_READ_STUCK) ||
                brokerSnapshot.hasSignal(AvailabilitySignalType.PARTITION_CLOSE_STUCK))
            .collect(Collectors.toList());
        if (nodeAffected.size() > 1) {
            return new FaultAttribution(FaultAttributionType.GLOBAL_SHARED_STORAGE_SUSPECT,
                AvailabilityTarget.cluster(), null, "multiple brokers reported node-level signals");
        }
        if (nodeAffected.size() == 1) {
            BrokerAvailabilitySnapshot brokerSnapshot = nodeAffected.get(0);
            AvailabilitySignalType signalType = firstNodeSignal(brokerSnapshot);
            return new FaultAttribution(FaultAttributionType.NODE_LOCAL_FAILURE,
                AvailabilityTarget.broker(brokerSnapshot.getBrokerId(), brokerSnapshot.getBrokerEpoch()),
                signalType, "single broker node-level signal");
        }

        return coveredSnapshots.stream()
            .flatMap(brokerSnapshot -> brokerSnapshot.getSignals().stream())
            .filter(signal -> !NODE_SIGNALS.contains(signal.getType()))
            .min(Comparator.comparing(signal -> signal.getType().ordinal()))
            .map(signal -> new FaultAttribution(typeFor(signal.getType()), signal.getTarget(), signal.getType(),
                "partition/log-local signal"))
            .orElseGet(FaultAttribution::none);
    }

    private Optional<FaultAttribution> validateCoverage(ClusterAvailabilitySnapshot snapshot) {
        if (snapshot.coverageBrokers().isEmpty()) {
            return Optional.of(insufficient("coverage base is empty"));
        }
        Map<Integer, BrokerAvailabilitySnapshot> brokerSnapshots = snapshot.brokerSnapshots();
        for (CoverageBroker broker : snapshot.coverageBrokers()) {
            BrokerAvailabilitySnapshot brokerSnapshot = brokerSnapshots.get(broker.brokerId());
            if (brokerSnapshot == null) {
                return Optional.of(insufficient("missing broker availability snapshot"));
            }
            if (brokerSnapshot.getSchemaVersion() != AvailabilityConstants.SCHEMA_VERSION) {
                return Optional.of(insufficient("incompatible availability signal schema"));
            }
            if (brokerSnapshot.getBrokerEpoch() != broker.brokerEpoch()) {
                return Optional.of(insufficient("broker epoch mismatch"));
            }
            if (snapshot.nowMs() - brokerSnapshot.getTimestampMs() > snapshot.staleMs()) {
                return Optional.of(insufficient("stale broker availability snapshot"));
            }
        }
        return Optional.empty();
    }

    private FaultAttribution insufficient(String reason) {
        return new FaultAttribution(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            AvailabilityTarget.cluster(), null, reason);
    }

    private AvailabilitySignalType firstNodeSignal(BrokerAvailabilitySnapshot snapshot) {
        if (snapshot.hasSignal(AvailabilitySignalType.APPEND_STUCK)) {
            return AvailabilitySignalType.APPEND_STUCK;
        }
        if (snapshot.hasSignal(AvailabilitySignalType.COLD_READ_STUCK)) {
            return AvailabilitySignalType.COLD_READ_STUCK;
        }
        return AvailabilitySignalType.PARTITION_CLOSE_STUCK;
    }

    private FaultAttributionType typeFor(AvailabilitySignalType signalType) {
        switch (signalType) {
            case PARTITION_RECOVER_FAILED:
                return FaultAttributionType.PARTITION_RECOVER_FAILED;
            case LOG_WRITE_FAIL:
                return FaultAttributionType.LOG_WRITE_FAIL;
            case LOG_READ_FAIL:
                return FaultAttributionType.LOG_READ_FAIL;
            default:
                return FaultAttributionType.NONE;
        }
    }
}
