/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 * unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller.availability;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(40)
public class AvailabilityAttributionEngineTest {
    private final AvailabilityAttributionEngine engine = new AvailabilityAttributionEngine(0.4, 2);

    @Test
    public void testGlobalSharedStorageUsesCoverageBase() {
        FaultAttribution attribution = engine.attribute(cluster(List.of(
            broker(1, signal(AvailabilitySignalType.APPEND_STUCK, AvailabilityTarget.broker(1, 10L))),
            broker(2, signal(AvailabilitySignalType.COLD_READ_STUCK, AvailabilityTarget.broker(2, 20L))),
            broker(3)
        )));

        assertEquals(FaultAttributionType.GLOBAL_SHARED_STORAGE_SUSPECT, attribution.type());
    }

    @Test
    public void testMissingStaleSchemaOrEpochMismatchBlocksAsInsufficientCoverage() {
        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            engine.attribute(new ClusterAvailabilitySnapshot(List.of(new CoverageBroker(1, 1L)), Map.of(), 100L, 90L)).type());

        BrokerAvailabilitySnapshot stale = broker(1);
        stale.setTimestampMs(1L);
        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            engine.attribute(cluster(List.of(stale), 1000L, 90L)).type());

        BrokerAvailabilitySnapshot incompatible = broker(1);
        incompatible.setSchemaVersion(0);
        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            engine.attribute(cluster(List.of(incompatible))).type());

        BrokerAvailabilitySnapshot epochMismatch = broker(1);
        epochMismatch.setBrokerEpoch(99L);
        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            engine.attribute(new ClusterAvailabilitySnapshot(List.of(new CoverageBroker(1, 1L)),
                Map.of(1, epochMismatch), 100L, 90L)).type());
    }

    @Test
    public void testNodeLocalTakesPriorityOverPartitionLocalOnSameBroker() {
        FaultAttribution attribution = engine.attribute(cluster(List.of(
            broker(1,
                signal(AvailabilitySignalType.PARTITION_CLOSE_STUCK, AvailabilityTarget.topicPartition("topic", 0)),
                signal(AvailabilitySignalType.LOG_WRITE_FAIL, AvailabilityTarget.topicPartition("topic", 0))),
            broker(2),
            broker(3)
        )));

        assertEquals(FaultAttributionType.NODE_LOCAL_FAILURE, attribution.type());
        assertEquals(AvailabilitySignalType.PARTITION_CLOSE_STUCK, attribution.sourceSignalType());
    }

    @Test
    public void testMultipleNodeAffectedBrokersUseGlobalProtectionAttribution() {
        FaultAttribution attribution = engine.attribute(cluster(List.of(
            broker(1, signal(AvailabilitySignalType.PARTITION_CLOSE_STUCK, AvailabilityTarget.topicPartition("topic", 0))),
            broker(2, signal(AvailabilitySignalType.PARTITION_CLOSE_STUCK, AvailabilityTarget.topicPartition("topic", 1))),
            broker(3),
            broker(4),
            broker(5),
            broker(6)
        )));

        assertEquals(FaultAttributionType.GLOBAL_SHARED_STORAGE_SUSPECT, attribution.type());
    }

    @Test
    public void testPartitionAndLogSignalsRemainLocalWhenNoNodeSignalExists() {
        FaultAttribution recover = engine.attribute(cluster(List.of(
            broker(1, signal(AvailabilitySignalType.PARTITION_RECOVER_FAILED, AvailabilityTarget.topicPartition("topic", 0))),
            broker(2)
        )));
        FaultAttribution read = engine.attribute(cluster(List.of(
            broker(1, signal(AvailabilitySignalType.LOG_READ_FAIL, AvailabilityTarget.topicPartitionOffset("topic", 0, 42L))),
            broker(2)
        )));

        assertEquals(FaultAttributionType.PARTITION_RECOVER_FAILED, recover.type());
        assertEquals(FaultAttributionType.LOG_READ_FAIL, read.type());
    }

    @Test
    public void testHealthyEmptySnapshotsProduceNoopAttribution() {
        assertEquals(FaultAttributionType.NONE, engine.attribute(cluster(List.of(broker(1), broker(2)))).type());
    }

    private ClusterAvailabilitySnapshot cluster(List<BrokerAvailabilitySnapshot> snapshots) {
        return cluster(snapshots, 100L, 90L);
    }

    private ClusterAvailabilitySnapshot cluster(List<BrokerAvailabilitySnapshot> snapshots, long nowMs, long staleMs) {
        return new ClusterAvailabilitySnapshot(snapshots.stream()
            .map(snapshot -> new CoverageBroker(snapshot.getBrokerId(), snapshot.getBrokerId() * 10L))
            .toList(), snapshots.stream().collect(java.util.stream.Collectors.toMap(
                BrokerAvailabilitySnapshot::getBrokerId, snapshot -> snapshot)), nowMs, staleMs);
    }

    private BrokerAvailabilitySnapshot broker(int brokerId, AvailabilitySignal... signals) {
        return new BrokerAvailabilitySnapshot(brokerId, brokerId * 10L, 100L, 70L, 100L, List.of(signals));
    }

    private AvailabilitySignal signal(AvailabilitySignalType type, AvailabilityTarget target) {
        return new AvailabilitySignal(type, target);
    }
}
