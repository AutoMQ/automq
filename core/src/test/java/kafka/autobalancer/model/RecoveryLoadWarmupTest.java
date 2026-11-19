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
 *
 */

package kafka.autobalancer.model;

import kafka.autobalancer.common.types.RawMetricTypes;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.metadata.LeaderConstants;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that post-handoff load warm-up gates only optional optimization, never recovery placement. */
@Tag("S3Unit")
public class RecoveryLoadWarmupTest {
    private static final Map<Byte, Double> ZERO_LOAD = Map.of(
        RawMetricTypes.PARTITION_BYTES_IN, 0.0, RawMetricTypes.PARTITION_BYTES_OUT, 0.0,
        RawMetricTypes.PARTITION_SIZE, 0.0);

    /** Given a reopened partition, the first zero-rate report cannot qualify its busy destination as idle. */
    @Test
    public void testFirstCompleteReportDoesNotQualifyRecoveryLoad() {
        TopicPartitionReplicaUpdater updater = new TopicPartitionReplicaUpdater(new TopicPartition("test", 0));
        updater.markLeaderChange(1000);
        updater.update(ZERO_LOAD.entrySet(), 2000);
        assertFalse(replica(updater).isRecoveryLoadReady());
        assertFalse(updater.get().isMetricsOutOfDate());
        // Two fresh reports also permit genuinely idle recovered partitions to become eligible.
        updater.update(ZERO_LOAD.entrySet(), 12000);
        assertTrue(replica(updater).isRecoveryLoadReady());
    }

    /** Given recovery, old, duplicate and partial reports cannot satisfy the two-report readiness gate. */
    @Test
    public void testOnlyDistinctCompletePostHandoffReportsCount() {
        TopicPartitionReplicaUpdater updater = new TopicPartitionReplicaUpdater(new TopicPartition("test", 0));
        updater.markLeaderChange(1000);
        updater.update(ZERO_LOAD.entrySet(), 900);
        updater.update(ZERO_LOAD.entrySet(), 2000);
        updater.update(ZERO_LOAD.entrySet(), 2000);
        updater.update(Map.of(RawMetricTypes.PARTITION_BYTES_IN, 100.0).entrySet(), 3000);
        assertFalse(replica(updater).isRecoveryLoadReady());
        updater.update(ZERO_LOAD.entrySet(), 12000);
        assertTrue(replica(updater).isRecoveryLoadReady());
    }

    /** Given initial metadata with no handoff, the existing single-report eligibility is unchanged. */
    @Test
    public void testInitialMetadataDoesNotAddRecoveryWarmup() {
        TopicPartitionReplicaUpdater updater = new TopicPartitionReplicaUpdater(new TopicPartition("test", 0));
        updater.update(ZERO_LOAD.entrySet(), 2000);
        assertFalse(updater.get().isMetricsOutOfDate());
        assertTrue(replica(updater).isRecoveryLoadReady());
    }

    /** Given faster reporting, two reports before a rate-meter tick must still leave the destination ineligible. */
    @Test
    public void testFastReportsMustSpanRateMeterTick() {
        TopicPartitionReplicaUpdater updater = new TopicPartitionReplicaUpdater(new TopicPartition("test", 0));
        updater.markLeaderChange(1000);
        updater.update(ZERO_LOAD.entrySet(), 2000);
        updater.update(ZERO_LOAD.entrySet(), 3000);
        assertFalse(replica(updater).isRecoveryLoadReady());
        updater.update(ZERO_LOAD.entrySet(), 7000);
        assertTrue(replica(updater).isRecoveryLoadReady());
    }

    /** Given one fresh recovery report, only the optimization snapshot is ineligible; raw loads remain usable. */
    @Test
    public void testWarmupIsRestrictedToOptionalBalancingSnapshot() {
        ClusterModel model = new ClusterModel();
        Uuid topicId = Uuid.randomUuid();
        TopicPartition tp = new TopicPartition("test", 0);
        model.registerBroker(0, "rack", true);
        model.registerBroker(1, "rack", true);
        model.createTopic(topicId, tp.topic());
        model.reassignPartition(topicId, tp.partition(), 0);
        model.reassignPartition(topicId, tp.partition(), 1);
        long now = System.currentTimeMillis();
        for (int broker = 0; broker < 2; broker++) {
            model.updateBrokerMetrics(broker, Map.of(
                RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0).entrySet(), now);
        }
        model.updateTopicPartitionMetrics(1, tp, ZERO_LOAD.entrySet(), now);
        assertTrue(model.snapshot().broker(1).isMetricsOutOfDate());
        assertNotNull(model.getClusterLoad(10000).brokerLoads());
        assertNotNull(model.getClusterLoad(10000).partitionLoads());
        assertFalse(((TopicPartitionReplicaUpdater.TopicPartitionReplica)
            model.snapshot().replicasFor(1).iterator().next().copy()).isRecoveryLoadReady());
        model.updateTopicPartitionMetrics(1, tp, ZERO_LOAD.entrySet(), now + 10000);
        assertFalse(model.snapshot().broker(1).isMetricsOutOfDate());
    }

    /** Given an unchanged leader, a duplicate record keeps warmed-up metrics; a later handoff resets them. */
    @Test
    public void testUnchangedLeaderDoesNotResetWarmup() {
        ClusterModel model = new ClusterModel();
        Uuid topicId = Uuid.randomUuid();
        TopicPartition tp = new TopicPartition("test", 0);
        model.registerBroker(0, "rack", true);
        model.registerBroker(1, "rack", true);
        model.createTopic(topicId, tp.topic());
        model.reassignPartition(topicId, tp.partition(), 0);
        model.reassignPartition(topicId, tp.partition(), 1);
        TopicPartitionReplicaUpdater updater = model.replicaUpdater(1, tp);
        long now = System.currentTimeMillis();
        updater.update(ZERO_LOAD.entrySet(), now);
        updater.update(ZERO_LOAD.entrySet(), now + 10000);
        model.reassignPartition(topicId, tp.partition(), 1);
        assertSame(updater, model.replicaUpdater(1, tp));
        assertTrue(replica(updater).isRecoveryLoadReady());
        model.reassignPartition(topicId, tp.partition(), 0);
        model.replicaUpdater(0, tp).update(ZERO_LOAD.entrySet(), now + 20000);
        assertFalse(replica(model.replicaUpdater(0, tp)).isRecoveryLoadReady());
    }

    /** Given a leaderless interval, the explicit recovery record still requires new load reports. */
    @Test
    public void testLeaderlessRecoveryWarmsUpButInitialRecordDoesNot() {
        RecordClusterModel model = new RecordClusterModel();
        Uuid topicId = Uuid.randomUuid();
        TopicPartition tp = new TopicPartition("test", 0);
        model.registerBroker(0, "rack", true);
        model.registerBroker(1, "rack", true);
        model.createTopic(topicId, tp.topic());
        model.onPartitionCreate(new PartitionRecord().setTopicId(topicId).setPartitionId(0).setLeader(0));
        assertTrue(replica(model.replicaUpdater(0, tp)).isRecoveryLoadReady());
        model.onPartitionChange(new PartitionChangeRecord().setTopicId(topicId).setPartitionId(0)
            .setLeader(LeaderConstants.NO_LEADER));
        model.onPartitionChange(new PartitionChangeRecord().setTopicId(topicId).setPartitionId(0).setLeader(1));
        TopicPartitionReplicaUpdater updater = model.replicaUpdater(1, tp);
        long now = System.currentTimeMillis();
        updater.update(ZERO_LOAD.entrySet(), now);
        assertFalse(replica(updater).isRecoveryLoadReady());
        model.onPartitionChange(new PartitionChangeRecord().setTopicId(topicId).setPartitionId(0)
            .setLeader(LeaderConstants.NO_LEADER_CHANGE));
        assertSame(updater, model.replicaUpdater(1, tp));
        updater.update(ZERO_LOAD.entrySet(), now + 10000);
        assertTrue(replica(updater).isRecoveryLoadReady());
    }

    private static TopicPartitionReplicaUpdater.TopicPartitionReplica replica(TopicPartitionReplicaUpdater updater) {
        return (TopicPartitionReplicaUpdater.TopicPartitionReplica) updater.get();
    }
}
