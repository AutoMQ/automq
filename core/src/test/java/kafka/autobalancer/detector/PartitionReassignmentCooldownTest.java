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

package kafka.autobalancer.detector;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Tag("S3Unit")
public class PartitionReassignmentCooldownTest {
    private static final long COOLDOWN_MS = 5_000L;
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);
    private static final Action MOVE_ACTION = new Action(ActionType.MOVE, TOPIC_PARTITION, 0, 1);

    /**
     * Given failed and successful reassignment submissions, when action batches execute, then only an accepted batch
     * starts the partition cooldown.
     */
    @Test
    public void testRecordsOnlyAcceptedReassignments() throws Exception {
        MockTime time = new MockTime(0, 1_000L, 0);
        ActionExecutorService actionExecutor = Mockito.mock(ActionExecutorService.class);
        Mockito.when(actionExecutor.execute(List.of(MOVE_ACTION)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("reassignment rejected")))
            .thenReturn(CompletableFuture.completedFuture(null));
        AnomalyDetectorImpl detector = detector(time, actionExecutor, COOLDOWN_MS);

        try {
            Assertions.assertThrowsExactly(ExecutionException.class,
                () -> detector.executeActionBatch(List.of(MOVE_ACTION), COOLDOWN_MS));
            ClusterModelSnapshot afterFailure = snapshot(TOPIC_PARTITION, 1, false);
            Assertions.assertEquals(0, detector.excludeRecentlyReassignedPartitions(afterFailure, COOLDOWN_MS));
            Assertions.assertNotNull(afterFailure.replica(1, TOPIC_PARTITION));

            detector.executeActionBatch(List.of(MOVE_ACTION), COOLDOWN_MS);
            ClusterModelSnapshot afterSuccess = snapshot(TOPIC_PARTITION, 1, false);
            Assertions.assertEquals(1, detector.excludeRecentlyReassignedPartitions(afterSuccess, COOLDOWN_MS));
            Assertions.assertNull(afterSuccess.replica(1, TOPIC_PARTITION));
        } finally {
            detector.shutdown();
        }
    }

    /**
     * Given a recently reassigned partition, when the configured cooldown elapses, then the partition becomes an
     * optimization candidate again.
     */
    @Test
    public void testCooldownExpires() {
        MockTime time = new MockTime(0, 1_000L, 0);
        PartitionReassignmentCooldown cooldown = new PartitionReassignmentCooldown(time);
        cooldown.record(List.of(MOVE_ACTION), COOLDOWN_MS);

        ClusterModelSnapshot beforeExpiry = snapshot(TOPIC_PARTITION, 1, false);
        time.sleep(COOLDOWN_MS - 1);
        Assertions.assertEquals(1, cooldown.excludeFrom(beforeExpiry, COOLDOWN_MS));

        ClusterModelSnapshot atExpiry = snapshot(TOPIC_PARTITION, 1, false);
        time.sleep(1);
        Assertions.assertEquals(0, cooldown.excludeFrom(atExpiry, COOLDOWN_MS));
        Assertions.assertNotNull(atExpiry.replica(1, TOPIC_PARTITION));
    }

    /**
     * Given cooldown history, when the cooldown is disabled dynamically, then the history is cleared and the
     * partition remains eligible for optimization.
     */
    @Test
    public void testDynamicDisableClearsCooldownHistory() throws Exception {
        MockTime time = new MockTime(0, 1_000L, 0);
        ActionExecutorService actionExecutor = Mockito.mock(ActionExecutorService.class);
        Mockito.when(actionExecutor.execute(List.of(MOVE_ACTION)))
            .thenReturn(CompletableFuture.completedFuture(null));
        AnomalyDetectorImpl detector = detector(time, actionExecutor, COOLDOWN_MS);

        try {
            detector.executeActionBatch(List.of(MOVE_ACTION), COOLDOWN_MS);
            detector.reconfigure(Map.of(
                AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_PARTITION_REASSIGNMENT_COOLDOWN_MS, 0L));

            Assertions.assertEquals(0L, detector.partitionReassignmentCooldownMs());
            ClusterModelSnapshot snapshot = snapshot(TOPIC_PARTITION, 1, false);
            Assertions.assertEquals(0, detector.excludeRecentlyReassignedPartitions(snapshot, COOLDOWN_MS));
            Assertions.assertNotNull(snapshot.replica(1, TOPIC_PARTITION));
        } finally {
            detector.shutdown();
        }
    }

    /**
     * Given a cooling-down partition on a slow broker, when candidates are filtered, then the partition remains
     * eligible so the detector can evacuate load from the slow broker.
     */
    @Test
    public void testSlowBrokerPartitionBypassesCooldown() {
        MockTime time = new MockTime(0, 1_000L, 0);
        PartitionReassignmentCooldown cooldown = new PartitionReassignmentCooldown(time);
        cooldown.record(List.of(MOVE_ACTION), COOLDOWN_MS);
        ClusterModelSnapshot snapshot = snapshot(TOPIC_PARTITION, 1, true);

        Assertions.assertEquals(0, cooldown.excludeFrom(snapshot, COOLDOWN_MS));
        Assertions.assertNotNull(snapshot.replica(1, TOPIC_PARTITION));
    }

    private static AnomalyDetectorImpl detector(MockTime time, ActionExecutorService actionExecutor,
        long cooldownMs) {
        Map<String, Object> configs = Map.of(
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, Long.MAX_VALUE,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_PARTITION_REASSIGNMENT_COOLDOWN_MS, cooldownMs);
        return new AnomalyDetectorImpl(configs, null, Mockito.mock(ClusterModel.class), actionExecutor, time);
    }

    private static ClusterModelSnapshot snapshot(TopicPartition topicPartition, int brokerId, boolean slowBroker) {
        ClusterModelSnapshot snapshot = new ClusterModelSnapshot();
        BrokerUpdater.Broker broker = new BrokerUpdater.Broker(
            brokerId, "rack", 0, Map.of(), MetricVersion.V1, false);
        broker.setSlowBroker(slowBroker);
        snapshot.addBroker(broker);
        addReplica(snapshot, topicPartition, brokerId);
        return snapshot;
    }

    private static void addReplica(ClusterModelSnapshot snapshot, TopicPartition topicPartition, int brokerId) {
        if (snapshot.broker(brokerId) == null) {
            snapshot.addBroker(new BrokerUpdater.Broker(
                brokerId, "rack", 0, Map.of(), MetricVersion.V1, false));
        }
        snapshot.addTopicPartition(brokerId, topicPartition,
            new TopicPartitionReplicaUpdater.TopicPartitionReplica(
                topicPartition, 0, MetricVersion.V1, false));
    }
}
