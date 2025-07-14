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

package kafka.autobalancer.model;

import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.metricsreporter.metric.BrokerMetrics;
import kafka.autobalancer.metricsreporter.metric.TopicPartitionMetrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Timeout(60)
@Tag("S3Unit")
public class ClusterModelTest {

    @Test
    public void testRegisterBroker() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        RegisterBrokerRecord record1 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(1);
        RegisterBrokerRecord record2 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(2);
        clusterModel.onBrokerRegister(record1);
        clusterModel.onBrokerRegister(record2);
        clusterModel.onBrokerRegister(record1);

        Assertions.assertEquals(1, ((BrokerUpdater.Broker) clusterModel.brokerUpdater(1).get()).getBrokerId());
        Assertions.assertEquals(2, ((BrokerUpdater.Broker) clusterModel.brokerUpdater(2).get()).getBrokerId());
    }

    @Test
    public void testUnregisterBroker() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        RegisterBrokerRecord record1 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(1);
        RegisterBrokerRecord record2 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(2);
        clusterModel.onBrokerRegister(record1);
        clusterModel.onBrokerRegister(record2);
        clusterModel.onBrokerRegister(record1);

        Assertions.assertEquals(1, ((BrokerUpdater.Broker) clusterModel.brokerUpdater(1).get()).getBrokerId());
        Assertions.assertEquals(2, ((BrokerUpdater.Broker) clusterModel.brokerUpdater(2).get()).getBrokerId());

        UnregisterBrokerRecord unregisterRecord = new UnregisterBrokerRecord()
                .setBrokerId(2);
        clusterModel.onBrokerUnregister(unregisterRecord);

        Assertions.assertEquals(1, ((BrokerUpdater.Broker) clusterModel.brokerUpdater(1).get()).getBrokerId());
        Assertions.assertNull(clusterModel.brokerUpdater(2));
    }

    @Test
    public void testFencedBroker() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        RegisterBrokerRecord record1 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(1);
        RegisterBrokerRecord record2 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(2);
        clusterModel.onBrokerRegister(record1);
        clusterModel.onBrokerRegister(record2);

        Assertions.assertEquals(1, ((BrokerUpdater.Broker) clusterModel.brokerUpdater(1).get()).getBrokerId());
        Assertions.assertEquals(2, ((BrokerUpdater.Broker) clusterModel.brokerUpdater(2).get()).getBrokerId());

        BrokerRegistrationChangeRecord fencedRecord = new BrokerRegistrationChangeRecord()
                .setBrokerId(2)
                .setFenced(BrokerRegistrationFencingChange.FENCE.value());
        clusterModel.onBrokerRegistrationChanged(fencedRecord);

        Assertions.assertNotNull(clusterModel.brokerUpdater(1).get());
        Assertions.assertNull(clusterModel.brokerUpdater(2).get());

        clusterModel.updateBrokerMetrics(1, Map.of(
                RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0).entrySet(), System.currentTimeMillis());
        clusterModel.updateBrokerMetrics(2, Map.of(
                RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0).entrySet(), System.currentTimeMillis());

        ClusterModelSnapshot snapshot = clusterModel.snapshot();
        Assertions.assertNotNull(snapshot.broker(1));
        Assertions.assertNull(snapshot.broker(2));
    }

    @Test
    public void testCreateTopic() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        TopicRecord record = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(record);

        Assertions.assertEquals(topicName, clusterModel.topicName(topicId));
    }

    @Test
    public void testDeleteTopic() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        TopicRecord record = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(record);

        Assertions.assertEquals(topicName, clusterModel.topicName(topicId));

        RemoveTopicRecord removeTopicRecord = new RemoveTopicRecord()
                .setTopicId(topicId);
        clusterModel.onTopicDelete(removeTopicRecord);

        Assertions.assertNull(clusterModel.topicName(topicId));
    }

    @Test
    public void testCreatePartition() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int brokerId = 1;
        TopicPartition tp = new TopicPartition(topicName, partition);

        // create on non-exist broker
        PartitionRecord partitionRecord = new PartitionRecord()
                .setLeader(2)
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(brokerId, tp));

        // create on non-exist topic
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord()
                .setBrokerId(brokerId);
        clusterModel.onBrokerRegister(brokerRecord);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(brokerId, tp));

        // create with invalid replicas
        TopicRecord topicRecord = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(brokerId, tp));

        partitionRecord.setLeader(brokerId);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertEquals(tp, clusterModel.replicaUpdater(brokerId, tp).topicPartition());
    }

    @Test
    public void testChangePartition() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int oldBrokerId = 1;
        int newBrokerId = 2;
        TopicPartition tp = new TopicPartition(topicName, partition);

        // reassign on non-exist broker
        PartitionChangeRecord partitionChangeRecord = new PartitionChangeRecord()
                .setLeader(3)
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionChange(partitionChangeRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(newBrokerId, tp));

        // create on non-exist topic
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(newBrokerId);
        clusterModel.onBrokerRegister(brokerRecord);
        clusterModel.onPartitionChange(partitionChangeRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(newBrokerId, tp));

        // create with invalid replicas
        TopicRecord topicRecord = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        clusterModel.onPartitionChange(partitionChangeRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(newBrokerId, tp));

        RegisterBrokerRecord brokerRecord2 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(oldBrokerId);
        clusterModel.onBrokerRegister(brokerRecord2);
        PartitionRecord partitionRecord = new PartitionRecord()
                .setLeader(oldBrokerId)
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertEquals(tp, clusterModel.replicaUpdater(oldBrokerId, tp).topicPartition());

        partitionChangeRecord.setLeader(newBrokerId);
        clusterModel.onPartitionChange(partitionChangeRecord);
        Assertions.assertEquals(tp, clusterModel.replicaUpdater(newBrokerId, tp).topicPartition());
        Assertions.assertNull(clusterModel.replicaUpdater(oldBrokerId, tp));

        partitionChangeRecord.setLeader(-1);
        clusterModel.onPartitionChange(partitionChangeRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(newBrokerId, tp));
    }

    @Test
    public void testUpdatePartition() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int brokerId = 1;

        // update on non-exist topic
        long now = System.currentTimeMillis();
        TopicPartitionMetrics topicPartitionMetrics = new TopicPartitionMetrics(now, brokerId, "", topicName, partition);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertFalse(clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
                new TopicPartition(topicName, partition), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time()));

        RegisterBrokerRecord registerBrokerRecord = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(brokerId);
        clusterModel.onBrokerRegister(registerBrokerRecord);
        TopicRecord topicRecord = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        PartitionRecord partitionRecord = new PartitionRecord()
                .setLeader(brokerId)
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertTrue(clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
                new TopicPartition(topicName, partition), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time()));
    }

    @Test
    public void testExcludeTopics() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        String topicName1 = "testTopic-1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition = 0;
        int brokerId = 1;

        RegisterBrokerRecord registerBrokerRecord = new RegisterBrokerRecord()
            .setBrokerId(brokerId)
            .setFenced(false);
        clusterModel.onBrokerRegister(registerBrokerRecord);
        TopicRecord topicRecord = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        PartitionRecord partitionRecord = new PartitionRecord()
                .setLeader(brokerId)
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);

        TopicRecord topicRecord1 = new TopicRecord()
                .setName(topicName1)
                .setTopicId(topicId1);
        clusterModel.onTopicCreate(topicRecord1);
        PartitionRecord partitionRecord1 = new PartitionRecord()
                .setLeader(brokerId)
                .setTopicId(topicId1)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord1);
        long now = System.currentTimeMillis();
        clusterModel.updateBrokerMetrics(brokerId, Map.of(
                RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0).entrySet(), now);

        TopicPartitionMetrics topicPartitionMetrics = new TopicPartitionMetrics(now, brokerId, "", topicName, partition);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
                new TopicPartition(topicName, partition), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time());

        TopicPartitionMetrics topicPartitionMetrics1 = new TopicPartitionMetrics(now, brokerId, "", topicName1, partition);
        topicPartitionMetrics1.put(RawMetricTypes.PARTITION_BYTES_IN, 60);
        topicPartitionMetrics1.put(RawMetricTypes.PARTITION_BYTES_OUT, 50);
        topicPartitionMetrics1.put(RawMetricTypes.PARTITION_SIZE, 10);
        clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics1.brokerId(),
                new TopicPartition(topicName1, partition), topicPartitionMetrics1.getMetricValueMap().entrySet(), topicPartitionMetrics1.time());

        ClusterModelSnapshot snapshot = clusterModel.snapshot(Collections.emptySet(), Set.of(topicName1), 10000);
        Collection<TopicPartitionReplicaUpdater.TopicPartitionReplica> replicas = snapshot.replicasFor(brokerId);
        Assertions.assertEquals(1, replicas.size());
        Assertions.assertEquals(topicName, replicas.iterator().next().getTopicPartition().topic());
        Assertions.assertEquals(partition, replicas.iterator().next().getTopicPartition().partition());
        Assertions.assertEquals(70, snapshot.broker(brokerId).loadValue(Resource.NW_IN));
        Assertions.assertEquals(60, snapshot.broker(brokerId).loadValue(Resource.NW_OUT));
    }

    @Test
    public void testMetricsTime() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int partition1 = 1;
        int brokerId = 1;

        RegisterBrokerRecord registerBrokerRecord = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(brokerId);
        clusterModel.onBrokerRegister(registerBrokerRecord);
        TopicRecord topicRecord = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        PartitionRecord partitionRecord = new PartitionRecord()
                .setLeader(brokerId)
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);
        PartitionRecord partitionRecord1 = new PartitionRecord()
                .setLeader(brokerId)
                .setTopicId(topicId)
                .setPartitionId(partition1);
        clusterModel.onPartitionCreate(partitionRecord1);

        long now = System.currentTimeMillis();

        Assertions.assertTrue(clusterModel.updateBrokerMetrics(brokerId, Map.of(
                RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0).entrySet(), now));
        TopicPartitionMetrics topicPartitionMetrics = new TopicPartitionMetrics(now - 1000, brokerId, "", topicName, partition);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertTrue(clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
                new TopicPartition(topicName, partition), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time()));

        topicPartitionMetrics = new TopicPartitionMetrics(now - 2000, brokerId, "", topicName, partition1);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertTrue(clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
                new TopicPartition(topicName, partition1), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time()));

        Map<Integer, Long> metricsTimeMap = clusterModel.calculateBrokerLatestMetricsTime();
        Assertions.assertEquals(1, metricsTimeMap.size());
        Assertions.assertEquals(now, metricsTimeMap.get(brokerId));
    }

    @Test
    public void testOutDatedMetrics() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int brokerId = 1;

        RegisterBrokerRecord registerBrokerRecord = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(brokerId);
        clusterModel.onBrokerRegister(registerBrokerRecord);
        TopicRecord topicRecord = new TopicRecord()
            .setName(topicName)
            .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        PartitionRecord partitionRecord = new PartitionRecord()
            .setLeader(brokerId)
            .setTopicId(topicId)
            .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);

        ClusterModelSnapshot snapshot = clusterModel.snapshot();
        Assertions.assertNotNull(snapshot.broker(brokerId));
        Assertions.assertTrue(snapshot.broker(brokerId).isMetricsOutOfDate());
        Assertions.assertNotNull(snapshot.replica(brokerId, new TopicPartition(topicName, partition)));
        Assertions.assertTrue(snapshot.replica(brokerId, new TopicPartition(topicName, partition)).isMetricsOutOfDate());

        long now = System.currentTimeMillis();

        Assertions.assertTrue(clusterModel.updateBrokerMetrics(brokerId, Map.of(
            RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
            RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
            RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0).entrySet(), now));
        snapshot = clusterModel.snapshot();
        Assertions.assertNotNull(snapshot.broker(brokerId));
        Assertions.assertTrue(snapshot.broker(brokerId).isMetricsOutOfDate());
        Assertions.assertNotNull(snapshot.replica(brokerId, new TopicPartition(topicName, partition)));
        Assertions.assertTrue(snapshot.replica(brokerId, new TopicPartition(topicName, partition)).isMetricsOutOfDate());

        TopicPartitionMetrics topicPartitionMetrics = new TopicPartitionMetrics(now, brokerId, "", topicName, partition);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertTrue(clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
            new TopicPartition(topicName, partition), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time()));
        snapshot = clusterModel.snapshot();
        Assertions.assertNotNull(snapshot.broker(brokerId));
        Assertions.assertFalse(snapshot.broker(brokerId).isMetricsOutOfDate());
        Assertions.assertNotNull(snapshot.replica(brokerId, new TopicPartition(topicName, partition)));
        Assertions.assertFalse(snapshot.replica(brokerId, new TopicPartition(topicName, partition)).isMetricsOutOfDate());
    }

    @Test
    public void testSlowBroker() {
        RecordClusterModel clusterModel = new RecordClusterModel();

        RegisterBrokerRecord registerBrokerRecord0 = new RegisterBrokerRecord()
            .setFenced(false)
            .setBrokerId(0);
        clusterModel.onBrokerRegister(registerBrokerRecord0);

        // test not enough samples
//        for (int i = 0; i < 10; i++) {
//            Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0, Double.MAX_VALUE,
//                    Double.MAX_VALUE, Double.MAX_VALUE).getMetricValueMap(),
//                    System.currentTimeMillis()));
//        }
//        ClusterModelSnapshot snapshot = clusterModel.snapshot();
//        snapshot.markSlowBrokers();
//        Assertions.assertFalse(snapshot.broker(0).isSlowBroker());
//        clusterModel.unregisterBroker(0);

        // test high append latency old version
        clusterModel.onBrokerRegister(registerBrokerRecord0);
        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
                    0, 0, 0).getMetricValueMap().entrySet(), System.currentTimeMillis()));
        }
        ClusterModelSnapshot snapshot = clusterModel.snapshot();
        snapshot.markSlowBrokers();
        Assertions.assertFalse(snapshot.broker(0).isSlowBroker());
        Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
                2000, 0, 0).getMetricValueMap().entrySet(), System.currentTimeMillis()));
        snapshot = clusterModel.snapshot();
        snapshot.markSlowBrokers();
        Assertions.assertFalse(snapshot.broker(0).isSlowBroker());
        clusterModel.unregisterBroker(0);

        // test high append latency
//        clusterModel.onBrokerRegister(registerBrokerRecord0);
//        for (int i = 0; i < 100; i++) {
//            Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
//                    0, 0, 0, MetricVersion.V1).getMetricValueMap().entrySet(), System.currentTimeMillis()));
//        }
//        snapshot = clusterModel.snapshot();
//        snapshot.markSlowBrokers();
//        Assertions.assertFalse(snapshot.broker(0).isSlowBroker());
//        Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
//                2000, 0, 0, MetricVersion.V1).getMetricValueMap().entrySet(), System.currentTimeMillis()));
//        snapshot = clusterModel.snapshot();
//        snapshot.markSlowBrokers();
//        Assertions.assertTrue(snapshot.broker(0).isSlowBroker());
//        clusterModel.unregisterBroker(0);

        // test high pending append latency
        clusterModel.onBrokerRegister(registerBrokerRecord0);
        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
                    0, 0, 0, MetricVersion.V1).getMetricValueMap().entrySet(), System.currentTimeMillis()));
        }
        snapshot = clusterModel.snapshot();
        snapshot.markSlowBrokers();
        Assertions.assertFalse(snapshot.broker(0).isSlowBroker());
        Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
                0, 20000, 0, MetricVersion.V1).getMetricValueMap().entrySet(), System.currentTimeMillis()));
        snapshot = clusterModel.snapshot();
        snapshot.markSlowBrokers();
        Assertions.assertTrue(snapshot.broker(0).isSlowBroker());
        clusterModel.unregisterBroker(0);

        // test high pending fetch latency
        clusterModel.onBrokerRegister(registerBrokerRecord0);
        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
                    0, 0, 0, MetricVersion.V1).getMetricValueMap().entrySet(), System.currentTimeMillis()));
        }
        snapshot = clusterModel.snapshot();
        snapshot.markSlowBrokers();
        Assertions.assertFalse(snapshot.broker(0).isSlowBroker());
        Assertions.assertTrue(clusterModel.updateBrokerMetrics(0, createBrokerMetrics(0,
                0, 0, 20000, MetricVersion.V1).getMetricValueMap().entrySet(), System.currentTimeMillis()));
        snapshot = clusterModel.snapshot();
        snapshot.markSlowBrokers();
        Assertions.assertTrue(snapshot.broker(0).isSlowBroker());
        clusterModel.unregisterBroker(0);
    }

    @Test
    public void testTrustedMetrics() {
        RecordClusterModel clusterModel = new RecordClusterModel();
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int partition1 = 1;
        int brokerId = 1;

        RegisterBrokerRecord registerBrokerRecord = new RegisterBrokerRecord()
            .setBrokerId(brokerId)
            .setFenced(false);
        clusterModel.onBrokerRegister(registerBrokerRecord);
        TopicRecord topicRecord = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        PartitionRecord partitionRecord = new PartitionRecord()
                .setLeader(brokerId)
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);
        PartitionRecord partitionRecord1 = new PartitionRecord()
                .setLeader(brokerId)
                .setTopicId(topicId)
                .setPartitionId(partition1);
        clusterModel.onPartitionCreate(partitionRecord1);

        long now = System.currentTimeMillis();

        Assertions.assertTrue(clusterModel.updateBrokerMetrics(brokerId, Map.of(
                RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 0.0,
                RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, 0.0).entrySet(), now));

        TopicPartitionMetrics topicPartitionMetrics = new TopicPartitionMetrics(now, brokerId, "", topicName, partition);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 15);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertTrue(clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
                new TopicPartition(topicName, partition), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time()));

        topicPartitionMetrics = new TopicPartitionMetrics(now, brokerId, "", topicName, partition1);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 20);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 25);
        topicPartitionMetrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertTrue(clusterModel.updateTopicPartitionMetrics(topicPartitionMetrics.brokerId(),
                new TopicPartition(topicName, partition1), topicPartitionMetrics.getMetricValueMap().entrySet(), topicPartitionMetrics.time()));

        ClusterModelSnapshot snapshot = clusterModel.snapshot();
        AbstractInstanceUpdater.Load brokerLoadIn = snapshot.broker(brokerId).load(Resource.NW_IN);
        Assertions.assertTrue(brokerLoadIn.isTrusted());
        Assertions.assertEquals(30, brokerLoadIn.getValue());

        AbstractInstanceUpdater.Load brokerLoadOut = snapshot.broker(brokerId).load(Resource.NW_OUT);
        Assertions.assertTrue(brokerLoadOut.isTrusted());
        Assertions.assertEquals(40, brokerLoadOut.getValue());

        Assertions.assertTrue(snapshot.broker(brokerId).load(Resource.NW_OUT).isTrusted());
        AbstractInstanceUpdater.Load loadIn0 = snapshot.replica(brokerId, new TopicPartition(topicName, partition)).load(Resource.NW_IN);
        Assertions.assertTrue(loadIn0.isTrusted());
        Assertions.assertEquals(10, loadIn0.getValue());
        AbstractInstanceUpdater.Load loadOut0 = snapshot.replica(brokerId, new TopicPartition(topicName, partition)).load(Resource.NW_OUT);
        Assertions.assertTrue(loadOut0.isTrusted());
        Assertions.assertEquals(15, loadOut0.getValue());

        AbstractInstanceUpdater.Load loadIn1 = snapshot.replica(brokerId, new TopicPartition(topicName, partition1)).load(Resource.NW_IN);
        Assertions.assertTrue(loadIn1.isTrusted());
        Assertions.assertEquals(20, loadIn1.getValue());
        AbstractInstanceUpdater.Load loadOut1 = snapshot.replica(brokerId, new TopicPartition(topicName, partition1)).load(Resource.NW_OUT);
        Assertions.assertTrue(loadOut1.isTrusted());
        Assertions.assertEquals(25, loadOut1.getValue());
    }

    private BrokerMetrics createBrokerMetrics(int brokerId, double appendLatency, double pendingAppendLatency,
                                              double pendingFetchLatency) {
        return createBrokerMetrics(brokerId, appendLatency, pendingAppendLatency, pendingFetchLatency, null);
    }

    private BrokerMetrics createBrokerMetrics(int brokerId, double appendLatency, double pendingAppendLatency,
                                              double pendingFetchLatency, MetricVersion metricVersion) {
        long now = System.currentTimeMillis();
        BrokerMetrics brokerMetrics = new BrokerMetrics(now, brokerId, "");
        if (metricVersion != null) {
            brokerMetrics.put(RawMetricTypes.BROKER_METRIC_VERSION, metricVersion.value());
        }
        brokerMetrics.put(RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, appendLatency);
        brokerMetrics.put(RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, pendingAppendLatency);
        brokerMetrics.put(RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS, pendingFetchLatency);
        return brokerMetrics;
    }
}
