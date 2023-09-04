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

import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.metricsreporter.metric.BrokerMetrics;
import kafka.autobalancer.metricsreporter.metric.RawMetricType;
import kafka.autobalancer.metricsreporter.metric.TopicPartitionMetrics;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

@Tag("esUnit")
public class ClusterModelTest {

    @Test
    public void testRegisterBroker() {
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
        RegisterBrokerRecord record1 = new RegisterBrokerRecord()
                .setBrokerId(1);
        RegisterBrokerRecord record2 = new RegisterBrokerRecord()
                .setBrokerId(2);
        clusterModel.onBrokerRegister(record1);
        clusterModel.onBrokerRegister(record2);
        clusterModel.onBrokerRegister(record1);

        Assertions.assertEquals(1, clusterModel.brokerUpdater(1).id());
        Assertions.assertEquals(2, clusterModel.brokerUpdater(2).id());
    }

    @Test
    public void testUnregisterBroker() {
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
        RegisterBrokerRecord record1 = new RegisterBrokerRecord()
                .setBrokerId(1);
        RegisterBrokerRecord record2 = new RegisterBrokerRecord()
                .setBrokerId(2);
        clusterModel.onBrokerRegister(record1);
        clusterModel.onBrokerRegister(record2);
        clusterModel.onBrokerRegister(record1);

        Assertions.assertEquals(1, clusterModel.brokerUpdater(1).id());
        Assertions.assertEquals(2, clusterModel.brokerUpdater(2).id());

        UnregisterBrokerRecord unregisterRecord = new UnregisterBrokerRecord()
                .setBrokerId(2);
        clusterModel.onBrokerUnregister(unregisterRecord);

        Assertions.assertEquals(1, clusterModel.brokerUpdater(1).id());
        Assertions.assertNull(clusterModel.brokerUpdater(2));
    }

    @Test
    public void testCreateTopic() {
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
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
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
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
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int brokerId = 1;
        TopicPartition tp = new TopicPartition(topicName, partition);

        // create on non-exist broker
        PartitionRecord partitionRecord = new PartitionRecord()
                .setReplicas(List.of(brokerId, 2))
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

        partitionRecord.setReplicas(List.of(brokerId));
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertEquals(tp, clusterModel.replicaUpdater(brokerId, tp).topicPartition());
    }

    @Test
    public void testChangePartition() {
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int oldBrokerId = 1;
        int newBrokerId = 2;
        TopicPartition tp = new TopicPartition(topicName, partition);

        // reassign on non-exist broker
        PartitionChangeRecord partitionChangeRecord = new PartitionChangeRecord()
                .setReplicas(List.of(newBrokerId, 3))
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionChange(partitionChangeRecord);
        Assertions.assertNull(clusterModel.replicaUpdater(newBrokerId, tp));

        // create on non-exist topic
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord()
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
                .setBrokerId(oldBrokerId);
        clusterModel.onBrokerRegister(brokerRecord2);
        PartitionRecord partitionRecord = new PartitionRecord()
                .setReplicas(List.of(oldBrokerId))
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertEquals(tp, clusterModel.replicaUpdater(oldBrokerId, tp).topicPartition());

        partitionChangeRecord.setReplicas(List.of(newBrokerId));
        clusterModel.onPartitionChange(partitionChangeRecord);
        Assertions.assertEquals(tp, clusterModel.replicaUpdater(newBrokerId, tp).topicPartition());
        Assertions.assertNull(clusterModel.replicaUpdater(oldBrokerId, tp));
    }

    @Test
    public void testUpdateBroker() {
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
        int brokerId = 1;

        // update on non-exist broker
        long now = System.currentTimeMillis();
        BrokerMetrics brokerMetrics = new BrokerMetrics(now, brokerId, "");
        brokerMetrics.put(RawMetricType.BROKER_CAPACITY_NW_IN, 10);
        brokerMetrics.put(RawMetricType.BROKER_CAPACITY_NW_OUT, 10);
        brokerMetrics.put(RawMetricType.ALL_TOPIC_BYTES_IN, 10);
        brokerMetrics.put(RawMetricType.ALL_TOPIC_BYTES_OUT, 10);
        brokerMetrics.put(RawMetricType.BROKER_CPU_UTIL, 10);
        Assertions.assertFalse(clusterModel.updateBroker(brokerMetrics));

        RegisterBrokerRecord registerBrokerRecord = new RegisterBrokerRecord()
                .setBrokerId(brokerId);
        clusterModel.onBrokerRegister(registerBrokerRecord);
        Assertions.assertEquals(brokerId, clusterModel.brokerUpdater(brokerId).id());
        Assertions.assertTrue(clusterModel.updateBroker(brokerMetrics));
    }

    @Test
    public void testUpdatePartition() {
        ClusterModel clusterModel = new ClusterModel(new AutoBalancerControllerConfig(Collections.emptyMap(), false));
        String topicName = "testTopic";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int brokerId = 1;

        // update on non-exist topic
        long now = System.currentTimeMillis();
        TopicPartitionMetrics topicPartitionMetrics = new TopicPartitionMetrics(now, brokerId, "", topicName, partition);
        topicPartitionMetrics.put(RawMetricType.TOPIC_PARTITION_BYTES_IN, 10);
        topicPartitionMetrics.put(RawMetricType.TOPIC_PARTITION_BYTES_OUT, 10);
        topicPartitionMetrics.put(RawMetricType.PARTITION_SIZE, 10);
        Assertions.assertFalse(clusterModel.updateTopicPartition(topicPartitionMetrics));

        RegisterBrokerRecord registerBrokerRecord = new RegisterBrokerRecord()
                .setBrokerId(brokerId);
        clusterModel.onBrokerRegister(registerBrokerRecord);
        TopicRecord topicRecord = new TopicRecord()
                .setName(topicName)
                .setTopicId(topicId);
        clusterModel.onTopicCreate(topicRecord);
        PartitionRecord partitionRecord = new PartitionRecord()
                .setReplicas(List.of(brokerId))
                .setTopicId(topicId)
                .setPartitionId(partition);
        clusterModel.onPartitionCreate(partitionRecord);
        Assertions.assertTrue(clusterModel.updateTopicPartition(topicPartitionMetrics));
    }

    @Test
    public void testSnapshot() {

    }
}
