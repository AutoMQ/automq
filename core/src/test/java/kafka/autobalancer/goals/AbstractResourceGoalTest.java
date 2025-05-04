/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.autobalancer.goals;

import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static kafka.autobalancer.common.types.Resource.NW_IN;
import static kafka.autobalancer.common.types.Resource.NW_OUT;

public class AbstractResourceGoalTest extends GoalTestBase {

    @Test
    public void testValidAction() {
        AbstractResourceGoal goal = Mockito.mock(AbstractResourceGoal.class);
        Mockito.doCallRealMethod().when(goal).validateAction(Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.any());
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        BrokerUpdater.Broker srcBroker = createBroker(cluster, "", 0);
        BrokerUpdater.Broker destBroker = createBroker(cluster, "", 1);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartition tp = new TopicPartition(TOPIC_0, 0);

        // test valid action on trusted brokers
        srcBroker.setLoad(NW_IN, 100, true);
        destBroker.setLoad(NW_IN, 0, true);
        replica.setLoad(NW_IN, 100, true);
        Assertions.assertTrue(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        // test valid action on untrusted brokers
        srcBroker.setLoad(NW_IN, 100, false);
        destBroker.setLoad(NW_IN, 0, false);
        replica.setLoad(NW_IN, 0, true);
        Assertions.assertTrue(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        // test invalid action
        srcBroker.setLoad(NW_IN, 100, false);
        destBroker.setLoad(NW_IN, 0, false);
        replica.setLoad(NW_IN, 100, true);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        srcBroker.setLoad(NW_IN, 100, true);
        destBroker.setLoad(NW_IN, 0, false);
        replica.setLoad(NW_IN, 100, true);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        srcBroker.setLoad(NW_IN, 100, false);
        destBroker.setLoad(NW_IN, 0, true);
        replica.setLoad(NW_IN, 100, true);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        srcBroker.setLoad(NW_IN, 100, false);
        destBroker.setLoad(NW_IN, 0, true);
        replica.setLoad(NW_IN, 100, false);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));
    }

    @ParameterizedTest
    @ValueSource(bytes = {NW_IN, NW_OUT})
    public void testPartitionComparator(byte resource) {
        AbstractResourceGoal.PartitionComparator comparator = new AbstractResourceGoal.PartitionComparator(resource);
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        replica1.setLoad(resource, 100);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica2 = createTopicPartition(cluster, 0, Topic.GROUP_METADATA_TOPIC_NAME, 0);
        replica2.setLoad(resource, 200);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica3 = createTopicPartition(cluster, 0, Topic.AUTO_BALANCER_METRICS_TOPIC_NAME, 0);
        replica3.setLoad(resource, 300);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica4 = createTopicPartition(cluster, 0, TOPIC_0, 1);
        replica4.setLoad(resource, 400);
        List<TopicPartitionReplicaUpdater.TopicPartitionReplica> replicas = new ArrayList<>();
        replicas.add(replica1);
        replicas.add(replica2);
        replicas.add(replica3);
        replicas.add(replica4);
        replicas.sort(comparator);
        Assertions.assertEquals(replica4, replicas.get(0));
        Assertions.assertEquals(replica1, replicas.get(1));
        Assertions.assertEquals(replica3, replicas.get(2));
        Assertions.assertEquals(replica2, replicas.get(3));
    }
}
