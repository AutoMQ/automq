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

package kafka.autobalancer.goals;

import kafka.autobalancer.common.Resource;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@Tag("esUnit")
public class AbstractGoalTest extends GoalTestBase {

    private final Map<String, AbstractGoal> goalMap = new HashMap<>();

    @BeforeEach
    public void setup() {
        Map<String, Object> config = new HashMap<>();
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, new StringJoiner(",")
                .add(NetworkInDistributionGoal.class.getName())
                .add(NetworkOutDistributionGoal.class.getName())
                .add(NetworkInCapacityGoal.class.getName())
                .add(NetworkOutCapacityGoal.class.getName()).toString());
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_UTILIZATION_THRESHOLD, 0.8);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_UTILIZATION_THRESHOLD, 0.8);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD, 0.2);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD, 0.2);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, 0.2);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, 0.2);
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(config, false);
        List<AbstractGoal> goalList = controllerConfig.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, AbstractGoal.class);
        goalList.sort(Comparator.reverseOrder());
        for (AbstractGoal goal : goalList) {
            goalMap.put(goal.name(), goal);
        }
    }

    @Test
    public void testMultiGoalOptimization() {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        BrokerUpdater.Broker broker0 = createBroker(cluster, RACK, 0, true);
        BrokerUpdater.Broker broker1 = createBroker(cluster, RACK, 1, true);
        BrokerUpdater.Broker broker2 = createBroker(cluster, RACK, 2, true);
        BrokerUpdater.Broker broker3 = createBroker(cluster, RACK, 3, true);

        broker0.setCapacity(Resource.NW_IN, 100);
        broker0.setCapacity(Resource.NW_OUT, 100);
        broker0.setLoad(Resource.NW_IN, 90);
        broker0.setLoad(Resource.NW_OUT, 50);

        broker1.setCapacity(Resource.NW_IN, 100);
        broker1.setCapacity(Resource.NW_OUT, 100);
        broker1.setLoad(Resource.NW_IN, 20);
        broker1.setLoad(Resource.NW_OUT, 90);

        broker2.setCapacity(Resource.NW_IN, 100);
        broker2.setCapacity(Resource.NW_OUT, 100);
        broker2.setLoad(Resource.NW_IN, 30);
        broker2.setLoad(Resource.NW_OUT, 70);

        broker3.setCapacity(Resource.NW_IN, 100);
        broker3.setCapacity(Resource.NW_OUT, 100);
        broker3.setLoad(Resource.NW_IN, 60);
        broker3.setLoad(Resource.NW_OUT, 10);

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_1, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica2 = createTopicPartition(cluster, 0, TOPIC_1, 1);
        replica0.setLoad(Resource.NW_IN, 40);
        replica0.setLoad(Resource.NW_OUT, 30);
        replica1.setLoad(Resource.NW_IN, 30);
        replica1.setLoad(Resource.NW_OUT, 15);
        replica2.setLoad(Resource.NW_IN, 20);
        replica2.setLoad(Resource.NW_OUT, 5);
        Assertions.assertEquals(90, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
        Assertions.assertEquals(50, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica3 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica4 = createTopicPartition(cluster, 1, TOPIC_1, 2);
        replica3.setLoad(Resource.NW_IN, 5);
        replica3.setLoad(Resource.NW_OUT, 50);
        replica4.setLoad(Resource.NW_IN, 15);
        replica4.setLoad(Resource.NW_OUT, 40);
        Assertions.assertEquals(20, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
        Assertions.assertEquals(90, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica5 = createTopicPartition(cluster, 2, TOPIC_0, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica6 = createTopicPartition(cluster, 2, TOPIC_2, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica7 = createTopicPartition(cluster, 2, TOPIC_3, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica8 = createTopicPartition(cluster, 2, TOPIC_0, 1);
        replica5.setLoad(Resource.NW_IN, 10);
        replica5.setLoad(Resource.NW_OUT, 10);
        replica6.setLoad(Resource.NW_IN, 2);
        replica6.setLoad(Resource.NW_OUT, 30);
        replica7.setLoad(Resource.NW_IN, 3);
        replica7.setLoad(Resource.NW_OUT, 15);
        replica8.setLoad(Resource.NW_IN, 15);
        replica8.setLoad(Resource.NW_OUT, 15);
        Assertions.assertEquals(30, cluster.replicasFor(2).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
        Assertions.assertEquals(70, cluster.replicasFor(2).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica9 = createTopicPartition(cluster, 3, TOPIC_0, 2);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica10 = createTopicPartition(cluster, 3, TOPIC_2, 1);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica11 = createTopicPartition(cluster, 3, TOPIC_3, 1);
        replica9.setLoad(Resource.NW_IN, 40);
        replica9.setLoad(Resource.NW_OUT, 1);
        replica10.setLoad(Resource.NW_IN, 8);
        replica10.setLoad(Resource.NW_OUT, 4);
        replica11.setLoad(Resource.NW_IN, 12);
        replica11.setLoad(Resource.NW_OUT, 5);
        Assertions.assertEquals(60, cluster.replicasFor(3).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
        Assertions.assertEquals(10, cluster.replicasFor(3).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());

        for (AbstractGoal goal : goalMap.values()) {
            goal.optimize(cluster, goalMap.values());
        }
        for (BrokerUpdater.Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goalMap.get(NetworkInCapacityGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            Assertions.assertTrue(goalMap.get(NetworkOutCapacityGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            Assertions.assertTrue(goalMap.get(NetworkInDistributionGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            if (broker.getBrokerId() == 2) {
                Assertions.assertFalse(goalMap.get(NetworkOutDistributionGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            } else {
                Assertions.assertTrue(goalMap.get(NetworkOutDistributionGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            }
        }
        for (AbstractGoal goal : goalMap.values()) {
            goal.optimize(cluster, goalMap.values());
        }
        // all goals succeed in second iteration
        for (BrokerUpdater.Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goalMap.get(NetworkInCapacityGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            Assertions.assertTrue(goalMap.get(NetworkOutCapacityGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            Assertions.assertTrue(goalMap.get(NetworkInDistributionGoal.class.getSimpleName()).isBrokerAcceptable(broker));
            Assertions.assertTrue(goalMap.get(NetworkOutDistributionGoal.class.getSimpleName()).isBrokerAcceptable(broker));
        }
    }
}
