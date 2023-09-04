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

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.Resource;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@Tag("esUnit")
public class AbstractResourceCapacityGoalTest extends GoalTestBase {
    private final Map<String, AbstractGoal> goalMap = new HashMap<>();

    @BeforeEach
    public void setup() {
        Map<String, Object> config = new HashMap<>();
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, new StringJoiner(",")
                .add(NetworkInCapacityGoal.class.getName())
                .add(NetworkOutCapacityGoal.class.getName()).toString());
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_UTILIZATION_THRESHOLD, 0.8);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_UTILIZATION_THRESHOLD, 0.8);
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(config, false);
        List<AbstractGoal> goalList = controllerConfig.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, AbstractGoal.class);
        for (AbstractGoal goal : goalList) {
            goalMap.put(goal.name(), goal);
        }
    }

    private AbstractGoal getGoalByResource(Resource resource) {
        AbstractGoal goal = null;
        switch (resource) {
            case NW_IN:
                goal = goalMap.get(NetworkInCapacityGoal.class.getSimpleName());
                break;
            case NW_OUT:
                goal = goalMap.get(NetworkOutCapacityGoal.class.getSimpleName());
                break;
            default:
                break;
        }
        return goal;
    }

    private void testActionAcceptanceScore(Resource resource) {
        AbstractGoal goal = getGoalByResource(resource);
        Assertions.assertNotNull(goal);

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        BrokerUpdater.Broker broker1 = createBroker(cluster, RACK, 1, true);
        BrokerUpdater.Broker broker2 = createBroker(cluster, RACK, 2, true);
        broker1.setCapacity(resource, 100);
        broker2.setCapacity(resource, 100);

        broker1.setLoad(resource, 90);
        broker2.setLoad(resource, 50);

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica = createTopicPartition(cluster, 1, TOPIC_0, 0);
        replica.setLoad(resource, 20);

        Action action = new Action(ActionType.MOVE, replica.getTopicPartition(), broker1.getBrokerId(), broker2.getBrokerId());
        Assertions.assertEquals(0.6, goal.actionAcceptanceScore(action, cluster), 1e-15);

        broker1.setLoad(resource, 50);
        broker2.setLoad(resource, 30);
        Assertions.assertEquals(0.5, goal.actionAcceptanceScore(action, cluster), 1e-15);

        broker1.setLoad(resource, 30);
        broker2.setLoad(resource, 50);
        Assertions.assertEquals(0.4, goal.actionAcceptanceScore(action, cluster), 1e-15);

        broker1.setLoad(resource, 75);
        broker2.setLoad(resource, 70);
        Assertions.assertEquals(0.0, goal.actionAcceptanceScore(action, cluster), 1e-15);

        broker1.setLoad(resource, 75);
        broker2.setLoad(resource, 85);
        Assertions.assertEquals(0.0, goal.actionAcceptanceScore(action, cluster), 1e-15);

        broker1.setLoad(resource, 90);
        broker2.setLoad(resource, 70);
        Assertions.assertEquals(0.0, goal.actionAcceptanceScore(action, cluster), 1e-15);

        broker1.setLoad(resource, 90);
        broker2.setLoad(resource, 85);
        Assertions.assertEquals(0.0, goal.actionAcceptanceScore(action, cluster), 1e-15);

        broker1.setLoad(resource, 90);
        broker2.setLoad(resource, 50);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica2 = createTopicPartition(cluster, 2, TOPIC_0, 1);
        replica2.setLoad(resource, 5);

        Action action2 = new Action(ActionType.SWAP, replica.getTopicPartition(), broker1.getBrokerId(), broker2.getBrokerId(), replica2.getTopicPartition());
        Assertions.assertEquals(0.575, goal.actionAcceptanceScore(action2, cluster), 1e-15);

        replica.setLoad(resource, 10);
        Assertions.assertEquals(0.525, goal.actionAcceptanceScore(action2, cluster), 1e-15);

        replica.setLoad(resource, 1);
        Assertions.assertEquals(0.0, goal.actionAcceptanceScore(action2, cluster), 1e-15);
    }

    private void testSingleResourceCapacityOptimizeOneMove(Resource resource) {
        AbstractGoal goal = getGoalByResource(resource);
        Assertions.assertNotNull(goal);

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        BrokerUpdater.Broker broker0 = createBroker(cluster, RACK, 0, true);
        BrokerUpdater.Broker broker1 = createBroker(cluster, RACK, 1, true);
        BrokerUpdater.Broker broker2 = createBroker(cluster, RACK, 2, true);
        BrokerUpdater.Broker broker3 = createBroker(cluster, RACK, 3, true);
        BrokerUpdater.Broker broker4 = createBroker(cluster, RACK, 4, false);

        double load0 = 90;
        double load1 = 60;
        double load2 = 28;
        double load3 = 50;
        broker0.setCapacity(resource, 100);
        broker0.setLoad(resource, 90);

        broker1.setCapacity(resource, 90);
        broker1.setLoad(resource, 30);

        broker2.setCapacity(resource, 30);
        broker2.setLoad(resource, 28);

        broker3.setCapacity(resource, 120);
        broker3.setLoad(resource, 50);

        broker4.setCapacity(resource, 999);
        broker4.setLoad(resource, 0.0);

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_2, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica2 = createTopicPartition(cluster, 0, TOPIC_3, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica3 = createTopicPartition(cluster, 0, TOPIC_0, 1);
        replica0.setLoad(resource, 20);
        replica1.setLoad(resource, 30);
        replica2.setLoad(resource, 5);
        replica3.setLoad(resource, 35);
        Assertions.assertEquals(load0, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(resource)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica4 = createTopicPartition(cluster, 1, TOPIC_4, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica5 = createTopicPartition(cluster, 1, TOPIC_2, 1);
        replica4.setLoad(resource, 10);
        replica5.setLoad(resource, 50);
        Assertions.assertEquals(load1, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(resource)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica6 = createTopicPartition(cluster, 2, TOPIC_1, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica7 = createTopicPartition(cluster, 2, TOPIC_2, 2);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica8 = createTopicPartition(cluster, 2, TOPIC_4, 1);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica9 = createTopicPartition(cluster, 2, TOPIC_3, 1);
        replica6.setLoad(resource, 15);
        replica7.setLoad(resource, 5);
        replica8.setLoad(resource, 2);
        replica9.setLoad(resource, 6);
        Assertions.assertEquals(load2, cluster.replicasFor(2).stream().mapToDouble(e -> e.load(resource)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica10 = createTopicPartition(cluster, 3, TOPIC_0, 2);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica11 = createTopicPartition(cluster, 3, TOPIC_4, 2);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica12 = createTopicPartition(cluster, 3, TOPIC_1, 1);
        replica10.setLoad(resource, 10);
        replica11.setLoad(resource, 15);
        replica12.setLoad(resource, 25);
        Assertions.assertEquals(load3, cluster.replicasFor(3).stream().mapToDouble(e -> e.load(resource)).sum());

        List<Action> actions = goal.optimize(cluster, goalMap.values());
        Assertions.assertNotEquals(0, actions.size());
        Assertions.assertNotNull(cluster);
        Assertions.assertEquals(0, cluster.replicasFor(4).size());
        for (BrokerUpdater.Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goal.isBrokerAcceptable(broker));
        }
    }

    private void testSingleResourceCapacityOptimizeMultiMove(Resource resource) {
        AbstractGoal goal = getGoalByResource(resource);
        Assertions.assertNotNull(goal);

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        BrokerUpdater.Broker broker0 = createBroker(cluster, RACK, 0, true);
        BrokerUpdater.Broker broker1 = createBroker(cluster, RACK, 1, true);

        double load0 = 90;
        double load1 = 60;
        broker0.setCapacity(resource, 100);
        broker0.setLoad(resource, load0);

        broker1.setCapacity(resource, 100);
        broker1.setLoad(resource, load1);

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_0, 1);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica2 = createTopicPartition(cluster, 0, TOPIC_0, 2);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica3 = createTopicPartition(cluster, 0, TOPIC_0, 3);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica4 = createTopicPartition(cluster, 0, TOPIC_0, 4);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica5 = createTopicPartition(cluster, 0, TOPIC_0, 5);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica6 = createTopicPartition(cluster, 0, TOPIC_0, 6);
        replica0.setLoad(resource, 40);
        replica1.setLoad(resource, 35);
        replica2.setLoad(resource, 1);
        replica3.setLoad(resource, 2);
        replica4.setLoad(resource, 3);
        replica5.setLoad(resource, 2);
        replica6.setLoad(resource, 7);
        Assertions.assertEquals(load0, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(resource)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica7 = createTopicPartition(cluster, 1, TOPIC_1, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica8 = createTopicPartition(cluster, 1, TOPIC_1, 1);
        replica7.setLoad(resource, 20);
        replica8.setLoad(resource, 40);
        Assertions.assertEquals(load1, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(resource)).sum());

        List<Action> actions = goal.optimize(cluster, goalMap.values());
        Assertions.assertNotEquals(0, actions.size());
        Assertions.assertNotNull(cluster);
        for (BrokerUpdater.Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goal.isBrokerAcceptable(broker));
        }
    }

    @Test
    public void testGoalActionAcceptanceScore() {
        testActionAcceptanceScore(Resource.NW_IN);
        testActionAcceptanceScore(Resource.NW_OUT);
    }

    @Test
    public void testSingleResourceCapacityOptimizeOneMove() {
        testSingleResourceCapacityOptimizeOneMove(Resource.NW_IN);
        testSingleResourceCapacityOptimizeOneMove(Resource.NW_OUT);
    }

    @Test
    public void testSingleResourceCapacityOptimizeMultiMove() {
        testSingleResourceCapacityOptimizeMultiMove(Resource.NW_IN);
        testSingleResourceCapacityOptimizeMultiMove(Resource.NW_OUT);
    }

    @Test
    public void testMultiGoalOptimizeWithOneToOneReplicaSwap() {
        AbstractGoal goal = getGoalByResource(Resource.NW_IN);
        Assertions.assertNotNull(goal);

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        BrokerUpdater.Broker broker0 = createBroker(cluster, RACK, 0, true);
        BrokerUpdater.Broker broker1 = createBroker(cluster, RACK, 1, true);

        broker0.setCapacity(Resource.NW_IN, 100);
        broker0.setCapacity(Resource.NW_OUT, 100);
        broker0.setLoad(Resource.NW_IN, 90);
        broker0.setLoad(Resource.NW_OUT, 50);

        broker1.setCapacity(Resource.NW_IN, 100);
        broker1.setCapacity(Resource.NW_OUT, 100);
        broker1.setLoad(Resource.NW_IN, 20);
        broker1.setLoad(Resource.NW_OUT, 90);

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_1, 0);
        replica0.setLoad(Resource.NW_IN, 40);
        replica0.setLoad(Resource.NW_OUT, 30);
        replica1.setLoad(Resource.NW_IN, 50);
        replica1.setLoad(Resource.NW_OUT, 20);
        Assertions.assertEquals(90, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
        Assertions.assertEquals(50, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());

        TopicPartitionReplicaUpdater.TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica3 = createTopicPartition(cluster, 1, TOPIC_1, 1);
        replica2.setLoad(Resource.NW_IN, 5);
        replica2.setLoad(Resource.NW_OUT, 50);
        replica3.setLoad(Resource.NW_IN, 15);
        replica3.setLoad(Resource.NW_OUT, 40);
        Assertions.assertEquals(20, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
        Assertions.assertEquals(90, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());

        List<Action> actions = goal.optimize(cluster, goalMap.values());
        Assertions.assertNotEquals(0, actions.size());
        Assertions.assertNotNull(cluster);
        for (BrokerUpdater.Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goal.isBrokerAcceptable(broker));
        }
    }


    @Test
    public void testMultiGoalOptimizeWithOneToNReplicaSwap() {
        //TODO: implement one-to-N replica swap

//        AbstractGoal goal = getGoalByResource(Resource.NW_IN);
//        Assertions.assertNotNull(goal);
//
//        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
//        BrokerUpdater.Broker broker0 = createBroker(cluster, Resource.NW_IN, RACK, 0, true);
//        BrokerUpdater.Broker broker1 = createBroker(cluster, Resource.NW_IN, RACK, 1, true);
//
//        broker0.setCapacity(Resource.NW_IN, 100);
//        broker0.setCapacity(Resource.NW_OUT, 100);
//        broker0.setLoad(Resource.NW_IN, 90);
//        broker0.setLoad(Resource.NW_OUT, 50);
//
//        broker1.setCapacity(Resource.NW_IN, 100);
//        broker1.setCapacity(Resource.NW_OUT, 100);
//        broker1.setLoad(Resource.NW_IN, 20);
//        broker1.setLoad(Resource.NW_OUT, 90);
//
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_1, 0);
//        replica0.setLoad(Resource.NW_IN, 40);
//        replica0.setLoad(Resource.NW_OUT, 30);
//        replica1.setLoad(Resource.NW_IN, 50);
//        replica1.setLoad(Resource.NW_OUT, 20);
//        Assertions.assertEquals(90, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
//        Assertions.assertEquals(20, cluster.replicasFor(0).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());
//
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_0, 1);
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica3 = createTopicPartition(cluster, 1, TOPIC_1, 1);
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica4 = createTopicPartition(cluster, 1, TOPIC_2, 0);
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica5 = createTopicPartition(cluster, 1, TOPIC_3, 0);
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica6 = createTopicPartition(cluster, 1, TOPIC_4, 0);
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica7 = createTopicPartition(cluster, 1, TOPIC_2, 1);
//        TopicPartitionReplicaUpdater.TopicPartitionReplica replica8 = createTopicPartition(cluster, 1, TOPIC_3, 1);
//        replica2.setLoad(Resource.NW_IN, 1);
//        replica2.setLoad(Resource.NW_OUT, 10);
//        replica3.setLoad(Resource.NW_IN, 2);
//        replica3.setLoad(Resource.NW_OUT, 15);
//        replica4.setLoad(Resource.NW_IN, 3);
//        replica4.setLoad(Resource.NW_OUT, 15);
//        replica5.setLoad(Resource.NW_IN, 4);
//        replica5.setLoad(Resource.NW_OUT, 15);
//        replica6.setLoad(Resource.NW_IN, 5);
//        replica6.setLoad(Resource.NW_OUT, 5);
//        replica7.setLoad(Resource.NW_IN, 2);
//        replica7.setLoad(Resource.NW_OUT, 15);
//        replica8.setLoad(Resource.NW_IN, 3);
//        replica8.setLoad(Resource.NW_OUT, 15);
//        Assertions.assertEquals(20, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(Resource.NW_IN)).sum());
//        Assertions.assertEquals(90, cluster.replicasFor(1).stream().mapToDouble(e -> e.load(Resource.NW_OUT)).sum());
//
//        ClusterModelSnapshot optimizedCluster = goal.optimize(cluster, goalMap.values());
//        Assertions.assertNotNull(optimizedCluster);
//        Assertions.assertEquals(0, optimizedCluster.replicasFor(4).size());
//        for (BrokerUpdater.Broker broker : optimizedCluster.brokers()) {
//            Assertions.assertTrue(goal.isBrokerAcceptable(broker));
//        }
    }
}
