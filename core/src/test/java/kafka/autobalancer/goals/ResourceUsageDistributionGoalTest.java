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
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater.Broker;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater.TopicPartitionReplica;
import kafka.automq.AutoMQConfig;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static kafka.autobalancer.common.types.Resource.NW_IN;
import static kafka.autobalancer.common.types.Resource.NW_OUT;

@Timeout(60)
@Tag("S3Unit")
public class ResourceUsageDistributionGoalTest extends GoalTestBase {

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @Test
    public void testGoalConfig() {

        Map<String, Object> config = new HashMap<>();
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, NetworkInUsageDistributionGoal.class.getName());
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 5 * 1024 * 1024);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, 0.1);
        config.put(AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_CONFIG, 50 * 1024 * 1024);

        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(config, false);
        List<Goal> goals = controllerConfig.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class);
        Assertions.assertEquals(1, goals.size());
        Assertions.assertInstanceOf(NetworkInUsageDistributionGoal.class, goals.get(0));
        NetworkInUsageDistributionGoal networkInUsageDistributionGoal = (NetworkInUsageDistributionGoal) goals.get(0);
        Assertions.assertEquals(5 * 1024 * 1024, networkInUsageDistributionGoal.usageDetectThreshold);
        Assertions.assertEquals(0.1, networkInUsageDistributionGoal.usageAvgDeviationRatio);
        Assertions.assertEquals(50 * 1024 * 1024, networkInUsageDistributionGoal.linearNormalizerThreshold);

        config.remove(AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_CONFIG);
        controllerConfig = new AutoBalancerControllerConfig(config, false);
        goals = controllerConfig.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class);
        Assertions.assertEquals(1, goals.size());
        Assertions.assertInstanceOf(NetworkInUsageDistributionGoal.class, goals.get(0));
        networkInUsageDistributionGoal = (NetworkInUsageDistributionGoal) goals.get(0);
        Assertions.assertEquals(100 * 1024 * 1024, networkInUsageDistributionGoal.linearNormalizerThreshold);

        config.put(AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_CONFIG, String.valueOf(60 * 1024 * 1024));
        controllerConfig = new AutoBalancerControllerConfig(config, false);
        goals = controllerConfig.getConfiguredInstances(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, Goal.class);
        Assertions.assertEquals(1, goals.size());
        Assertions.assertInstanceOf(NetworkInUsageDistributionGoal.class, goals.get(0));
        networkInUsageDistributionGoal = (NetworkInUsageDistributionGoal) goals.get(0);
        Assertions.assertEquals(60 * 1024 * 1024, networkInUsageDistributionGoal.linearNormalizerThreshold);
    }

    private void testSingleResourceGoalScore(byte resource) {
        AbstractNetworkUsageDistributionGoal goal;
        if (resource == NW_IN) {
            goal = new NetworkInUsageDistributionGoal();
        } else {
            goal = new NetworkOutUsageDistributionGoal();
        }

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);
        Broker broker2 = createBroker(cluster, RACK, 2);
        Broker broker3 = createBroker(cluster, RACK, 3);

        double load0 = 600 * 1024 * 1024;
        broker0.setLoad(resource, load0);
        double load1 = 900 * 1024 * 1024;
        broker1.setLoad(resource, load1);
        double load2 = 0;
        broker2.setLoad(resource, load2);
        double load3 = 500 * 1024 * 1024;
        broker3.setLoad(resource, load3);

        TopicPartitionReplica replica = createTopicPartition(cluster, 0, TOPIC_0, 0);
        replica.setLoad(resource, 50 * 1024 * 1024);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        replica1.setLoad(resource, 50 * 1024 * 1024);

        TopicPartitionReplica replica2 = createTopicPartition(cluster, 3, TOPIC_0, 2);
        replica2.setLoad(resource, 20 * 1024 * 1024);

        goal.configure(Map.of("autobalancer.controller.network.in.distribution.detect.avg.deviation", 0.15,
                "autobalancer.controller.network.in.usage.distribution.detect.threshold", 2 * 1024 * 1024,
                "autobalancer.controller.network.out.distribution.detect.avg.deviation", 0.15,
                "autobalancer.controller.network.out.usage.distribution.detect.threshold", 2 * 1024 * 1024,
            AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_CONFIG, 50 * 1024 * 1024));
        goal.initialize(Set.of(broker0, broker1, broker2));
        Assertions.assertEquals(2 * 1024 * 1024, goal.usageDetectThreshold);
        Assertions.assertEquals(0.15, goal.usageAvgDeviationRatio);
        Assertions.assertEquals(500 * 1024 * 1024, goal.usageAvg);
        Assertions.assertEquals(75 * 1024 * 1024, goal.usageAvgDeviation);
        Assertions.assertEquals(425 * 1024 * 1024, goal.usageDistLowerBound);
        Assertions.assertEquals(575 * 1024 * 1024, goal.usageDistUpperBound);
        Assertions.assertEquals(50 * 1024 * 1024, goal.linearNormalizerThreshold);

        double score0 = goal.brokerScore(broker0);
        double score1 = goal.brokerScore(broker1);
        double score2 = goal.brokerScore(broker2);
        double score3 = goal.brokerScore(broker3);

        Assertions.assertTrue(score0 < 1.0);
        Assertions.assertTrue(score0 > 0.1);
        Assertions.assertTrue(score0 > score1);
        Assertions.assertTrue(score1 < 0.1);
        Assertions.assertTrue(score1 > score2);
        Assertions.assertEquals(1.0, score3);

        Action action = new Action(ActionType.MOVE, replica.getTopicPartition(), 0, 2);
        Action action1 = new Action(ActionType.MOVE, replica1.getTopicPartition(), 1, 2);

        double actionScore = goal.actionAcceptanceScore(action, cluster);
        double actionScore1 = goal.actionAcceptanceScore(action1, cluster);

        Assertions.assertEquals(0.50024, actionScore, 0.00001);
        Assertions.assertEquals(0.50024, actionScore1, 0.00001);
    }

    @Test
    public void testGoalScore() {
        testSingleResourceGoalScore(NW_IN);
        testSingleResourceGoalScore(NW_OUT);
    }

    private void testSingleResourceDistributionOptimizeOneMove(byte resource) {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);
        Broker broker2 = createBroker(cluster, RACK, 2);

        broker0.setLoad(resource, 40 * 1024 * 1024);
        broker1.setLoad(resource, 80 * 1024 * 1024);
        broker2.setLoad(resource, 120 * 1024 * 1024);

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        replica0.setLoad(resource, 40 * 1024 * 1024);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_0, 2);
        replica1.setLoad(resource, 40 * 1024 * 1024);
        replica2.setLoad(resource, 40 * 1024 * 1024);

        TopicPartitionReplica replica3 = createTopicPartition(cluster, 2, TOPIC_0, 3);
        TopicPartitionReplica replica4 = createTopicPartition(cluster, 2, TOPIC_0, 4);
        TopicPartitionReplica replica5 = createTopicPartition(cluster, 2, TOPIC_0, 5);
        replica3.setLoad(resource, 40 * 1024 * 1024);
        replica4.setLoad(resource, 40 * 1024 * 1024);
        replica5.setLoad(resource, 40 * 1024 * 1024);

        Goal goal = getGoalByResource(resource);
        goal.initialize(Set.of(broker0, broker1, broker2));

        Assertions.assertEquals(0.319, goal.actionAcceptanceScore(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 0), 0, 1), cluster), 0.001);
        Assertions.assertEquals(0.319, goal.actionAcceptanceScore(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 0), 0, 2), cluster), 0.001);

        Assertions.assertEquals(0.5, goal.actionAcceptanceScore(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 2), 1, 0), cluster), 0.001);
        Assertions.assertEquals(0.319, goal.actionAcceptanceScore(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 2), 1, 2), cluster), 0.001);

        Assertions.assertEquals(0.608, goal.actionAcceptanceScore(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 4), 2, 0), cluster), 0.001);
        Assertions.assertEquals(0.5, goal.actionAcceptanceScore(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 4), 2, 1), cluster), 0.001);

        List<Action> actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertFalse(actions.isEmpty());
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals(2, actions.get(0).getSrcBrokerId());
        Assertions.assertEquals(0, actions.get(0).getDestBrokerId());
        Assertions.assertEquals(TOPIC_0, actions.get(0).getSrcTopicPartition().topic());
        Assertions.assertTrue(Set.of(3, 4, 5).contains(actions.get(0).getSrcTopicPartition().partition()));
        Assertions.assertNull(actions.get(0).getDestTopicPartition());
    }

    @Test
    public void testSingleResourceDistributionOptimizeOneMove() {
        testSingleResourceDistributionOptimizeOneMove(NW_IN);
        testSingleResourceDistributionOptimizeOneMove(NW_OUT);
    }

    private void testSingleResourceDistributionOptimizeMultiMoveOut(byte resource) {
        AbstractGoal goal = (AbstractGoal) getGoalByResource(resource);
        Assertions.assertNotNull(goal);

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        double load0 = 80;
        double load1 = 10;
        broker0.setLoad(resource, load0);

        broker1.setLoad(resource, load1);

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_0, 1);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 0, TOPIC_0, 2);
        TopicPartitionReplica replica3 = createTopicPartition(cluster, 0, TOPIC_0, 3);
        TopicPartitionReplica replica4 = createTopicPartition(cluster, 0, TOPIC_0, 4);
        TopicPartitionReplica replica5 = createTopicPartition(cluster, 0, TOPIC_0, 5);
        TopicPartitionReplica replica6 = createTopicPartition(cluster, 0, TOPIC_0, 6);
        replica0.setLoad(resource, 10);
        replica1.setLoad(resource, 20);
        replica2.setLoad(resource, 15);
        replica3.setLoad(resource, 5);
        replica4.setLoad(resource, 5);
        replica5.setLoad(resource, 10);
        replica6.setLoad(resource, 15);
        Assertions.assertEquals(load0, cluster.replicasFor(0).stream().mapToDouble(e -> e.loadValue(resource)).sum());

        TopicPartitionReplica replica7 = createTopicPartition(cluster, 1, TOPIC_1, 0);
        TopicPartitionReplica replica8 = createTopicPartition(cluster, 1, TOPIC_1, 1);
        replica7.setLoad(resource, 5);
        replica8.setLoad(resource, 5);
        Assertions.assertEquals(load1, cluster.replicasFor(1).stream().mapToDouble(e -> e.loadValue(resource)).sum());

        Collection<Goal> goals = getGoals();
        goals.forEach(g -> g.initialize(Set.of(broker0, broker1)));
        List<Action> actions = goal.optimize(cluster, goals, Collections.emptyList());
        Assertions.assertNotEquals(0, actions.size());
        Assertions.assertNotNull(cluster);
        for (Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goal.isBrokerAcceptable(broker));
        }
    }

    private void testSingleResourceDistributionOptimizeMultiMoveIn(byte resource) {
        AbstractGoal goal = (AbstractGoal) getGoalByResource(resource);
        Assertions.assertNotNull(goal);

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        double load0 = 10;
        double load1 = 80;
        broker0.setLoad(resource, load0);

        broker1.setLoad(resource, load1);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_1, 0);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 0, TOPIC_1, 1);
        replica1.setLoad(resource, 5);
        replica2.setLoad(resource, 5);
        Assertions.assertEquals(load0, cluster.replicasFor(0).stream().mapToDouble(e -> e.loadValue(resource)).sum());

        TopicPartitionReplica replica3 = createTopicPartition(cluster, 1, TOPIC_0, 0);
        TopicPartitionReplica replica4 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplica replica5 = createTopicPartition(cluster, 1, TOPIC_0, 2);
        TopicPartitionReplica replica6 = createTopicPartition(cluster, 1, TOPIC_0, 3);
        TopicPartitionReplica replica7 = createTopicPartition(cluster, 1, TOPIC_0, 4);
        TopicPartitionReplica replica8 = createTopicPartition(cluster, 1, TOPIC_0, 5);
        TopicPartitionReplica replica9 = createTopicPartition(cluster, 1, TOPIC_0, 6);
        replica3.setLoad(resource, 10);
        replica4.setLoad(resource, 20);
        replica5.setLoad(resource, 15);
        replica6.setLoad(resource, 5);
        replica7.setLoad(resource, 5);
        replica8.setLoad(resource, 10);
        replica9.setLoad(resource, 15);
        Assertions.assertEquals(load1, cluster.replicasFor(1).stream().mapToDouble(e -> e.loadValue(resource)).sum());

        Collection<Goal> goals = getGoals();
        goals.forEach(g -> g.initialize(Set.of(broker0, broker1)));
        List<Action> actions = goal.optimize(cluster, goals, Collections.emptyList());
        Assertions.assertNotEquals(0, actions.size());
        Assertions.assertNotNull(cluster);
        for (Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goal.isBrokerAcceptable(broker));
        }
    }

    @Test
    public void testSingleResourceDistributionOptimizeMultiMove() {
        testSingleResourceDistributionOptimizeMultiMoveOut(NW_IN);
        testSingleResourceDistributionOptimizeMultiMoveOut(NW_OUT);
        testSingleResourceDistributionOptimizeMultiMoveIn(NW_IN);
        testSingleResourceDistributionOptimizeMultiMoveIn(NW_OUT);
    }

    private void testSkipUntrustedBroker(byte resource) {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        broker0.setLoad(resource, 0);
        broker1.setLoad(resource, 80 * 1024 * 1024);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_1, 0);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_1, 1);
        replica1.setLoad(resource, 35 * 1024 * 1024);
        replica2.setLoad(resource, 45 * 1024 * 1024);

        Goal goal = getGoalByResource(resource);
        goal.initialize(Set.of(broker0, broker1));

        List<Action> actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals(new Action(ActionType.MOVE, new TopicPartition(TOPIC_1, 1), 1, 0), actions.get(0));

        broker0.setLoad(resource, 0, false);
        actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertTrue(actions.isEmpty());
    }

    @Test
    public void testSkipUntrustedBroker() {
        testSkipUntrustedBroker(NW_IN);
        testSkipUntrustedBroker(NW_OUT);
    }

    @ParameterizedTest
    @ValueSource(bytes = {NW_IN, NW_OUT})
    public void testSkipOutDatedBroker(byte resource) {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        broker0.setLoad(resource, 0);
        broker0.setMetricsOutOfDate(true);
        broker1.setLoad(resource, 80 * 1024 * 1024);
        broker1.setMetricsOutOfDate(false);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_1, 0);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_1, 1);
        replica1.setLoad(resource, 35 * 1024 * 1024);
        replica2.setLoad(resource, 45 * 1024 * 1024);

        Goal goal = getGoalByResource(resource);
        goal.initialize(Set.of(broker0, broker1));

        List<Action> actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertTrue(actions.isEmpty());

        broker0.setMetricsOutOfDate(false);
        actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals(new Action(ActionType.MOVE, new TopicPartition(TOPIC_1, 1), 1, 0), actions.get(0));
    }

    private ClusterModelSnapshot buildClusterModelSnapshot() {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        broker0.setLoad(NW_IN, 0);
        broker0.setLoad(NW_OUT, 40 * 1024 * 1024);
        broker1.setLoad(NW_IN, 80 * 1024 * 1024);
        broker1.setLoad(NW_OUT, 40 * 1024 * 1024);

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_1, 0);
        replica0.setLoad(NW_IN, 0);
        replica0.setLoad(NW_OUT, 40 * 1024 * 1024);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_1, 1);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_1, 2);
        replica1.setLoad(NW_IN, 45 * 1024 * 1024);
        replica1.setLoad(NW_OUT, 20 * 1024 * 1024);

        replica2.setLoad(NW_IN, 35 * 1024 * 1024);
        replica2.setLoad(NW_OUT, 20 * 1024 * 1024);
        return cluster;
    }

    @Test
    public void testSkipUntrustedPartition() {
        ClusterModelSnapshot cluster = buildClusterModelSnapshot();
        Broker broker0 = cluster.broker(0);
        Broker broker1 = cluster.broker(1);
        TopicPartitionReplica replica0 = cluster.replica(0, new TopicPartition(TOPIC_1, 0));
        TopicPartitionReplica replica1 = cluster.replica(1, new TopicPartition(TOPIC_1, 1));
        TopicPartitionReplica replica2 = cluster.replica(1, new TopicPartition(TOPIC_1, 2));

        Goal nwInGoal = getGoalByResource(NW_IN);
        Goal nwOutGoal = getGoalByResource(NW_OUT);
        nwInGoal.initialize(Set.of(broker0, broker1));
        nwOutGoal.initialize(Set.of(broker0, broker1));

        List<Action> actions = nwInGoal.optimize(cluster, List.of(nwInGoal, nwOutGoal), Collections.emptyList());
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals(new Action(ActionType.MOVE, new TopicPartition(TOPIC_1, 1), 1, 0), actions.get(0));

        broker0.setLoad(NW_OUT, 40 * 1024 * 1024, false);
        replica0.setLoad(NW_OUT, 40 * 1024 * 1024, false);
        actions = nwInGoal.optimize(cluster, List.of(nwInGoal, nwOutGoal), Collections.emptyList());
        Assertions.assertTrue(actions.isEmpty());

        // reset cluster
        cluster = buildClusterModelSnapshot();
        replica1 = cluster.replica(1, new TopicPartition(TOPIC_1, 1));
        replica2 = cluster.replica(1, new TopicPartition(TOPIC_1, 2));
        replica1.setLoad(NW_OUT, 40 * 1024 * 1024);
        replica2.setLoad(NW_OUT, 0, false);
        actions = nwInGoal.optimize(cluster, List.of(nwInGoal, nwOutGoal), Collections.emptyList());
        Assertions.assertTrue(actions.isEmpty());

        replica1.setLoad(NW_OUT, 40 * 1024 * 1024);
        replica2.setLoad(NW_OUT, 0);
        actions = nwInGoal.optimize(cluster, List.of(nwInGoal, nwOutGoal), Collections.emptyList());
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals(new Action(ActionType.MOVE, new TopicPartition(TOPIC_1, 2), 1, 0), actions.get(0));
    }

    private void testMultiGoalOptimizeWithOneToOneReplicaSwap(byte resource) {
        AbstractGoal goal = (AbstractGoal) getGoalByResource(resource);
        Assertions.assertNotNull(goal);

        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        broker0.setLoad(NW_IN, 90);
        broker0.setLoad(NW_OUT, 50);

        broker1.setLoad(NW_IN, 20);
        broker1.setLoad(NW_OUT, 90);

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartitionReplica replica1 = createTopicPartition(cluster, 0, TOPIC_1, 0);
        replica0.setLoad(NW_IN, 40);
        replica0.setLoad(NW_OUT, 30);
        replica1.setLoad(NW_IN, 50);
        replica1.setLoad(NW_OUT, 20);
        Assertions.assertEquals(90, cluster.replicasFor(0).stream().mapToDouble(e -> e.loadValue(NW_IN)).sum());
        Assertions.assertEquals(50, cluster.replicasFor(0).stream().mapToDouble(e -> e.loadValue(NW_OUT)).sum());

        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplica replica3 = createTopicPartition(cluster, 1, TOPIC_1, 1);
        replica2.setLoad(NW_IN, 5);
        replica2.setLoad(NW_OUT, 50);
        replica3.setLoad(NW_IN, 15);
        replica3.setLoad(NW_OUT, 40);
        Assertions.assertEquals(20, cluster.replicasFor(1).stream().mapToDouble(e -> e.loadValue(NW_IN)).sum());
        Assertions.assertEquals(90, cluster.replicasFor(1).stream().mapToDouble(e -> e.loadValue(NW_OUT)).sum());

        List<Action> actions = goal.optimize(cluster, getGoals(), Collections.emptyList());
        System.out.printf("Actions: %s%n", actions);
        Assertions.assertNotEquals(0, actions.size());
        Assertions.assertNotNull(cluster);
        for (Broker broker : cluster.brokers()) {
            Assertions.assertTrue(goal.isBrokerAcceptable(broker));
        }
    }

    @Test
    @Disabled
    public void testMultiGoalOptimizeWithOneToOneReplicaSwap() {
        testMultiGoalOptimizeWithOneToOneReplicaSwap(NW_IN);
        testMultiGoalOptimizeWithOneToOneReplicaSwap(NW_OUT);
    }

    private void setupCluster(byte resource, ClusterModelSnapshot cluster, Broker broker0, Broker broker1) {
        double load0 = 10;
        double load1 = 90;
        broker0.setLoad(resource, load0);
        broker1.setLoad(resource, load1);

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        replica0.setLoad(resource, 10);
        Assertions.assertEquals(load0, cluster.replicasFor(0).stream().mapToDouble(e -> e.loadValue(resource)).sum());

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_0, 2);
        TopicPartitionReplica replica3 = createTopicPartition(cluster, 1, TOPIC_0, 3);
        replica1.setLoad(resource, 20);
        replica2.setLoad(resource, 40);
        replica3.setLoad(resource, 30);
        Assertions.assertEquals(load1, cluster.replicasFor(1).stream().mapToDouble(e -> e.loadValue(resource)).sum());
    }

    private void testNotIncreaseLoadForSlowBroker(byte resource) {
        AbstractGoal goal = (AbstractGoal) getGoalByResource(resource);
        Assertions.assertNotNull(goal);

        // test with normal brokers
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);
        setupCluster(resource, cluster, broker0, broker1);

        Collection<Goal> goals = getGoals();
        for (Goal g : goals) {
            g.initialize(Set.of(broker0, broker1));
        }

        List<Action> actions = goal.optimize(cluster, goals, Collections.emptyList());
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 2), 1, 0), actions.get(0));
        cluster.brokers().forEach(b -> Assertions.assertTrue(goal.isBrokerAcceptable(b)));

        // test with broker0 marked as slow broker
        cluster = new ClusterModelSnapshot();
        broker0 = createBroker(cluster, RACK, 0);
        broker0.setSlowBroker(true);
        broker1 = createBroker(cluster, RACK, 1);
        setupCluster(resource, cluster, broker0, broker1);
        actions = goal.optimize(cluster, getGoals(), Collections.emptyList());
        Assertions.assertTrue(actions.isEmpty());
    }

    @Test
    public void testNotIncreaseLoadForSlowBroker() {
        testNotIncreaseLoadForSlowBroker(NW_IN);
        testNotIncreaseLoadForSlowBroker(NW_OUT);
    }

    @ParameterizedTest
    @ValueSource(bytes = {NW_IN, NW_OUT})
    public void testTrivialActions(byte resource) {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        broker0.setLoad(resource, 30 * 1024 * 1024);
        broker1.setLoad(resource, 70 * 1024 * 1024);

        AbstractResourceUsageDistributionGoal goal;
        if (resource == NW_IN) {
            goal = new NetworkInUsageDistributionGoal();
        } else {
            goal = new NetworkOutUsageDistributionGoal();
        }
        goal.configure(Map.of(
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_TRIVIAL_CHANGE_RATIO, 0.05,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_TRIVIAL_CHANGE_RATIO, 0.05
        ));
        goal.initialize(Set.of(broker0, broker1));
        Assertions.assertTrue(goal.isTrivialLoadChange(broker0, 2 * 1024 * 1024));
        Assertions.assertFalse(goal.isTrivialLoadChange(broker0, 3 * 1024 * 1024));
        Assertions.assertTrue(goal.isTrivialLoadChange(broker1, 2 * 1024 * 1024));
        Assertions.assertFalse(goal.isTrivialLoadChange(broker1, 3 * 1024 * 1024));

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 1, TOPIC_0, 0);
        replica0.setLoad(resource, 2 * 1024 * 1024);
        List<Action> actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertTrue(actions.isEmpty());
        Assertions.assertEquals(broker0.loadValue(resource), 30 * 1024 * 1024);
        Assertions.assertEquals(broker1.loadValue(resource), 70 * 1024 * 1024);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        replica1.setLoad(resource, 1024 * 1024);
        actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertEquals(2, actions.size());
        Assertions.assertEquals(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 0), 1, 0), actions.get(0));
        Assertions.assertEquals(new Action(ActionType.MOVE, new TopicPartition(TOPIC_0, 1), 1, 0), actions.get(1));
        Assertions.assertEquals(broker0.loadValue(resource), 33 * 1024 * 1024);
        Assertions.assertEquals(broker1.loadValue(resource), 67 * 1024 * 1024);
    }

    @ParameterizedTest
    @ValueSource(bytes = {NW_IN, NW_OUT})
    public void testLoadBelowThresholdCluster(byte resource) {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0);
        Broker broker1 = createBroker(cluster, RACK, 1);

        broker0.setLoad(resource, 0);
        broker1.setLoad(resource, 1024);

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 1, TOPIC_0, 0);
        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_0, 2);
        TopicPartitionReplica replica3 = createTopicPartition(cluster, 1, TOPIC_0, 3);
        replica0.setLoad(resource, 256);
        replica1.setLoad(resource, 256);
        replica2.setLoad(resource, 256);
        replica3.setLoad(resource, 256);
        Assertions.assertEquals(0, cluster.replicasFor(0).stream().mapToDouble(e -> e.loadValue(resource)).sum());
        Assertions.assertEquals(1024, cluster.replicasFor(1).stream().mapToDouble(e -> e.loadValue(resource)).sum());

        AbstractResourceUsageDistributionGoal goal;
        if (resource == NW_IN) {
            goal = new NetworkInUsageDistributionGoal();
        } else {
            goal = new NetworkOutUsageDistributionGoal();
        }
        goal.configure(Map.of(
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 1024 * 1024,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 1024 * 1024
        ));
        goal.initialize(Set.of(broker0, broker1));
        Assertions.assertTrue(goal.isBrokerAcceptable(broker0));
        Assertions.assertTrue(goal.isBrokerAcceptable(broker1));
        List<Action> actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertTrue(actions.isEmpty());

        goal.configure(Map.of(
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 0,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 0
        ));
        Assertions.assertFalse(goal.isBrokerAcceptable(broker0));
        Assertions.assertFalse(goal.isBrokerAcceptable(broker1));
        actions = goal.optimize(cluster, List.of(goal), Collections.emptyList());
        Assertions.assertEquals(2, actions.size());
        Assertions.assertEquals(512, cluster.replicasFor(0).stream().mapToDouble(e -> e.loadValue(resource)).sum());
        Assertions.assertEquals(512, cluster.replicasFor(1).stream().mapToDouble(e -> e.loadValue(resource)).sum());
    }
}
