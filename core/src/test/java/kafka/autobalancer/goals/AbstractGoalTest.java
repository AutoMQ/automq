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
import kafka.autobalancer.model.BrokerUpdater.Broker;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater.TopicPartitionReplica;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

@Tag("S3Unit")
public class AbstractGoalTest extends GoalTestBase {

    private final Map<String, Goal> goalMap = new HashMap<>();

    @BeforeEach
    public void setup() {
        Map<String, Object> config = new HashMap<>();
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, new StringJoiner(",")
                .add(NetworkInUsageDistributionGoal.class.getName())
                .add(NetworkOutUsageDistributionGoal.class.getName()).toString());
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 0);
        config.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD, 0);
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
    public void testCalculateAcceptanceScore() {
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        Broker broker0 = createBroker(cluster, RACK, 0, true);
        Broker broker1 = createBroker(cluster, RACK, 1, true);
        Broker broker2 = createBroker(cluster, RACK, 2, true);

        broker0.setLoad(Resource.NW_IN, 40 * 1024 * 1024);
        broker1.setLoad(Resource.NW_IN, 80 * 1024 * 1024);
        broker2.setLoad(Resource.NW_IN, 120 * 1024 * 1024);

        TopicPartitionReplica replica0 = createTopicPartition(cluster, 0, TOPIC_0, 0);
        replica0.setLoad(Resource.NW_IN, 40 * 1024 * 1024);

        TopicPartitionReplica replica1 = createTopicPartition(cluster, 1, TOPIC_0, 1);
        TopicPartitionReplica replica2 = createTopicPartition(cluster, 1, TOPIC_0, 2);
        replica1.setLoad(Resource.NW_IN, 40 * 1024 * 1024);
        replica2.setLoad(Resource.NW_IN, 40 * 1024 * 1024);

        TopicPartitionReplica replica3 = createTopicPartition(cluster, 2, TOPIC_0, 3);
        TopicPartitionReplica replica4 = createTopicPartition(cluster, 2, TOPIC_0, 4);
        TopicPartitionReplica replica5 = createTopicPartition(cluster, 2, TOPIC_0, 5);
        replica3.setLoad(Resource.NW_IN, 40 * 1024 * 1024);
        replica4.setLoad(Resource.NW_IN, 40 * 1024 * 1024);
        replica5.setLoad(Resource.NW_IN, 40 * 1024 * 1024);

        Goal goal = goalMap.get(NetworkInUsageDistributionGoal.class.getSimpleName());
        AbstractResourceDistributionGoal distributionGoal = (AbstractResourceDistributionGoal) goal;
        distributionGoal.initialize(Set.of(broker0, broker1, broker2));

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
}
