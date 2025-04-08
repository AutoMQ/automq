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

import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater.Broker;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater.TopicPartitionReplica;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static kafka.autobalancer.common.types.Resource.NW_IN;
import static kafka.autobalancer.common.types.Resource.NW_OUT;

@Timeout(60)
@Tag("S3Unit")
public class GoalTestBase {
    private final Map<String, Goal> goalMap = new HashMap<>();
    protected static final String RACK = "default";
    protected static final String TOPIC_0 = "TestTopic0";
    protected static final String TOPIC_1 = "TestTopic1";
    protected static final String TOPIC_2 = "TestTopic2";
    protected static final String TOPIC_3 = "TestTopic3";
    protected static final String TOPIC_4 = "TestTopic4";

    protected void setup() {
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
        for (AbstractGoal goal : goalList) {
            goalMap.put(goal.name(), goal);
        }
    }

    protected Goal getGoalByResource(byte resource) {
        Goal goal = null;
        switch (resource) {
            case NW_IN:
                goal = goalMap.get(NetworkInUsageDistributionGoal.class.getSimpleName());
                break;
            case NW_OUT:
                goal = goalMap.get(NetworkOutUsageDistributionGoal.class.getSimpleName());
                break;
            default:
                break;
        }
        return goal;
    }

    protected Collection<Goal> getGoals() {
        return goalMap.values();
    }

    protected Broker createBroker(ClusterModelSnapshot cluster, String rack, int brokerId) {
        return createBroker(cluster, rack, brokerId, MetricVersion.V0);
    }

    protected Broker createBroker(ClusterModelSnapshot cluster, String rack, int brokerId, MetricVersion metricVersion) {
        Broker broker = new Broker(brokerId, rack, System.currentTimeMillis(), null, metricVersion, false);
        cluster.addBroker(broker);
        return broker;
    }

    protected TopicPartitionReplica createTopicPartition(ClusterModelSnapshot cluster,
                                                         int brokerId,
                                                         String topic,
                                                         int partition) {
        return createTopicPartition(cluster, brokerId, topic, partition, MetricVersion.V0);
    }

    protected TopicPartitionReplica createTopicPartition(ClusterModelSnapshot cluster,
                                                         int brokerId,
                                                         String topic,
                                                         int partition,
                                                         MetricVersion metricVersion) {
        TopicPartition tp = new TopicPartition(topic, partition);
        TopicPartitionReplica replica = new TopicPartitionReplica(tp, System.currentTimeMillis(), metricVersion, false);
        cluster.addTopicPartition(brokerId, tp, replica);
        return replica;
    }
}
