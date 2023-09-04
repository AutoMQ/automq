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

import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;

@Tag("esUnit")
public class GoalTestBase {
    protected static final String RACK = "default";
    protected static final String TOPIC_0 = "TestTopic0";
    protected static final String TOPIC_1 = "TestTopic1";
    protected static final String TOPIC_2 = "TestTopic2";
    protected static final String TOPIC_3 = "TestTopic3";
    protected static final String TOPIC_4 = "TestTopic4";

    protected BrokerUpdater.Broker createBroker(ClusterModelSnapshot cluster, String rack,
                                                int brokerId, boolean active) {
        BrokerUpdater.Broker broker = new BrokerUpdater.Broker(brokerId);
        broker.setActive(active);
        cluster.addBroker(rack, broker);
        return broker;
    }

    protected TopicPartitionReplicaUpdater.TopicPartitionReplica createTopicPartition(ClusterModelSnapshot cluster,
                                                                                      int brokerId,
                                                                                      String topic,
                                                                                      int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica = new TopicPartitionReplicaUpdater.TopicPartitionReplica(tp);
        cluster.addTopicPartition(brokerId, replica);
        return replica;
    }
}
