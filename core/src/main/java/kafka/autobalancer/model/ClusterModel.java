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

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.RawMetricType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ClusterModel {
    protected final Logger logger;
    private static final String DEFAULT_RACK_ID = "rack_default";
    private static final long DEFAULT_MAX_TOLERATED_METRICS_DELAY_MS = 60000L;

    /*
     * Guard the change on cluster structure (add/remove for brokers, replicas)
     */
    private final Lock clusterLock = new ReentrantLock();

    /* cluster structure indices*/
    private final Map<Integer, String> brokerIdToRackMap = new HashMap<>();
    private final Map<Integer, BrokerUpdater> brokerMap = new HashMap<>();
    private final Map<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater>> brokerReplicaMap = new HashMap<>();
    private final Map<Uuid, String> idToTopicNameMap = new HashMap<>();
    private final Map<String, Map<Integer, Integer>> topicPartitionReplicaMap = new HashMap<>();

    public ClusterModel() {
        this(null);
    }

    public ClusterModel(LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[ClusterModel]");
        }
        logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    }

    public ClusterModelSnapshot snapshot() {
        return snapshot(Collections.emptySet(), Collections.emptySet(), DEFAULT_MAX_TOLERATED_METRICS_DELAY_MS);
    }

    public ClusterModelSnapshot snapshot(Set<Integer> excludedBrokerIds, Set<String> excludedTopics, long maxToleratedMetricsDelay) {
        ClusterModelSnapshot snapshot = new ClusterModelSnapshot();
        clusterLock.lock();
        try {
            long now = System.currentTimeMillis();
            for (BrokerUpdater brokerUpdater : brokerMap.values()) {
                Broker broker = brokerUpdater.get();
                if (broker == null) {
                    continue;
                }
                if (excludedBrokerIds.contains(broker.getBrokerId())) {
                    continue;
                }
                snapshot.addBroker(brokerIdToRackMap.get(broker.getBrokerId()), broker);
            }
            for (Map.Entry<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater>> entry : brokerReplicaMap.entrySet()) {
                int brokerId = entry.getKey();
                if (snapshot.broker(brokerId) == null) {
                    continue;
                }
                for (TopicPartitionReplicaUpdater replicaUpdater : entry.getValue().values()) {
                    TopicPartitionReplica replica = replicaUpdater.get(now - maxToleratedMetricsDelay);
                    if (replica == null) {
                        logger.warn("Broker {} has out of sync topic-partition {}, will be ignored in this round", brokerId, replicaUpdater.topicPartition());
                        snapshot.removeBroker(brokerIdToRackMap.get(brokerId), brokerId);
                        break;
                    }
                    if (excludedTopics.contains(replica.getTopicPartition().topic())) {
                        continue;
                    }
                    snapshot.addTopicPartition(brokerId, replica);
                }
            }
        } finally {
            clusterLock.unlock();
        }

        snapshot.aggregate();

        return snapshot;
    }

    public boolean updateTopicPartitionMetrics(int brokerId, TopicPartition tp, Map<RawMetricType, Double> metricsMap, long time) {
        TopicPartitionReplicaUpdater replicaUpdater = null;
        clusterLock.lock();
        try {
            Map<TopicPartition, TopicPartitionReplicaUpdater> replicaMap = brokerReplicaMap.get(brokerId);
            if (replicaMap != null) {
                replicaUpdater = replicaMap.get(tp);
            }
        } finally {
            clusterLock.unlock();
        }
        if (replicaUpdater != null) {
            return replicaUpdater.update(metricsMap, time);
        }
        return false;
    }

    public void registerBroker(int brokerId, String rackId) {
        clusterLock.lock();
        try {
            if (brokerMap.containsKey(brokerId)) {
                return;
            }
            BrokerUpdater brokerUpdater = new BrokerUpdater(brokerId, true);
            if (Utils.isBlank(rackId)) {
                rackId = DEFAULT_RACK_ID;
            }
            brokerIdToRackMap.putIfAbsent(brokerId, rackId);
            brokerMap.putIfAbsent(brokerId, brokerUpdater);
            brokerReplicaMap.put(brokerId, new HashMap<>());
        } finally {
            clusterLock.unlock();
        }
    }

    public void unregisterBroker(int brokerId) {
        clusterLock.lock();
        try {
            brokerIdToRackMap.remove(brokerId);
            brokerMap.remove(brokerId);
            brokerReplicaMap.remove(brokerId);
        } finally {
            clusterLock.unlock();
        }
    }

    public void changeBrokerStatus(int brokerId, boolean active) {
        clusterLock.lock();
        try {
            brokerMap.computeIfPresent(brokerId, (id, brokerUpdater) -> {
                brokerUpdater.setActive(active);
                return brokerUpdater;
            });
        } finally {
            clusterLock.unlock();
        }
    }

    public void createTopic(Uuid topicId, String topicName) {
        clusterLock.lock();
        try {
            idToTopicNameMap.putIfAbsent(topicId, topicName);
            topicPartitionReplicaMap.putIfAbsent(topicName, new HashMap<>());
        } finally {
            clusterLock.unlock();
        }
    }

    public void deleteTopic(Uuid topicId) {
        clusterLock.lock();
        try {
            String topicName = idToTopicNameMap.get(topicId);
            if (topicName == null) {
                return;
            }
            idToTopicNameMap.remove(topicId);
            for (Map.Entry<Integer, Integer> entry : topicPartitionReplicaMap.get(topicName).entrySet()) {
                int partitionId = entry.getKey();
                int brokerId = entry.getValue();
                Map<TopicPartition, TopicPartitionReplicaUpdater> replicaMap = brokerReplicaMap.get(brokerId);
                if (replicaMap != null) {
                    replicaMap.remove(new TopicPartition(topicName, partitionId));
                }
            }
            topicPartitionReplicaMap.remove(topicName);
        } finally {
            clusterLock.unlock();
        }
    }

    public void createPartition(Uuid topicId, int partitionId, int brokerId) {
        clusterLock.lock();
        try {
            String topicName = idToTopicNameMap.get(topicId);
            if (topicName == null) {
                return;
            }
            if (!topicPartitionReplicaMap.containsKey(topicName)) {
                logger.error("Create partition on invalid topic {}", topicName);
                return;
            }
            if (!brokerMap.containsKey(brokerId)) {
                logger.error("Create partition for topic {} on invalid broker {}", topicName, brokerId);
                return;
            }
            topicPartitionReplicaMap.get(topicName).put(partitionId, brokerId);
            TopicPartition tp = new TopicPartition(topicName, partitionId);
            brokerReplicaMap.get(brokerId).put(tp, new TopicPartitionReplicaUpdater(tp));
        } finally {
            clusterLock.unlock();
        }
    }

    public void reassignPartition(Uuid topicId, int partitionId, int brokerId) {
        clusterLock.lock();
        try {
            String topicName = idToTopicNameMap.get(topicId);
            if (topicName == null) {
                return;
            }

            if (!topicPartitionReplicaMap.containsKey(topicName)) {
                logger.error("Reassign partition {} on invalid topic {}", partitionId, topicName);
                return;
            }

            if (!brokerMap.containsKey(brokerId)) {
                logger.error("Reassign partition {} for topic {} on invalid broker {}", partitionId, topicName, brokerId);
                return;
            }
            int oldBrokerId = topicPartitionReplicaMap.get(topicName).getOrDefault(partitionId, -1);
            if (oldBrokerId == brokerId) {
                logger.warn("Reassign partition {} for topic {} on same broker {}", partitionId, topicName, oldBrokerId);
                return;
            }
            if (oldBrokerId != -1) {
                TopicPartition tp = new TopicPartition(topicName, partitionId);
                TopicPartitionReplicaUpdater replicaUpdater = brokerReplicaMap.get(oldBrokerId).get(tp);
                brokerReplicaMap.get(brokerId).put(tp, replicaUpdater);
                brokerReplicaMap.get(oldBrokerId).remove(tp);
            }
            topicPartitionReplicaMap.get(topicName).put(partitionId, brokerId);
        } finally {
            clusterLock.unlock();
        }
    }

    public BrokerUpdater brokerUpdater(int brokerId) {
        clusterLock.lock();
        try {
            return brokerMap.get(brokerId);
        } finally {
            clusterLock.unlock();
        }
    }

    public TopicPartitionReplicaUpdater replicaUpdater(int brokerId, TopicPartition tp) {
        clusterLock.lock();
        try {
            if (!brokerReplicaMap.containsKey(brokerId)) {
                return null;
            }
            return brokerReplicaMap.get(brokerId).get(tp);
        } finally {
            clusterLock.unlock();
        }
    }

    public String topicName(Uuid topicId) {
        clusterLock.lock();
        try {
            return idToTopicNameMap.get(topicId);
        } finally {
            clusterLock.unlock();
        }
    }
    /* Code visible for test end*/
}
