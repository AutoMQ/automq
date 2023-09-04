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
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;
import kafka.autobalancer.metricsreporter.metric.BrokerMetrics;
import kafka.autobalancer.metricsreporter.metric.TopicPartitionMetrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ClusterModel implements BrokerStatusListener, TopicPartitionStatusListener {
    private final Logger logger;
    private static final String DEFAULT_RACK_ID = "rack_default";

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

    private final long maxToleratedMetricsDelay;
    private final boolean aggregateBrokerLoad;

    public ClusterModel(AutoBalancerControllerConfig config) {
        this(config, null);
    }

    public ClusterModel(AutoBalancerControllerConfig config, LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[ClusterModel]");
        }
        logger = logContext.logger(ClusterModel.class);
        maxToleratedMetricsDelay = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS);
        aggregateBrokerLoad = config.getBoolean(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION);
    }

    public ClusterModelSnapshot snapshot() {
        return snapshot(Collections.emptySet(), Collections.emptySet());
    }

    public ClusterModelSnapshot snapshot(Set<Integer> excludedBrokerIds, Set<String> excludedTopics) {
        ClusterModelSnapshot snapshot = new ClusterModelSnapshot();
        clusterLock.lock();
        try {
            long now = System.currentTimeMillis();
            for (BrokerUpdater brokerUpdater : brokerMap.values()) {
                BrokerUpdater.Broker broker = brokerUpdater.get(now - maxToleratedMetricsDelay);
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
                    TopicPartitionReplicaUpdater.TopicPartitionReplica replica = replicaUpdater.get(now - maxToleratedMetricsDelay);
                    if (replica == null) {
                        continue;
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

        if (aggregateBrokerLoad) {
            snapshot.aggregate();
        }

        return snapshot;
    }

    public boolean updateBroker(BrokerMetrics brokerMetrics) {
        BrokerUpdater brokerUpdater = null;
        clusterLock.lock();
        try {
            brokerUpdater = brokerMap.get(brokerMetrics.brokerId());
        } finally {
            clusterLock.unlock();
        }
        if (brokerUpdater != null) {
            return brokerUpdater.update(brokerMetrics);
        }
        return false;
    }

    public boolean updateTopicPartition(TopicPartitionMetrics topicPartitionMetrics) {
        TopicPartitionReplicaUpdater replicaUpdater = null;
        clusterLock.lock();
        try {
            Map<TopicPartition, TopicPartitionReplicaUpdater> replicaMap = brokerReplicaMap.get(topicPartitionMetrics.brokerId());
            if (replicaMap != null) {
                replicaUpdater = replicaMap.get(new TopicPartition(topicPartitionMetrics.topic(), topicPartitionMetrics.partition()));
            }
        } finally {
            clusterLock.unlock();
        }
        if (replicaUpdater != null) {
            return replicaUpdater.update(topicPartitionMetrics);
        }
        return false;
    }

    @Override
    public void onBrokerRegister(RegisterBrokerRecord record) {
        clusterLock.lock();
        try {
            if (brokerMap.containsKey(record.brokerId())) {
                return;
            }
            String rackId = StringUtils.isEmpty(record.rack()) ? DEFAULT_RACK_ID : record.rack();
            BrokerUpdater brokerUpdater = new BrokerUpdater(record.brokerId());
            brokerUpdater.setActive(true);
            brokerIdToRackMap.putIfAbsent(record.brokerId(), rackId);
            brokerMap.putIfAbsent(record.brokerId(), brokerUpdater);
            brokerReplicaMap.put(record.brokerId(), new HashMap<>());
        } finally {
            clusterLock.unlock();
        }
    }

    @Override
    public void onBrokerUnregister(UnregisterBrokerRecord record) {
        clusterLock.lock();
        try {
            brokerIdToRackMap.remove(record.brokerId());
            brokerMap.remove(record.brokerId());
            brokerReplicaMap.remove(record.brokerId());
        } finally {
            clusterLock.unlock();
        }
    }

    @Override
    public void onBrokerRegistrationChanged(BrokerRegistrationChangeRecord record) {
        BrokerUpdater brokerUpdater;
        clusterLock.lock();
        try {
            if (!brokerMap.containsKey(record.brokerId())) {
                return;
            }
            brokerUpdater = brokerMap.get(record.brokerId());
        } finally {
            clusterLock.unlock();
        }
        if (brokerUpdater != null) {
            brokerUpdater.setActive(record.fenced() != BrokerRegistrationFencingChange.FENCE.value()
                    && record.inControlledShutdown() != BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value());
        }
    }

    @Override
    public void onTopicCreate(TopicRecord record) {
        clusterLock.lock();
        try {
            idToTopicNameMap.putIfAbsent(record.topicId(), record.name());
            topicPartitionReplicaMap.putIfAbsent(record.name(), new HashMap<>());
        } finally {
            clusterLock.unlock();
        }
    }

    @Override
    public void onTopicDelete(RemoveTopicRecord record) {
        clusterLock.lock();
        try {
            String topicName = idToTopicNameMap.get(record.topicId());
            if (topicName == null) {
                return;
            }
            idToTopicNameMap.remove(record.topicId());
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

    @Override
    public void onPartitionCreate(PartitionRecord record) {
        clusterLock.lock();
        try {
            String topicName = idToTopicNameMap.get(record.topicId());
            if (topicName == null) {
                return;
            }
            if (record.replicas().size() != 1) {
                logger.error("Illegal replica size {} for {}-{}", record.replicas().size(), topicName, record.partitionId());
                return;
            }
            if (!topicPartitionReplicaMap.containsKey(topicName)) {
                logger.error("Create partition on invalid topic {}", topicName);
                return;
            }
            int brokerIdToCreateOn = record.replicas().iterator().next();
            if (!brokerMap.containsKey(brokerIdToCreateOn)) {
                logger.error("Create partition for topic {} on invalid broker {}", topicName, brokerIdToCreateOn);
                return;
            }
            topicPartitionReplicaMap.get(topicName).put(record.partitionId(), brokerIdToCreateOn);
            TopicPartition tp = new TopicPartition(topicName, record.partitionId());
            brokerReplicaMap.get(brokerIdToCreateOn).put(tp, new TopicPartitionReplicaUpdater(tp));
        } finally {
            clusterLock.unlock();
        }
    }

    @Override
    public void onPartitionChange(PartitionChangeRecord record) {
        clusterLock.lock();
        try {
            String topicName = idToTopicNameMap.get(record.topicId());
            if (topicName == null) {
                return;
            }

            if (record.replicas() == null || record.replicas().size() != 1) {
                return;
            }
            if (!topicPartitionReplicaMap.containsKey(topicName)) {
                logger.error("Reassign partition {} on invalid topic {}", record.partitionId(), topicName);
                return;
            }
            int brokerIdToReassign = record.replicas().iterator().next();
            if (!brokerMap.containsKey(brokerIdToReassign)) {
                logger.error("Reassign partition {} for topic {} on invalid broker {}", record.partitionId(), topicName, brokerIdToReassign);
                return;
            }
            int oldBrokerId = topicPartitionReplicaMap.get(topicName).getOrDefault(record.partitionId(), -1);
            if (oldBrokerId == brokerIdToReassign) {
                logger.warn("Reassign partition {} for topic {} on same broker {}, {}", record.partitionId(), topicName, oldBrokerId, record);
                return;
            }
            if (oldBrokerId != -1) {
                TopicPartition tp = new TopicPartition(topicName, record.partitionId());
                TopicPartitionReplicaUpdater replicaUpdater = brokerReplicaMap.get(oldBrokerId).get(tp);
                brokerReplicaMap.get(brokerIdToReassign).put(tp, replicaUpdater);
                brokerReplicaMap.get(oldBrokerId).remove(tp);
            }
            topicPartitionReplicaMap.get(topicName).put(record.partitionId(), brokerIdToReassign);
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
