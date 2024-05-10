/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.model;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;
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
     * Guard the access on cluster structure (read/add/remove for brokers, replicas)
     */
    private final Lock clusterLock = new ReentrantLock();

    /* cluster structure indices*/
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
        S3StreamKafkaMetricsManager.setAutoBalancerMetricsTimeMapSupplier(this::calculateBrokerLatestMetricsTime);
    }

    Map<Integer, Long> calculateBrokerLatestMetricsTime() {
        clusterLock.lock();
        try {
            Map<Integer, Long> metricsTimeMap = new HashMap<>();
            // Record latest broker metric time
            for (Map.Entry<Integer, BrokerUpdater> entry : brokerMap.entrySet()) {
                int brokerId = entry.getKey();
                BrokerUpdater brokerUpdater = entry.getValue();
                if (brokerUpdater.isValidInstance()) {
                    metricsTimeMap.put(brokerId, brokerUpdater.getTimestamp());
                }
            }
            // Record minimum latest topic partition metric time
            for (Map.Entry<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater>> entry : brokerReplicaMap.entrySet()) {
                int brokerId = entry.getKey();
                if (!metricsTimeMap.containsKey(brokerId)) {
                    continue;
                }
                Map<TopicPartition, TopicPartitionReplicaUpdater> replicaMap = entry.getValue();
                for (Map.Entry<TopicPartition, TopicPartitionReplicaUpdater> tpEntry : replicaMap.entrySet()) {
                    TopicPartitionReplicaUpdater replicaUpdater = tpEntry.getValue();
                    if (replicaUpdater.isValidInstance()) {
                        metricsTimeMap.put(brokerId, Math.min(metricsTimeMap.get(brokerId), replicaUpdater.getTimestamp()));
                    }
                }
            }
            return metricsTimeMap;
        } finally {
            clusterLock.unlock();
        }
    }

    public ClusterModelSnapshot snapshot() {
        return snapshot(Collections.emptySet(), Collections.emptySet(), DEFAULT_MAX_TOLERATED_METRICS_DELAY_MS);
    }

    public ClusterModelSnapshot snapshot(Set<Integer> excludedBrokerIds, Set<String> excludedTopics, long maxToleratedMetricsDelay) {
        ClusterModelSnapshot snapshot = createSnapshot();
        clusterLock.lock();
        try {
            long now = System.currentTimeMillis();
            for (Map.Entry<Integer, BrokerUpdater> entry : brokerMap.entrySet()) {
                int brokerId = entry.getKey();
                if (excludedBrokerIds.contains(brokerId)) {
                    continue;
                }
                BrokerUpdater brokerUpdater = entry.getValue();
                if (!brokerUpdater.isValidInstance()) {
                    continue;
                }
                BrokerUpdater.Broker broker = (BrokerUpdater.Broker) brokerUpdater.get(now - maxToleratedMetricsDelay);
                if (broker == null) {
                    logger.warn("Broker {} metrics is out of sync, will be ignored in this round", brokerId);
                    continue;
                }
                snapshot.addBroker(broker);
            }
            for (Map.Entry<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater>> entry : brokerReplicaMap.entrySet()) {
                int brokerId = entry.getKey();
                BrokerUpdater.Broker broker = snapshot.broker(brokerId);
                if (broker == null) {
                    continue;
                }
                Map<Byte, AbstractInstanceUpdater.Load> totalLoads = new HashMap<>();
                for (Map.Entry<TopicPartition, TopicPartitionReplicaUpdater> tpEntry : entry.getValue().entrySet()) {
                    TopicPartition tp = tpEntry.getKey();
                    TopicPartitionReplicaUpdater replicaUpdater = tpEntry.getValue();
                    if (!replicaUpdater.isValidInstance()) {
                        continue;
                    }
                    TopicPartitionReplicaUpdater.TopicPartitionReplica replica =
                            (TopicPartitionReplicaUpdater.TopicPartitionReplica) replicaUpdater.get(now - maxToleratedMetricsDelay);
                    if (replica == null) {
                        logger.warn("Broker {} has out of sync topic-partition {}, will be ignored in this round", brokerId, tp);
                        snapshot.removeBroker(brokerId);
                        break;
                    }
                    accumulateLoads(totalLoads, replica);
                    if (!excludedTopics.contains(tp.topic())) {
                        snapshot.addTopicPartition(brokerId, tp, replica);
                    }
                }
                for (Map.Entry<Byte, AbstractInstanceUpdater.Load> loadEntry : totalLoads.entrySet()) {
                    broker.setLoad(loadEntry.getKey(), loadEntry.getValue());
                }
            }
        } finally {
            clusterLock.unlock();
        }

        return snapshot;
    }

    private void accumulateLoads(Map<Byte, AbstractInstanceUpdater.Load> totalLoads, TopicPartitionReplicaUpdater.TopicPartitionReplica replica) {
        for (Map.Entry<Byte, AbstractInstanceUpdater.Load> load : replica.getLoads().entrySet()) {
            byte resource = load.getKey();
            totalLoads.compute(resource, (r, totalLoad) -> {
                if (totalLoad == null) {
                    return new AbstractInstanceUpdater.Load(load.getValue());
                }
                totalLoad.add(load.getValue());
                return totalLoad;
            });
        }
    }

    protected ClusterModelSnapshot createSnapshot() {
        return new ClusterModelSnapshot();
    }

    public boolean updateBrokerMetrics(int brokerId, Map<Byte, Double> metricsMap, long time) {
        BrokerUpdater brokerUpdater = null;
        clusterLock.lock();
        try {
            brokerUpdater = brokerMap.get(brokerId);
        } finally {
            clusterLock.unlock();
        }
        if (brokerUpdater != null) {
            return brokerUpdater.update(metricsMap, time);
        }
        return false;
    }

    public boolean updateTopicPartitionMetrics(int brokerId, TopicPartition tp, Map<Byte, Double> metricsMap, long time) {
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
            if (Utils.isBlank(rackId)) {
                rackId = DEFAULT_RACK_ID;
            }
            BrokerUpdater brokerUpdater = createBrokerUpdater(brokerId, rackId);
            brokerMap.putIfAbsent(brokerId, brokerUpdater);
            brokerReplicaMap.put(brokerId, new HashMap<>());
        } finally {
            clusterLock.unlock();
        }
    }

    public BrokerUpdater createBrokerUpdater(int brokerId, String rack) {
        return new BrokerUpdater(brokerId, rack, true);
    }

    public void unregisterBroker(int brokerId) {
        clusterLock.lock();
        try {
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
            brokerReplicaMap.get(brokerId).put(tp, createReplicaUpdater(tp));
        } finally {
            clusterLock.unlock();
        }
    }

    public TopicPartitionReplicaUpdater createReplicaUpdater(TopicPartition tp) {
        return new TopicPartitionReplicaUpdater(tp);
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
