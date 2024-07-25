/*
 * Copyright 2024, AutoMQ HK Limited.
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
import kafka.autobalancer.common.types.Resource;
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
    protected final Lock clusterLock = new ReentrantLock();

    /* cluster structure indices*/
    protected final Map<Integer, BrokerUpdater> brokerMap = new HashMap<>();
    protected final Map<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater>> brokerReplicaMap = new HashMap<>();
    protected final Map<Uuid, String> idToTopicNameMap = new HashMap<>();
    protected final Map<String, Map<Integer, Integer>> topicPartitionReplicaMap = new HashMap<>();

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

    public ClusterLoad getClusterLoad(long maxToleratedMetricsDelay) {
        clusterLock.lock();
        try {
            Map<Integer, Double> brokerLoads = new HashMap<>();
            Map<TopicPartition, Double> tpLoads = new HashMap<>();
            boolean invalid = false;
            long now = System.currentTimeMillis();
            for (Map.Entry<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater>> entry : brokerReplicaMap.entrySet()) {
                int brokerId = entry.getKey();
                brokerLoads.put(brokerId, 0.0);
                for (Map.Entry<TopicPartition, TopicPartitionReplicaUpdater> tpEntry : entry.getValue().entrySet()) {
                    TopicPartition tp = tpEntry.getKey();
                    TopicPartitionReplicaUpdater replicaUpdater = tpEntry.getValue();
                    if (!replicaUpdater.isValidInstance()) {
                        continue;
                    }
                    TopicPartitionReplicaUpdater.TopicPartitionReplica replica =
                            (TopicPartitionReplicaUpdater.TopicPartitionReplica) replicaUpdater.get(now - maxToleratedMetricsDelay);
                    if (replica == null) {
                        invalid = true;
                        brokerLoads = null;
                        tpLoads = null;
                        break;
                    }
                    tpLoads.put(tp, partitionLoad(replica));
                    brokerLoads.compute(brokerId, (id, load) -> {
                        if (load == null) {
                            return partitionLoad(replica);
                        }
                        return load + partitionLoad(replica);
                    });
                }
                if (invalid) {
                    break;
                }
            }
            return new ClusterLoad(brokerLoads, tpLoads);
        } finally {
            clusterLock.unlock();
        }
    }

    protected double partitionLoad(TopicPartitionReplicaUpdater.TopicPartitionReplica replica) {
        return replica.loadValue(Resource.NW_IN) + replica.loadValue(Resource.NW_OUT);
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

    public boolean updateBrokerMetrics(int brokerId, Iterable<Map.Entry<Byte, Double>> metricsMap, long time) {
        BrokerUpdater brokerUpdater = null;
        clusterLock.lock();
        try {
            brokerUpdater = brokerMap.get(brokerId);
            if (brokerUpdater != null) {
                boolean ret = brokerUpdater.update(metricsMap, time);
                for (TopicPartitionReplicaUpdater replicaUpdater : brokerReplicaMap.get(brokerId).values()) {
                    replicaUpdater.setMetricVersion(brokerUpdater.metricVersion());
                }
                return ret;
            }
        } finally {
            clusterLock.unlock();
        }
        return false;
    }

    public boolean updateTopicPartitionMetrics(int brokerId, TopicPartition tp, Iterable<Map.Entry<Byte, Double>> metricsMap, long time) {
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

    public void registerBroker(int brokerId, String rackId, boolean active) {
        clusterLock.lock();
        try {
            if (brokerMap.containsKey(brokerId)) {
                return;
            }
            if (Utils.isBlank(rackId)) {
                rackId = DEFAULT_RACK_ID;
            }
            BrokerUpdater brokerUpdater = createBrokerUpdater(brokerId, rackId, active);
            brokerMap.putIfAbsent(brokerId, brokerUpdater);
            brokerReplicaMap.put(brokerId, new HashMap<>());
        } finally {
            clusterLock.unlock();
        }
    }

    public BrokerUpdater createBrokerUpdater(int brokerId, String rack, boolean active) {
        return new BrokerUpdater(brokerId, rack, active);
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
            Map<Integer, Integer> partitionReplicaMap = topicPartitionReplicaMap.get(topicName);
            if (partitionReplicaMap == null) {
                logger.error("Failed to find topic name for id {} when deleting topic", topicId);
                return;
            }
            for (Map.Entry<Integer, Integer> entry : partitionReplicaMap.entrySet()) {
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

            Map<Integer, Integer> partitionReplicaMap = topicPartitionReplicaMap.get(topicName);
            if (partitionReplicaMap == null) {
                logger.error("Failed to find topic name for id {} when reassigning partition", topicId);
                return;
            }

            if (!brokerMap.containsKey(brokerId)) {
                logger.error("Reassign partition {} for topic {} on invalid broker {}", partitionId, topicName, brokerId);
                return;
            }
            int oldBrokerId = partitionReplicaMap.getOrDefault(partitionId, -1);
            if (oldBrokerId == brokerId) {
                return;
            }
            TopicPartition tp = new TopicPartition(topicName, partitionId);
            if (oldBrokerId != -1 && brokerReplicaMap.containsKey(oldBrokerId)) {
                brokerReplicaMap.get(oldBrokerId).remove(tp);
            }
            brokerReplicaMap.get(brokerId).put(tp, createReplicaUpdater(tp));
            partitionReplicaMap.put(partitionId, brokerId);
        } finally {
            clusterLock.unlock();
        }
    }

    public void deletePartition(Uuid topicId, int partitionId) {
        clusterLock.lock();
        try {
            String topicName = idToTopicNameMap.get(topicId);
            if (topicName == null) {
                return;
            }
            Map<Integer, Integer> partitionReplicaMap = topicPartitionReplicaMap.get(topicName);
            if (partitionReplicaMap == null) {
                logger.error("Failed to find topic name for id {} when deleting partition", topicId);
                return;
            }
            if (!partitionReplicaMap.containsKey(partitionId)) {
                return;
            }
            int brokerId = partitionReplicaMap.remove(partitionId);
            if (brokerReplicaMap.containsKey(brokerId)) {
                brokerReplicaMap.get(brokerId).remove(new TopicPartition(topicName, partitionId));
            }
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

    public static class ClusterLoad {
        private final Map<Integer, Double> brokerLoads;
        private final Map<TopicPartition, Double> partitionLoads;

        public ClusterLoad(Map<Integer, Double> brokerLoads, Map<TopicPartition, Double> partitionLoads) {
            this.brokerLoads = brokerLoads;
            this.partitionLoads = partitionLoads;
        }

        public Map<Integer, Double> brokerLoads() {
            return brokerLoads;
        }

        public Map<TopicPartition, Double> partitionLoads() {
            return partitionLoads;
        }
    }
}
