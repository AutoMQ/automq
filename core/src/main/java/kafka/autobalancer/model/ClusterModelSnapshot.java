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

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ClusterModelSnapshot {

    private final Map<String, Integer> rackToBrokerMap;
    private final Map<Integer, BrokerUpdater.Broker> brokerMap;
    private final Map<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater.TopicPartitionReplica>> brokerToReplicaMap;

    public ClusterModelSnapshot() {
        rackToBrokerMap = new HashMap<>();
        brokerMap = new HashMap<>();
        brokerToReplicaMap = new HashMap<>();
    }

    public void addBroker(int brokerId, String rack, BrokerUpdater.Broker broker) {
        rackToBrokerMap.putIfAbsent(rack, brokerId);
        brokerMap.putIfAbsent(brokerId, broker);
        brokerToReplicaMap.putIfAbsent(brokerId, new HashMap<>());
    }

    public void removeBroker(String rack, int brokerId) {
        rackToBrokerMap.remove(rack);
        brokerMap.remove(brokerId);
        brokerToReplicaMap.remove(brokerId);
    }

    public void addTopicPartition(int brokerId, TopicPartition tp, TopicPartitionReplicaUpdater.TopicPartitionReplica tpInstance) {
        brokerToReplicaMap.putIfAbsent(brokerId, new HashMap<>());
        brokerToReplicaMap.get(brokerId).put(tp, tpInstance);
    }

    public BrokerUpdater.Broker broker(int brokerId) {
        return brokerMap.get(brokerId);
    }

    public Collection<BrokerUpdater.Broker> brokers() {
        return brokerMap.values();
    }

    public TopicPartitionReplicaUpdater.TopicPartitionReplica replica(int brokerId, TopicPartition tp) {
        if (!brokerToReplicaMap.containsKey(brokerId)) {
            return null;
        }
        if (!brokerToReplicaMap.get(brokerId).containsKey(tp)) {
            return null;
        }
        return brokerToReplicaMap.get(brokerId).get(tp);
    }

    public Collection<TopicPartitionReplicaUpdater.TopicPartitionReplica> replicasFor(int brokerId) {
        return brokerToReplicaMap.get(brokerId).values();
    }

    public void applyAction(Action action) {
        BrokerUpdater.Broker srcBroker = brokerMap.get(action.getSrcBrokerId());
        BrokerUpdater.Broker destBroker = brokerMap.get(action.getDestBrokerId());
        if (srcBroker == null || destBroker == null) {
            return;
        }
        TopicPartitionReplicaUpdater.TopicPartitionReplica srcReplica = brokerToReplicaMap.get(action.getSrcBrokerId()).get(action.getSrcTopicPartition());
        ModelUtils.moveReplicaLoad(srcBroker, destBroker, srcReplica);
        brokerToReplicaMap.get(action.getSrcBrokerId()).remove(action.getSrcTopicPartition());
        brokerToReplicaMap.get(action.getDestBrokerId()).put(action.getSrcTopicPartition(), srcReplica);
        if (action.getType() == ActionType.SWAP) {
            TopicPartitionReplicaUpdater.TopicPartitionReplica destReplica = brokerToReplicaMap.get(action.getDestBrokerId()).get(action.getDestTopicPartition());
            ModelUtils.moveReplicaLoad(destBroker, srcBroker, destReplica);
            brokerToReplicaMap.get(action.getDestBrokerId()).remove(action.getDestTopicPartition());
            brokerToReplicaMap.get(action.getSrcBrokerId()).put(action.getDestTopicPartition(), destReplica);
        }
    }

}
