/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.common.types.metrics.AbnormalMetric;

import org.apache.kafka.common.TopicPartition;

import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ClusterModelSnapshot {
    protected static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    private final Map<Integer, BrokerUpdater.Broker> brokerMap;
    private final Map<Integer, Map<TopicPartition, TopicPartitionReplicaUpdater.TopicPartitionReplica>> brokerToReplicaMap;

    public ClusterModelSnapshot() {
        brokerMap = new HashMap<>();
        brokerToReplicaMap = new HashMap<>();
    }

    public void addBroker(BrokerUpdater.Broker broker) {
        brokerMap.putIfAbsent(broker.getBrokerId(), broker);
        brokerToReplicaMap.putIfAbsent(broker.getBrokerId(), new HashMap<>());
    }

    public void removeBroker(int brokerId) {
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

    public void undoAction(Action action) {
        Action undoAction = action.undo();
        applyAction(undoAction);
    }

    public void markSlowBrokers() {
        Map<BrokerUpdater.Broker, Map<Byte, Snapshot>> brokerMetricsValues = new HashMap<>();
        Map<Byte, Map<BrokerUpdater.Broker, Snapshot>> metricsValues = new HashMap<>();
        for (BrokerUpdater.Broker broker : brokerMap.values()) {
            if (!broker.getMetricVersion().isSlowBrokerSupported()) {
                LOGGER.warn("Slow broker detection is not supported for broker-{} with version {}.", broker.getBrokerId(), broker.getMetricVersion());
                continue;
            }
            Map<Byte, Snapshot> metricsValue = brokerMetricsValues.computeIfAbsent(broker, k -> new HashMap<>());
            for (Map.Entry<Byte, Snapshot> entry : broker.getMetricsSnapshot().entrySet()) {
                Snapshot snapshot = entry.getValue();
                if (snapshot == null) {
                    continue;
                }
                Map<BrokerUpdater.Broker, Snapshot> brokerMetric = metricsValues.computeIfAbsent(entry.getKey(), k -> new HashMap<>());
                brokerMetric.put(broker, snapshot);
                metricsValue.put(entry.getKey(), snapshot);
            }
        }
        for (Map.Entry<BrokerUpdater.Broker, Map<Byte, Snapshot>> entry : brokerMetricsValues.entrySet()) {
            BrokerUpdater.Broker broker = entry.getKey();
            Map<Byte, Snapshot> metricsValue = entry.getValue();
            for (Map.Entry<Byte, Snapshot> metricEntry : metricsValue.entrySet()) {
                Byte metricType = metricEntry.getKey();
                Snapshot snapshot = metricEntry.getValue();
                AbnormalMetric abnormalMetric = RawMetricTypes.ofAbnormalType(metricType);
                if (abnormalMetric == null) {
                    continue;
                }
                if (abnormalMetric.isAbnormal(snapshot, metricsValues.get(metricType))) {
                    broker.setSlowBroker(true);
                }
            }
        }
    }

}
