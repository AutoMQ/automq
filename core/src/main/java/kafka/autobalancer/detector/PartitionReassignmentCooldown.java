/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.autobalancer.detector;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks partitions successfully submitted by this controller's Auto Balancer and temporarily removes them from
 * optimization candidates. Controller-driven recovery is outside this state, and partitions on detected slow brokers
 * remain eligible. The local history is intentionally bounded by the configured cooldown and is not persisted across
 * controller failover.
 */
final class PartitionReassignmentCooldown {
    private final Time time;
    private final Map<TopicPartition, Long> lastReassignmentTimestamps = new ConcurrentHashMap<>();

    PartitionReassignmentCooldown(Time time) {
        this.time = time;
    }

    void record(List<Action> actions, long cooldownMs) {
        if (cooldownMs <= 0) {
            return;
        }
        long timestamp = time.milliseconds();
        // SWAP actions are split into MOVEs by AnomalyDetectorImpl.checkAndMergeActions() before execution.
        for (Action action : actions) {
            lastReassignmentTimestamps.put(action.getSrcTopicPartition(), timestamp);
        }
    }

    int excludeFrom(ClusterModelSnapshot snapshot, long cooldownMs) {
        Set<TopicPartition> coolingDown = coolingDownPartitions(cooldownMs);
        if (coolingDown.isEmpty()) {
            return 0;
        }

        int excluded = 0;
        for (BrokerUpdater.Broker broker : snapshot.brokers()) {
            if (broker.isSlowBroker()) {
                continue;
            }
            List<TopicPartitionReplicaUpdater.TopicPartitionReplica> replicas =
                new ArrayList<>(snapshot.replicasFor(broker.getBrokerId()));
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica replica : replicas) {
                TopicPartition tp = replica.getTopicPartition();
                if (coolingDown.contains(tp) && snapshot.excludeTopicPartition(broker.getBrokerId(), tp)) {
                    excluded++;
                }
            }
        }
        return excluded;
    }

    void clear() {
        lastReassignmentTimestamps.clear();
    }

    private Set<TopicPartition> coolingDownPartitions(long cooldownMs) {
        if (cooldownMs <= 0) {
            clear();
            return Set.of();
        }
        long now = time.milliseconds();
        lastReassignmentTimestamps.entrySet().removeIf(entry -> now - entry.getValue() >= cooldownMs);
        return new HashSet<>(lastReassignmentTimestamps.keySet());
    }
}
