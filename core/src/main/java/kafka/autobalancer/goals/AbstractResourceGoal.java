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

package kafka.autobalancer.goals;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.model.AbstractInstanceUpdater;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.ModelUtils;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractResourceGoal extends AbstractGoal {
    private final PartitionComparator partitionComparator = new PartitionComparator(resource());

    protected abstract byte resource();

    @Override
    public List<BrokerUpdater.Broker> getBrokersToOptimize(Collection<BrokerUpdater.Broker> brokers) {
        List<BrokerUpdater.Broker> brokersToOptimize = new ArrayList<>();
        for (BrokerUpdater.Broker broker : brokers) {
            if (!isBrokerAcceptable(broker)) {
                if (!broker.load(resource()).isTrusted()) {
                    // do not balance broker with untrusted load
                    LOGGER.warn("Broker {} has untrusted {} load, skip optimizing for {}", broker.getBrokerId(),
                            Resource.HUMAN_READABLE_RESOURCE_NAMES.get(resource()), name());
                    continue;
                }
                LOGGER.warn("Broker {} violates goal {}", broker.getBrokerId(), name());
                brokersToOptimize.add(broker);
            }
        }
        return brokersToOptimize;
    }

    @Override
    protected boolean moveReplica(Action action, ClusterModelSnapshot cluster, BrokerUpdater.Broker src, BrokerUpdater.Broker dest) {
        TopicPartitionReplicaUpdater.TopicPartitionReplica srcReplica = cluster.replica(action.getSrcBrokerId(), action.getSrcTopicPartition());
        switch (action.getType()) {
            case MOVE:
                ModelUtils.moveReplicaLoad(src, dest, srcReplica);
                break;
            case SWAP:
                TopicPartitionReplicaUpdater.TopicPartitionReplica destReplica = cluster.replica(action.getDestBrokerId(), action.getDestTopicPartition());
                ModelUtils.moveReplicaLoad(src, dest, srcReplica);
                ModelUtils.moveReplicaLoad(dest, src, destReplica);
                break;
            default:
                return false;
        }
        return true;
    }

    /**
     * Try to reduce resource load by move or swap replicas out.
     *
     * @param actionType       type of action
     * @param cluster          cluster model
     * @param srcBroker        broker to reduce load
     * @param candidateBrokers candidate brokers to move replicas to, or swap replicas with
     * @param goalsByPriority  all configured goals sorted by priority
     * @return a list of actions able to reduce load of srcBroker along with load change
     */
    protected Result tryReduceLoadByAction(ActionType actionType,
                                                 ClusterModelSnapshot cluster,
                                                 BrokerUpdater.Broker srcBroker,
                                                 List<BrokerUpdater.Broker> candidateBrokers,
                                                 Collection<Goal> goalsByPriority,
                                                 Collection<Goal> optimizedGoals,
                                                 Map<String, Set<String>> goalsByGroup) {
        List<Action> actionList = new ArrayList<>();
        candidateBrokers = candidateBrokers.stream().filter(b -> !b.isSlowBroker()).collect(Collectors.toList());
        if (candidateBrokers.isEmpty()) {
            return new Result(actionList, 0.0);
        }
        List<TopicPartitionReplicaUpdater.TopicPartitionReplica> srcReplicas = cluster
                .replicasFor(srcBroker.getBrokerId())
                .stream()
                .sorted(partitionComparator) // higher load first
                .collect(Collectors.toList());
        double loadChange = 0.0;
        for (TopicPartitionReplicaUpdater.TopicPartitionReplica tp : srcReplicas) {
            candidateBrokers.sort(lowLoadComparator()); // lower load first
            Optional<Action> optionalAction;
            ActionParameters parameters = new ActionParameters(cluster, tp, srcBroker, candidateBrokers, goalsByPriority, optimizedGoals, goalsByGroup);
            if (actionType == ActionType.MOVE) {
                optionalAction = tryMovePartitionOut(parameters);
            } else {
                optionalAction = trySwapPartitionOut(parameters, Comparator.comparingDouble(p -> p.loadValue(resource())),
                        (src, candidate) -> src.loadValue(resource()) > candidate.loadValue(resource()));
            }

            if (optionalAction.isPresent()) {
                Action action = optionalAction.get();
                cluster.applyAction(action);
                actionList.add(action);
                loadChange -= tp.loadValue(resource());
                if (action.getType() == ActionType.SWAP) {
                    loadChange += cluster.replica(action.getDestBrokerId(), action.getDestTopicPartition()).loadValue(resource());
                }
            }
            if (isBrokerAcceptable(srcBroker)) {
                // broker is acceptable after action, skip iterating reset partitions
                return new Result(actionList, loadChange);
            }
        }
        return new Result(actionList, loadChange);
    }

    /**
     * Try to increase resource load by move or swap replicas in.
     *
     * @param actionType       type of action
     * @param cluster          cluster model
     * @param srcBroker        broker to increase load
     * @param candidateBrokers candidate brokers to move replicas from, or swap replicas with
     * @param goalsByPriority  all configured goals sorted by priority
     * @return a list of actions able to increase load of srcBroker along with load change
     */
    protected Result tryIncreaseLoadByAction(ActionType actionType,
                                                   ClusterModelSnapshot cluster,
                                                   BrokerUpdater.Broker srcBroker,
                                                   List<BrokerUpdater.Broker> candidateBrokers,
                                                   Collection<Goal> goalsByPriority,
                                                   Collection<Goal> optimizedGoals,
                                                   Map<String, Set<String>> goalsByGroup) {
        List<Action> actionList = new ArrayList<>();
        candidateBrokers.sort(highLoadComparator()); // higher load first
        double loadChange = 0.0;
        for (BrokerUpdater.Broker candidateBroker : candidateBrokers) {
            List<TopicPartitionReplicaUpdater.TopicPartitionReplica> candidateReplicas = cluster
                    .replicasFor(candidateBroker.getBrokerId())
                    .stream()
                    .sorted(partitionComparator) // higher load first
                    .collect(Collectors.toList());
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica tp : candidateReplicas) {
                Optional<Action> optionalAction;
                ActionParameters parameters = new ActionParameters(cluster, tp, candidateBroker, List.of(srcBroker), goalsByPriority, optimizedGoals, goalsByGroup);
                if (actionType == ActionType.MOVE) {
                    optionalAction = tryMovePartitionOut(parameters);
                } else {
                    optionalAction = trySwapPartitionOut(parameters, Comparator.comparingDouble(p -> p.loadValue(resource())),
                            (src, candidate) -> src.loadValue(resource()) > candidate.loadValue(resource()));
                }

                if (optionalAction.isPresent()) {
                    Action action = optionalAction.get();
                    cluster.applyAction(action);
                    actionList.add(action);
                    loadChange += tp.loadValue(resource());
                    if (action.getType() == ActionType.SWAP) {
                        loadChange -= cluster.replica(action.getDestBrokerId(), action.getDestTopicPartition()).loadValue(resource());
                    }
                }
                if (isBrokerAcceptable(srcBroker)) {
                    // broker is acceptable after action, skip iterating reset partitions
                    return new Result(actionList, loadChange);
                }
            }
        }
        return new Result(actionList, loadChange);
    }

    @Override
    public double actionAcceptanceScore(Action action, ClusterModelSnapshot cluster) {
        if (validateAction(action.getSrcBrokerId(), action.getDestBrokerId(), action.getSrcTopicPartition(), cluster)) {
            return super.actionAcceptanceScore(action, cluster);
        }
        return NOT_ACCEPTABLE;
    }

    boolean validateAction(int srcBrokerId, int destBrokerId, TopicPartition tp, ClusterModelSnapshot cluster) {
        BrokerUpdater.Broker destBroker = cluster.broker(destBrokerId);
        BrokerUpdater.Broker srcBroker = cluster.broker(srcBrokerId);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica = cluster.replica(srcBrokerId, tp);
        AbstractInstanceUpdater.Load replicaLoad = replica.load(resource());
        if (!replicaLoad.isTrusted()) {
            return false;
        }
        if (replicaLoad.getValue() == 0) {
            return true;
        }
        return destBroker.load(resource()).isTrusted() && srcBroker.load(resource()).isTrusted();
    }

    protected abstract Comparator<BrokerUpdater.Broker> highLoadComparator();

    protected abstract Comparator<BrokerUpdater.Broker> lowLoadComparator();

    public static class Result {
        private final List<Action> actions;
        private final double loadChange;

        public Result(List<Action> actions, double loadChange) {
            this.actions = actions;
            this.loadChange = loadChange;
        }

        public List<Action> actions() {
            return actions;
        }

        public double loadChange() {
            return loadChange;
        }
    }

    static class PartitionComparator implements Comparator<TopicPartitionReplicaUpdater.TopicPartitionReplica> {
        private final byte resource;

        PartitionComparator(byte resource) {
            this.resource = resource;
        }

        @Override
        public int compare(TopicPartitionReplicaUpdater.TopicPartitionReplica p1, TopicPartitionReplicaUpdater.TopicPartitionReplica p2) {
            boolean isCritical1 = GoalUtils.isCriticalTopic(p1.getTopicPartition().topic());
            boolean isCritical2 = GoalUtils.isCriticalTopic(p2.getTopicPartition().topic());
            if (isCritical1 && !isCritical2) {
                return 1;
            } else if (!isCritical1 && isCritical2) {
                return -1;
            }
            return Double.compare(p2.loadValue(resource), p1.loadValue(resource));
        }
    }
}
