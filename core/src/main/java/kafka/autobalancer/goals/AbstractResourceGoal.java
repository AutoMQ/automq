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

package kafka.autobalancer.goals;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.Resource;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.ModelUtils;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractResourceGoal extends AbstractGoal {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);

    protected abstract Resource resource();

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
     * @return a list of actions able to reduce load of srcBroker
     */
    protected List<Action> tryReduceLoadByAction(ActionType actionType,
                                                 ClusterModelSnapshot cluster,
                                                 BrokerUpdater.Broker srcBroker,
                                                 List<BrokerUpdater.Broker> candidateBrokers,
                                                 Collection<Goal> goalsByPriority,
                                                 Collection<Goal> optimizedGoals,
                                                 Map<String, Set<String>> goalsByGroup) {
        List<Action> actionList = new ArrayList<>();
        candidateBrokers = candidateBrokers.stream().filter(b -> !b.isSlowBroker()).collect(Collectors.toList());
        if (candidateBrokers.isEmpty()) {
            return actionList;
        }
        List<TopicPartitionReplicaUpdater.TopicPartitionReplica> srcReplicas = cluster
                .replicasFor(srcBroker.getBrokerId())
                .stream()
                .sorted(Comparator.comparingDouble(r -> -r.load(resource()))) // higher load first
                .collect(Collectors.toList());
        for (TopicPartitionReplicaUpdater.TopicPartitionReplica tp : srcReplicas) {
            candidateBrokers.sort(lowLoadComparator()); // lower load first
            Optional<Action> optionalAction;
            if (actionType == ActionType.MOVE) {
                optionalAction = tryMovePartitionOut(cluster, tp, srcBroker, candidateBrokers, goalsByPriority,
                        optimizedGoals, goalsByGroup);
            } else {
                optionalAction = trySwapPartitionOut(cluster, tp, srcBroker, candidateBrokers, goalsByPriority,
                        optimizedGoals, goalsByGroup, Comparator.comparingDouble(p -> p.load(resource())),
                        (src, candidate) -> src.load(resource()) > candidate.load(resource()));
            }

            if (optionalAction.isPresent()) {
                Action action = optionalAction.get();
                cluster.applyAction(action);
                actionList.add(action);
            }
            if (isBrokerAcceptable(srcBroker)) {
                // broker is acceptable after action, skip iterating reset partitions
                return actionList;
            }
        }
        return actionList;
    }

    /**
     * Try to increase resource load by move or swap replicas in.
     *
     * @param actionType       type of action
     * @param cluster          cluster model
     * @param srcBroker        broker to increase load
     * @param candidateBrokers candidate brokers to move replicas from, or swap replicas with
     * @param goalsByPriority  all configured goals sorted by priority
     * @return a list of actions able to increase load of srcBroker
     */
    protected List<Action> tryIncreaseLoadByAction(ActionType actionType,
                                                   ClusterModelSnapshot cluster,
                                                   BrokerUpdater.Broker srcBroker,
                                                   List<BrokerUpdater.Broker> candidateBrokers,
                                                   Collection<Goal> goalsByPriority,
                                                   Collection<Goal> optimizedGoals,
                                                   Map<String, Set<String>> goalsByGroup) {
        List<Action> actionList = new ArrayList<>();
        candidateBrokers.sort(highLoadComparator()); // higher load first
        for (BrokerUpdater.Broker candidateBroker : candidateBrokers) {
            List<TopicPartitionReplicaUpdater.TopicPartitionReplica> candidateReplicas = cluster
                    .replicasFor(candidateBroker.getBrokerId())
                    .stream()
                    .sorted(Comparator.comparingDouble(r -> -r.load(resource()))) // higher load first
                    .collect(Collectors.toList());
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica tp : candidateReplicas) {
                Optional<Action> optionalAction;
                if (actionType == ActionType.MOVE) {
                    optionalAction = tryMovePartitionOut(cluster, tp, candidateBroker, List.of(srcBroker),
                            goalsByPriority, optimizedGoals, goalsByGroup);
                } else {
                    optionalAction = trySwapPartitionOut(cluster, tp, candidateBroker, List.of(srcBroker), goalsByPriority,
                            optimizedGoals, goalsByGroup, Comparator.comparingDouble(p -> p.load(resource())),
                            (src, candidate) -> src.load(resource()) > candidate.load(resource()));
                }

                if (optionalAction.isPresent()) {
                    Action action = optionalAction.get();
                    cluster.applyAction(action);
                    actionList.add(action);
                }
                if (isBrokerAcceptable(srcBroker)) {
                    // broker is acceptable after action, skip iterating reset partitions
                    return actionList;
                }
            }
        }
        return actionList;
    }

    protected abstract Comparator<BrokerUpdater.Broker> highLoadComparator();

    protected abstract Comparator<BrokerUpdater.Broker> lowLoadComparator();
}
