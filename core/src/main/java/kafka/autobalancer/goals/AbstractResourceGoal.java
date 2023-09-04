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

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.common.Resource;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractResourceGoal extends AbstractGoal {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGoal.class);

    abstract Resource resource();

    private Optional<Action> getAcceptableAction(List<Map.Entry<Action, Double>> candidateActionScores) {
        Action acceptableAction = null;
        Optional<Map.Entry<Action, Double>> optionalEntry = candidateActionScores.stream()
                .max(Comparator.comparingDouble(Map.Entry::getValue));
        if (optionalEntry.isPresent() && optionalEntry.get().getValue() > POSITIVE_ACTION_SCORE_THRESHOLD) {
            acceptableAction = optionalEntry.get().getKey();
        }
        return Optional.ofNullable(acceptableAction);
    }

    private double normalizeGoalsScore(Map<AbstractGoal, Double> scoreMap) {
        int totalWeight = scoreMap.keySet().stream().mapToInt(AbstractGoal::priority).sum();
        return scoreMap.entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * (double) entry.getKey().priority() / totalWeight)
                .sum();
    }

    private Optional<Action> trySwapPartitionOut(ClusterModelSnapshot cluster,
                                                 TopicPartitionReplicaUpdater.TopicPartitionReplica srcReplica,
                                                 BrokerUpdater.Broker srcBroker,
                                                 List<BrokerUpdater.Broker> candidates,
                                                 Collection<AbstractGoal> goalsByPriority) {
        List<Map.Entry<Action, Double>> candidateActionScores = new ArrayList<>();
        for (BrokerUpdater.Broker candidate : candidates) {
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica candidateReplica : cluster.replicasFor(candidate.getBrokerId())) {
                if (candidate.load(resource()) > srcReplica.load(resource())) {
                    continue;
                }
                boolean isHardGoalViolated = false;
                Action action = new Action(ActionType.SWAP, srcReplica.getTopicPartition(), srcBroker.getBrokerId(),
                        candidate.getBrokerId(), candidateReplica.getTopicPartition());
                Map<AbstractGoal, Double> scoreMap = new HashMap<>();
                for (AbstractGoal goal : goalsByPriority) {
                    double score = goal.actionAcceptanceScore(action, cluster);
                    if (goal.isHardGoal() && score == 0) {
                        isHardGoalViolated = true;
                        break;
                    }
                    scoreMap.put(goal, score);
                }
                if (!isHardGoalViolated) {
                    candidateActionScores.add(new AbstractMap.SimpleEntry<>(action, normalizeGoalsScore(scoreMap)));
                }
            }
        }
        LOGGER.debug("All possible action score: {}", candidateActionScores);
        return getAcceptableAction(candidateActionScores);
    }

    private Optional<Action> tryMovePartitionOut(ClusterModelSnapshot cluster,
                                                 TopicPartitionReplicaUpdater.TopicPartitionReplica replica,
                                                 BrokerUpdater.Broker srcBroker,
                                                 List<BrokerUpdater.Broker> candidates,
                                                 Collection<AbstractGoal> goalsByPriority) {
        List<Map.Entry<Action, Double>> candidateActionScores = new ArrayList<>();
        for (BrokerUpdater.Broker candidate : candidates) {
            boolean isHardGoalViolated = false;
            Action action = new Action(ActionType.MOVE, replica.getTopicPartition(), srcBroker.getBrokerId(), candidate.getBrokerId());
            Map<AbstractGoal, Double> scoreMap = new HashMap<>();
            for (AbstractGoal goal : goalsByPriority) {
                double score = goal.actionAcceptanceScore(action, cluster);
                if (goal.isHardGoal() && score == 0) {
                    isHardGoalViolated = true;
                    break;
                }
                scoreMap.put(goal, score);
            }
            if (isHardGoalViolated) {
                break;
            }
            candidateActionScores.add(new AbstractMap.SimpleEntry<>(action, normalizeGoalsScore(scoreMap)));
        }
        LOGGER.debug("All possible action score: {} for {}", candidateActionScores, name());
        return getAcceptableAction(candidateActionScores);
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
                                                 Collection<AbstractGoal> goalsByPriority) {
        List<Action> actionList = new ArrayList<>();
        List<TopicPartitionReplicaUpdater.TopicPartitionReplica> srcReplicas = cluster
                .replicasFor(srcBroker.getBrokerId())
                .stream()
                .sorted(Comparator.comparingDouble(r -> -r.load(resource()))) // higher load first
                .collect(Collectors.toList());
        for (TopicPartitionReplicaUpdater.TopicPartitionReplica tp : srcReplicas) {
            candidateBrokers.sort(Comparator.comparingDouble(b -> b.utilizationFor(resource()))); // lower load first
            Optional<Action> optionalAction;
            if (actionType == ActionType.MOVE) {
                optionalAction = tryMovePartitionOut(cluster, tp, srcBroker, candidateBrokers, goalsByPriority);
            } else {
                optionalAction = trySwapPartitionOut(cluster, tp, srcBroker, candidateBrokers, goalsByPriority);
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
                                                   Collection<AbstractGoal> goalsByPriority) {
        List<Action> actionList = new ArrayList<>();
        candidateBrokers.sort(Comparator.comparingDouble(b -> -b.utilizationFor(resource()))); // higher load first
        for (BrokerUpdater.Broker candidateBroker : candidateBrokers) {
            List<TopicPartitionReplicaUpdater.TopicPartitionReplica> candidateReplicas = cluster
                    .replicasFor(candidateBroker.getBrokerId())
                    .stream()
                    .sorted(Comparator.comparingDouble(r -> -r.load(resource()))) // higher load first
                    .collect(Collectors.toList());
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica tp : candidateReplicas) {
                Optional<Action> optionalAction;
                if (actionType == ActionType.MOVE) {
                    optionalAction = tryMovePartitionOut(cluster, tp, candidateBroker, List.of(srcBroker), goalsByPriority);
                } else {
                    optionalAction = trySwapPartitionOut(cluster, tp, candidateBroker, List.of(srcBroker), goalsByPriority);
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
}
