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
import kafka.autobalancer.model.Broker;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplica;
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

    protected abstract Resource resource();

    private Optional<Action> getAcceptableAction(List<Map.Entry<Action, Double>> candidateActionScores) {
        Action acceptableAction = null;
        Optional<Map.Entry<Action, Double>> optionalEntry = candidateActionScores.stream()
                .max(Comparator.comparingDouble(Map.Entry::getValue));
        if (optionalEntry.isPresent() && optionalEntry.get().getValue() > POSITIVE_ACTION_SCORE_THRESHOLD) {
            acceptableAction = optionalEntry.get().getKey();
        }
        return Optional.ofNullable(acceptableAction);
    }

    private double normalizeGoalsScore(Map<Goal, Double> scoreMap) {
        int totalWeight = scoreMap.keySet().stream().mapToInt(Goal::priority).sum();
        return scoreMap.entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * (double) entry.getKey().priority() / totalWeight)
                .sum();
    }

    private Optional<Action> trySwapPartitionOut(ClusterModelSnapshot cluster,
                                                 TopicPartitionReplica srcReplica,
                                                 Broker srcBroker,
                                                 List<Broker> candidates,
                                                 Collection<Goal> goalsByPriority) {
        List<Map.Entry<Action, Double>> candidateActionScores = new ArrayList<>();
        for (Broker candidate : candidates) {
            for (TopicPartitionReplica candidateReplica : cluster.replicasFor(candidate.getBrokerId())) {
                if (candidate.load(resource()) > srcReplica.load(resource())) {
                    continue;
                }
                boolean isHardGoalViolated = false;
                Action action = new Action(ActionType.SWAP, srcReplica.getTopicPartition(), srcBroker.getBrokerId(),
                        candidate.getBrokerId(), candidateReplica.getTopicPartition());
                Map<Goal, Double> scoreMap = new HashMap<>();
                for (Goal goal : goalsByPriority) {
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
                                                 TopicPartitionReplica replica,
                                                 Broker srcBroker,
                                                 List<Broker> candidates,
                                                 Collection<Goal> goalsByPriority) {
        List<Map.Entry<Action, Double>> candidateActionScores = new ArrayList<>();
        for (Broker candidate : candidates) {
            boolean isHardGoalViolated = false;
            Action action = new Action(ActionType.MOVE, replica.getTopicPartition(), srcBroker.getBrokerId(), candidate.getBrokerId());
            Map<Goal, Double> scoreMap = new HashMap<>();
            for (Goal goal : goalsByPriority) {
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
                                                 Broker srcBroker,
                                                 List<Broker> candidateBrokers,
                                                 Collection<Goal> goalsByPriority) {
        List<Action> actionList = new ArrayList<>();
        List<TopicPartitionReplica> srcReplicas = cluster
                .replicasFor(srcBroker.getBrokerId())
                .stream()
                .sorted(Comparator.comparingDouble(r -> -r.load(resource()))) // higher load first
                .collect(Collectors.toList());
        for (TopicPartitionReplica tp : srcReplicas) {
            candidateBrokers.sort(lowLoadComparator()); // lower load first
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
                                                   Broker srcBroker,
                                                   List<Broker> candidateBrokers,
                                                   Collection<Goal> goalsByPriority) {
        List<Action> actionList = new ArrayList<>();
        candidateBrokers.sort(highLoadComparator()); // higher load first
        for (Broker candidateBroker : candidateBrokers) {
            List<TopicPartitionReplica> candidateReplicas = cluster
                    .replicasFor(candidateBroker.getBrokerId())
                    .stream()
                    .sorted(Comparator.comparingDouble(r -> -r.load(resource()))) // higher load first
                    .collect(Collectors.toList());
            for (TopicPartitionReplica tp : candidateReplicas) {
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

    protected abstract Comparator<Broker> highLoadComparator();

    protected abstract Comparator<Broker> lowLoadComparator();
}
