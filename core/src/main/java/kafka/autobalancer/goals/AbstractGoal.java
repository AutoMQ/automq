/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.goals;

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public abstract class AbstractGoal implements Goal {
    public static final Double NOT_ACCEPTABLE = -1.0;
    public static final double POSITIVE_ACTION_SCORE_THRESHOLD = 0.5;
    protected boolean initialized = false;

    @Override
    public void initialize(Collection<BrokerUpdater.Broker> brokers) {
        initialized = true;
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    protected Optional<Action> tryMovePartitionOut(ActionParameters parameters) {
        List<Map.Entry<Action, Double>> candidateActionScores = new ArrayList<>();
        for (BrokerUpdater.Broker candidate : parameters.candidates) {
            Action action = new Action(ActionType.MOVE, parameters.replica.getTopicPartition(), parameters.srcBroker.getBrokerId(), candidate.getBrokerId());
            double score = calculateCandidateActionScores(parameters.goalsByPriority, action, parameters.cluster, parameters.optimizedGoals, parameters.goalsByGroup);
            if (score > POSITIVE_ACTION_SCORE_THRESHOLD) {
                candidateActionScores.add(Map.entry(action, score));
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("try move partition {} out for broker {}, all possible action score: {} on goal {}", parameters    .replica.getTopicPartition(),
                parameters.srcBroker.getBrokerId(), candidateActionScores, name());
        }
        return getAcceptableAction(candidateActionScores);
    }

    protected Optional<Action> trySwapPartitionOut(ActionParameters parameters,
                                                   Comparator<TopicPartitionReplicaUpdater.TopicPartitionReplica> replicaComparator,
                                                   BiPredicate<TopicPartitionReplicaUpdater.TopicPartitionReplica,
                                                           TopicPartitionReplicaUpdater.TopicPartitionReplica> replicaBiPredicate) {
        for (BrokerUpdater.Broker candidate : parameters.candidates) {
            List<TopicPartitionReplicaUpdater.TopicPartitionReplica> candidateReplicas = parameters.cluster
                    .replicasFor(candidate.getBrokerId())
                    .stream()
                    .sorted(replicaComparator)
                    .collect(Collectors.toList());
            for (TopicPartitionReplicaUpdater.TopicPartitionReplica candidateReplica : candidateReplicas) {
                if (!replicaBiPredicate.test(parameters.replica, candidateReplica)) {
                    break;
                }
                Action action = new Action(ActionType.SWAP, parameters.replica.getTopicPartition(), parameters.srcBroker.getBrokerId(),
                        candidate.getBrokerId(), candidateReplica.getTopicPartition());
                double score = calculateCandidateActionScores(parameters.goalsByPriority, action, parameters.cluster, parameters.optimizedGoals, parameters.goalsByGroup);
                if (score > POSITIVE_ACTION_SCORE_THRESHOLD) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("try swap partition {} out for broker {} with {}, action score: {}", parameters.replica.getTopicPartition(),
                            parameters.srcBroker.getBrokerId(), candidateReplica.getTopicPartition(), score);
                    }
                    return Optional.of(action);
                }
            }
        }
        return Optional.empty();
    }

    protected double calculateCandidateActionScores(Collection<Goal> goalsByPriority, Action action,
                                                  ClusterModelSnapshot cluster, Collection<Goal> optimizedGoals,
                                                  Map<String, Set<String>> goalsByGroup) {
        Map<String, Map<Goal, Double>> goalScoreMapByGroup = new HashMap<>();
        for (Goal goal : goalsByPriority) {
            double score = goal.actionAcceptanceScore(action, cluster);
            if (score == NOT_ACCEPTABLE) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("action {} is not acceptable for goal {}", action, goal);
                }
                return NOT_ACCEPTABLE;
            }
            goalScoreMapByGroup.compute(goal.group(), (k, v) -> v == null ? new HashMap<>() : v).put(goal, score);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("action {} scores on each goal: {}", action, goalScoreMapByGroup);
        }
        Map<String, Double> groupScoreMap = weightedGoalsScoreByGroup(goalScoreMapByGroup);
        for (Map.Entry<String, Double> entry : groupScoreMap.entrySet()) {
            String group = entry.getKey();
            if (entry.getValue() < POSITIVE_ACTION_SCORE_THRESHOLD) {
                Set<String> goalMembers = goalsByGroup.get(group);
                if (goalMembers != null) {
                    for (Goal goal : optimizedGoals) {
                        if (goalMembers.contains(goal.name())) {
                            // action that makes the optimized goal group worse is not acceptable
                            return NOT_ACCEPTABLE;
                        }
                    }
                }
            }
        }

        return groupScoreMap.get(group());
    }

    /**
     * Calculate the score difference of src and dest. The score should be normalized to [0, 1.0]
     *
     * @param srcBrokerBefore  source broker before action
     * @param destBrokerBefore dest broker before action
     * @param srcBrokerAfter   source broker after action
     * @param destBrokerAfter  dest broker after action
     * @return normalized score. < 0.5 means negative action
     * == 0.5 means action with no affection
     * > 0.5 means positive action
     */
    private double scoreDelta(BrokerUpdater.Broker srcBrokerBefore, BrokerUpdater.Broker destBrokerBefore, BrokerUpdater.Broker srcBrokerAfter, BrokerUpdater.Broker destBrokerAfter) {
        double scoreBefore = Math.min(brokerScore(srcBrokerBefore), brokerScore(destBrokerBefore));
        double scoreAfter = Math.min(brokerScore(srcBrokerAfter), brokerScore(destBrokerAfter));
        return GoalUtils.linearNormalization(scoreAfter - scoreBefore, 1.0, -1.0);
    }

    /**
     * Calculate acceptance score based on status change of src and dest brokers.
     *
     * @param srcBrokerBefore  source broker before action
     * @param destBrokerBefore dest broker before action
     * @param srcBrokerAfter   source broker after action
     * @param destBrokerAfter  dest broker after action
     * @return normalized score. -1.0 means not allowed action
     * > 0 means permitted action, but can be positive or negative for this goal
     */
    protected double calculateAcceptanceScore(BrokerUpdater.Broker srcBrokerBefore, BrokerUpdater.Broker destBrokerBefore, BrokerUpdater.Broker srcBrokerAfter, BrokerUpdater.Broker destBrokerAfter) {
        double score = scoreDelta(srcBrokerBefore, destBrokerBefore, srcBrokerAfter, destBrokerAfter);

        if (!isHardGoal()) {
            return score;
        }

        boolean isSrcBrokerAcceptedBefore = isBrokerAcceptable(srcBrokerBefore);
        boolean isDestBrokerAcceptedBefore = isBrokerAcceptable(destBrokerBefore);
        boolean isSrcBrokerAcceptedAfter = isBrokerAcceptable(srcBrokerAfter);
        boolean isDestBrokerAcceptedAfter = isBrokerAcceptable(destBrokerAfter);

        if (isSrcBrokerAcceptedBefore && !isSrcBrokerAcceptedAfter) {
            return NOT_ACCEPTABLE;
        } else if (isDestBrokerAcceptedBefore && !isDestBrokerAcceptedAfter) {
            return NOT_ACCEPTABLE;
        }

        if (!isSrcBrokerAcceptedBefore && !isSrcBrokerAcceptedAfter) {
            return score < POSITIVE_ACTION_SCORE_THRESHOLD ? NOT_ACCEPTABLE : score;
        } else if (!isDestBrokerAcceptedBefore && !isDestBrokerAcceptedAfter) {
            return score < POSITIVE_ACTION_SCORE_THRESHOLD ? NOT_ACCEPTABLE : score;
        }
        return score;
    }

    /**
     * Calculate the weighted score of an action based on its scores from goals from same group.
     *
     * @param scoreMap score map from all goals by group
     * @return the sum of weighted score by group
     */
    protected Map<String, Double> weightedGoalsScoreByGroup(Map<String, Map<Goal, Double>> scoreMap) {
        Map<String, Double> groupScoreMap = new HashMap<>();
        for (Map.Entry<String, Map<Goal, Double>> entry : scoreMap.entrySet()) {
            String group = entry.getKey();
            Map<Goal, Double> goalScoreMap = entry.getValue();
            double totalWeight = goalScoreMap.keySet().stream().mapToDouble(Goal::weight).sum();
            double weightedScore = goalScoreMap.entrySet().stream()
                    .mapToDouble(e -> e.getValue() * e.getKey().weight() / totalWeight)
                    .sum();
            groupScoreMap.put(group, weightedScore);
        }
        return groupScoreMap;
    }

    /**
     * Get the acceptable action with the highest score.
     *
     * @param candidateActionScores candidate actions with scores
     * @return the acceptable action with the highest score
     */
    protected Optional<Action> getAcceptableAction(List<Map.Entry<Action, Double>> candidateActionScores) {
        Action acceptableAction = null;
        Optional<Map.Entry<Action, Double>> optionalEntry = candidateActionScores.stream()
                .max(Comparator.comparingDouble(Map.Entry::getValue));
        if (optionalEntry.isPresent() && optionalEntry.get().getValue() > POSITIVE_ACTION_SCORE_THRESHOLD) {
            acceptableAction = optionalEntry.get().getKey();
        }
        return Optional.ofNullable(acceptableAction);
    }

    @Override
    public double actionAcceptanceScore(Action action, ClusterModelSnapshot cluster) {
        if (!GoalUtils.isValidAction(action, cluster)) {
            return NOT_ACCEPTABLE;
        }
        BrokerUpdater.Broker srcBrokerBefore = cluster.broker(action.getSrcBrokerId());
        BrokerUpdater.Broker destBrokerBefore = cluster.broker(action.getDestBrokerId());

        if (!(isEligibleBroker(srcBrokerBefore) && isEligibleBroker(destBrokerBefore))) {
            return POSITIVE_ACTION_SCORE_THRESHOLD;
        }

        BrokerUpdater.Broker srcBrokerAfter = srcBrokerBefore.copy();
        BrokerUpdater.Broker destBrokerAfter = destBrokerBefore.copy();

        if (!moveReplica(action, cluster, srcBrokerAfter, destBrokerAfter)) {
            return NOT_ACCEPTABLE;
        }

        return calculateAcceptanceScore(srcBrokerBefore, destBrokerBefore, srcBrokerAfter, destBrokerAfter);
    }

    @Override
    public List<BrokerUpdater.Broker> getBrokersToOptimize(Collection<BrokerUpdater.Broker> brokers) {
        List<BrokerUpdater.Broker> brokersToOptimize = new ArrayList<>();
        for (BrokerUpdater.Broker broker : brokers) {
            if (!isBrokerAcceptable(broker)) {
                LOGGER.warn("Broker {} violates goal {}", broker.getBrokerId(), name());
                brokersToOptimize.add(broker);
            }
        }
        return brokersToOptimize;
    }

    protected abstract boolean moveReplica(Action action, ClusterModelSnapshot cluster, BrokerUpdater.Broker src, BrokerUpdater.Broker dest);
    protected abstract boolean isBrokerAcceptable(BrokerUpdater.Broker broker);
    protected abstract double brokerScore(BrokerUpdater.Broker broker);
    protected abstract void onBalanceFailed(BrokerUpdater.Broker broker);

    @Override
    public int hashCode() {
        return Objects.hashCode(name());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractGoal goal = (AbstractGoal) o;
        return name().equals(goal.name());
    }

    @Override
    public String toString() {
        return name();
    }
}
