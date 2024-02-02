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

import kafka.autobalancer.common.Action;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.ModelUtils;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractGoal implements Goal {
    protected static final double POSITIVE_ACTION_SCORE_THRESHOLD = 0.5;

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
     * @return normalized score. 0 means not allowed action
     * > 0 means permitted action, but can be positive or negative for this goal
     */
    protected double calculateAcceptanceScore(BrokerUpdater.Broker srcBrokerBefore, BrokerUpdater.Broker destBrokerBefore, BrokerUpdater.Broker srcBrokerAfter, BrokerUpdater.Broker destBrokerAfter) {
        double score = scoreDelta(srcBrokerBefore, destBrokerBefore, srcBrokerAfter, destBrokerAfter);

        if (type() != GoalType.HARD) {
            return score;
        }

        boolean isSrcBrokerAcceptedBefore = isBrokerAcceptable(srcBrokerBefore);
        boolean isDestBrokerAcceptedBefore = isBrokerAcceptable(destBrokerBefore);
        boolean isSrcBrokerAcceptedAfter = isBrokerAcceptable(srcBrokerAfter);
        boolean isDestBrokerAcceptedAfter = isBrokerAcceptable(destBrokerAfter);

        if (isSrcBrokerAcceptedBefore && !isSrcBrokerAcceptedAfter) {
            return 0.0;
        } else if (isDestBrokerAcceptedBefore && !isDestBrokerAcceptedAfter) {
            return 0.0;
        }

        if (!isSrcBrokerAcceptedBefore && !isSrcBrokerAcceptedAfter) {
            return score <= POSITIVE_ACTION_SCORE_THRESHOLD ? 0.0 : score;
        } else if (!isDestBrokerAcceptedBefore && !isDestBrokerAcceptedAfter) {
            return score <= POSITIVE_ACTION_SCORE_THRESHOLD ? 0.0 : score;
        }
        return score;
    }

    /**
     * Calculate the weighted score of an action based on its scores from all goals.
     *
     * @param scoreMap score map from all goals
     * @return the final score
     */
    protected double normalizeGoalsScore(Map<Goal, Double> scoreMap) {
        int totalWeight = scoreMap.keySet().stream().mapToInt(e -> e.type().priority()).sum();
        return scoreMap.entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * (double) entry.getKey().type().priority() / totalWeight)
                .sum();
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
            return 0.0;
        }
        BrokerUpdater.Broker srcBrokerBefore = cluster.broker(action.getSrcBrokerId());
        BrokerUpdater.Broker destBrokerBefore = cluster.broker(action.getDestBrokerId());
        BrokerUpdater.Broker srcBrokerAfter = srcBrokerBefore.copy();
        BrokerUpdater.Broker destBrokerAfter = destBrokerBefore.copy();
        TopicPartitionReplicaUpdater.TopicPartitionReplica srcReplica = cluster.replica(action.getSrcBrokerId(), action.getSrcTopicPartition());

        switch (action.getType()) {
            case MOVE:
                ModelUtils.moveReplicaLoad(srcBrokerAfter, destBrokerAfter, srcReplica);
                break;
            case SWAP:
                ModelUtils.moveReplicaLoad(srcBrokerAfter, destBrokerAfter, srcReplica);
                ModelUtils.moveReplicaLoad(destBrokerAfter, srcBrokerAfter,
                        cluster.replica(action.getDestBrokerId(), action.getDestTopicPartition()));
                break;
            default:
                return 0.0;
        }

        return calculateAcceptanceScore(srcBrokerBefore, destBrokerBefore, srcBrokerAfter, destBrokerAfter);
    }

    @Override
    public Set<BrokerUpdater.Broker> getEligibleBrokers(ClusterModelSnapshot cluster) {
        return cluster.brokers().stream().filter(BrokerUpdater.Broker::isActive).collect(Collectors.toSet());
    }

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
