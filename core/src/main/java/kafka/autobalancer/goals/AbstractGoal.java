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
import kafka.autobalancer.model.BrokerUpdater.Broker;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.ModelUtils;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater.TopicPartitionReplica;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractGoal implements Goal, Configurable, Comparable<AbstractGoal> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGoal.class);
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
    private double scoreDelta(Broker srcBrokerBefore, Broker destBrokerBefore, Broker srcBrokerAfter, Broker destBrokerAfter) {
        double scoreBefore = Math.min(brokerScore(srcBrokerBefore), brokerScore(destBrokerBefore));
        double scoreAfter = Math.min(brokerScore(srcBrokerAfter), brokerScore(destBrokerAfter));
        return GoalUtils.normalize(scoreAfter - scoreBefore, 1.0, -1.0);
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
    private double calculateAcceptanceScore(Broker srcBrokerBefore, Broker destBrokerBefore, Broker srcBrokerAfter, Broker destBrokerAfter) {
        double score = scoreDelta(srcBrokerBefore, destBrokerBefore, srcBrokerAfter, destBrokerAfter);
        boolean isSrcBrokerAcceptedBefore = isBrokerAcceptable(srcBrokerBefore);
        boolean isDestBrokerAcceptedBefore = isBrokerAcceptable(destBrokerBefore);
        boolean isSrcBrokerAcceptedAfter = isBrokerAcceptable(srcBrokerAfter);
        boolean isDestBrokerAcceptedAfter = isBrokerAcceptable(destBrokerAfter);

        if (!isHardGoal()) {
            return score;
        }

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

    @Override
    public double actionAcceptanceScore(Action action, ClusterModelSnapshot cluster) {
        if (!GoalUtils.isValidAction(action, cluster)) {
            return 0.0;
        }
        Broker srcBrokerBefore = cluster.broker(action.getSrcBrokerId());
        Broker destBrokerBefore = cluster.broker(action.getDestBrokerId());
        Broker srcBrokerAfter = new Broker(srcBrokerBefore);
        Broker destBrokerAfter = new Broker(destBrokerBefore);
        TopicPartitionReplica srcReplica = cluster.replica(action.getSrcBrokerId(), action.getSrcTopicPartition());

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
    public int priority() {
        return GoalUtils.priority(this);
    }

    @Override
    public Set<Broker> getEligibleBrokers(ClusterModelSnapshot cluster) {
        return cluster.brokers().stream().filter(Broker::isActive).collect(Collectors.toSet());
    }

    @Override
    public int compareTo(AbstractGoal other) {
        return Integer.compare(other.priority(), this.priority());
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
