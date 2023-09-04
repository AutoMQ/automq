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
import kafka.autobalancer.model.BrokerUpdater.Broker;
import kafka.autobalancer.model.ClusterModelSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class AbstractResourceCapacityGoal extends AbstractResourceGoal {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceCapacityGoal.class);
    protected double utilizationThreshold;

    @Override
    public boolean isHardGoal() {
        return true;
    }

    @Override
    public List<Action> optimize(ClusterModelSnapshot cluster, Collection<AbstractGoal> goalsByPriority) {
        List<Action> actions = new ArrayList<>();
        validateConfig();
        Set<Broker> eligibleBrokers = getEligibleBrokers(cluster);
        List<Broker> brokersToOptimize = new ArrayList<>();
        for (Broker broker : eligibleBrokers) {
            if (!isBrokerAcceptable(broker)) {
                LOGGER.warn("Broker {} violates goal {}", broker.getBrokerId(), name());
                brokersToOptimize.add(broker);
            }
        }
        brokersToOptimize.forEach(eligibleBrokers::remove);
        List<Broker> candidateBrokers = new ArrayList<>(eligibleBrokers);
        for (Broker broker : brokersToOptimize) {
            if (isBrokerAcceptable(broker)) {
                continue;
            }
            List<Action> brokerActions = tryReduceLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers, goalsByPriority);
            if (!isBrokerAcceptable(broker)) {
                brokerActions.addAll(tryReduceLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
            }
            actions.addAll(brokerActions);
            if (!isBrokerAcceptable(broker)) {
                // broker still violates goal after iterating all partitions
                onBalanceFailed(broker);
            }
        }
        return actions;
    }

    @Override
    public boolean isBrokerAcceptable(Broker broker) {
        return broker.utilizationFor(resource()) <= this.utilizationThreshold;
    }

    @Override
    public void validateConfig() {
        this.utilizationThreshold = Math.min(1.0, Math.max(0.0, this.utilizationThreshold));
    }

    @Override
    public double brokerScore(Broker broker) {
        // use spare utilization as score
        double spare = this.utilizationThreshold - (broker.utilizationFor(resource()));
        // normalize
        return GoalUtils.normalize(spare, this.utilizationThreshold, this.utilizationThreshold - 1);
    }
}
