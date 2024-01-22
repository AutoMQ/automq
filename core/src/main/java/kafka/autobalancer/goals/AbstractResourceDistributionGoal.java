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
import kafka.autobalancer.model.Broker;
import kafka.autobalancer.model.ClusterModelSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractResourceDistributionGoal extends AbstractResourceGoal {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceDistributionGoal.class);

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    public List<Action> optimize(ClusterModelSnapshot cluster, Collection<Goal> goalsByPriority) {
        List<Action> actions = new ArrayList<>();
        Set<Broker> eligibleBrokers = getEligibleBrokers(cluster);
        calcUsageBound(eligibleBrokers);
        List<Broker> brokersToOptimize = new ArrayList<>();
        for (Broker broker : eligibleBrokers) {
            if (!isBrokerAcceptable(broker)) {
                LOGGER.warn("Broker {} violates goal {}", broker.getBrokerId(), name());
                brokersToOptimize.add(broker);
            }
        }
        for (Broker broker : brokersToOptimize) {
            if (isBrokerAcceptable(broker)) {
                continue;
            }
            List<Broker> candidateBrokers =
                    eligibleBrokers.stream().filter(b -> b.getBrokerId() != broker.getBrokerId()).collect(Collectors.toList());
            double load = broker.load(resource());
            if (requireLessLoad(load)) {
                List<Action> brokerActions = tryReduceLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers, goalsByPriority);
                if (!isBrokerAcceptable(broker)) {
                    brokerActions.addAll(tryReduceLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
                }
                actions.addAll(brokerActions);
            } else if (requireMoreLoad(load)) {
                List<Action> brokerActions = tryIncreaseLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers, goalsByPriority);
                if (isBrokerAcceptable(broker)) {
                    brokerActions.addAll(tryIncreaseLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
                }
                actions.addAll(brokerActions);
            }

            if (!isBrokerAcceptable(broker)) {
                // broker still violates goal after iterating all partitions
                onBalanceFailed(broker);
            }
        }
        return actions;
    }

    abstract void calcUsageBound(Set<Broker> brokers);

    abstract boolean requireLessLoad(double load);

    abstract boolean requireMoreLoad(double load);
}
