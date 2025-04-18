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
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractResourceDistributionGoal extends AbstractResourceGoal {

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    public List<Action> doOptimize(List<BrokerUpdater.Broker> eligibleBrokers, ClusterModelSnapshot cluster,
                                   Collection<Goal> goalsByPriority, Collection<Goal> optimizedGoals,
                                   Map<String, Set<String>> goalsByGroup) {
        List<Action> actions = new ArrayList<>();
        List<BrokerUpdater.Broker> brokersToOptimize = getBrokersToOptimize(eligibleBrokers);
        for (BrokerUpdater.Broker broker : brokersToOptimize) {
            if (isBrokerAcceptable(broker)) {
                continue;
            }
            List<BrokerUpdater.Broker> candidateBrokers = eligibleBrokers.stream()
                    .filter(b -> b.getBrokerId() != broker.getBrokerId() && broker.load(resource()).isTrusted()).collect(Collectors.toList());
            Result result = null;
            if (requireLessLoad(broker)) {
                result = tryReduceLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers,
                        goalsByPriority, optimizedGoals, goalsByGroup);
//                if (!isBrokerAcceptable(broker)) {
//                    brokerActions.addAll(tryReduceLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
//                }
            } else if (requireMoreLoad(broker)) {
                if (broker.isSlowBroker()) {
                    // prevent scheduling more partitions to slow broker
                    continue;
                }
                result = tryIncreaseLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers,
                        goalsByPriority, optimizedGoals, goalsByGroup);
//                if (!isBrokerAcceptable(broker)) {
//                    brokerActions.addAll(tryIncreaseLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
//                }
            }

            if (result != null) {
                if (!isTrivialLoadChange(broker, result.loadChange())) {
                    actions.addAll(result.actions());
                } else {
                    result.actions().forEach(cluster::undoAction);
                }
            }

            if (!isBrokerAcceptable(broker)) {
                // broker still violates goal after iterating all partitions
                onBalanceFailed(broker);
            }
        }
        return actions;
    }

    protected boolean isTrivialLoadChange(BrokerUpdater.Broker broker, double loadChange) {
        return false;
    }

    protected abstract boolean requireLessLoad(BrokerUpdater.Broker broker);

    protected abstract boolean requireMoreLoad(BrokerUpdater.Broker broker);
}
