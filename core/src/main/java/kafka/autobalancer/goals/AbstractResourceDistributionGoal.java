/*
 * Copyright 2024, AutoMQ HK Limited.
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
