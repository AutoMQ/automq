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
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractResourceDistributionGoal extends AbstractResourceGoal {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    public List<Action> doOptimize(Set<BrokerUpdater.Broker> eligibleBrokers, ClusterModelSnapshot cluster,
                                   Collection<Goal> goalsByPriority, Collection<Goal> optimizedGoals,
                                   Map<String, Set<String>> goalsByGroup) {
        List<Action> actions = new ArrayList<>();
        List<BrokerUpdater.Broker> brokersToOptimize = new ArrayList<>();
        for (BrokerUpdater.Broker broker : eligibleBrokers) {
            if (!isBrokerAcceptable(broker)) {
                LOGGER.warn("BrokerUpdater.Broker {} violates goal {}", broker.getBrokerId(), name());
                brokersToOptimize.add(broker);
            }
        }
        for (BrokerUpdater.Broker broker : brokersToOptimize) {
            if (isBrokerAcceptable(broker)) {
                continue;
            }
            List<BrokerUpdater.Broker> candidateBrokers =
                    eligibleBrokers.stream().filter(b -> b.getBrokerId() != broker.getBrokerId()).collect(Collectors.toList());
            if (requireLessLoad(broker)) {
                List<Action> brokerActions = tryReduceLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers,
                        goalsByPriority, optimizedGoals, goalsByGroup);
//                if (!isBrokerAcceptable(broker)) {
//                    brokerActions.addAll(tryReduceLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
//                }
                actions.addAll(brokerActions);
            } else if (requireMoreLoad(broker)) {
                if (broker.isSlowBroker()) {
                    // prevent scheduling more partitions to slow broker
                    continue;
                }
                List<Action> brokerActions = tryIncreaseLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers,
                        goalsByPriority, optimizedGoals, goalsByGroup);
//                if (!isBrokerAcceptable(broker)) {
//                    brokerActions.addAll(tryIncreaseLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
//                }
                actions.addAll(brokerActions);
            }

            if (!isBrokerAcceptable(broker)) {
                // broker still violates goal after iterating all partitions
                onBalanceFailed(broker);
            }
        }
        return actions;
    }

    protected abstract boolean requireLessLoad(BrokerUpdater.Broker broker);

    protected abstract boolean requireMoreLoad(BrokerUpdater.Broker broker);
}
