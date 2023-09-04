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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractResourceDistributionGoal extends AbstractResourceGoal {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceDistributionGoal.class);

    protected double utilizationDetectThreshold;

    protected double utilizationAvgDeviation;
    private double meanResourceUtil;
    private double resourceUtilDistLowerBound;
    private double resourceUtilDistUpperBound;

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    public void validateConfig() {
        this.utilizationDetectThreshold = Math.min(1.0, Math.max(0.0, this.utilizationDetectThreshold));
        this.utilizationAvgDeviation = Math.abs(utilizationAvgDeviation);
    }

    @Override
    public List<Action> optimize(ClusterModelSnapshot cluster, Collection<AbstractGoal> goalsByPriority) {
        List<Action> actions = new ArrayList<>();
        validateConfig();
        Set<BrokerUpdater.Broker> eligibleBrokers = getEligibleBrokers(cluster);
        calcUtilizationBound(eligibleBrokers);
        List<BrokerUpdater.Broker> brokersToOptimize = new ArrayList<>();
        for (BrokerUpdater.Broker broker : eligibleBrokers) {
            if (!isBrokerAcceptable(broker)) {
                LOGGER.warn("Broker {} violates goal {}", broker.getBrokerId(), name());
                brokersToOptimize.add(broker);
            }
        }
        Resource resource = resource();
        for (BrokerUpdater.Broker broker : brokersToOptimize) {
            if (isBrokerAcceptable(broker)) {
                continue;
            }
            List<BrokerUpdater.Broker> candidateBrokers =
                    eligibleBrokers.stream().filter(b -> b.getBrokerId() != broker.getBrokerId()).collect(Collectors.toList());
            double loadUtil = broker.utilizationFor(resource);
            if (requireLessLoad(loadUtil)) {
                List<Action> brokerActions = tryReduceLoadByAction(ActionType.MOVE, cluster, broker, candidateBrokers, goalsByPriority);
                if (!isBrokerAcceptable(broker)) {
                    brokerActions.addAll(tryReduceLoadByAction(ActionType.SWAP, cluster, broker, candidateBrokers, goalsByPriority));
                }
                actions.addAll(brokerActions);
            } else if (requireMoreLoad(loadUtil)) {
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

    private void calcUtilizationBound(Set<BrokerUpdater.Broker> brokers) {
        Resource resource = resource();
        meanResourceUtil = brokers.stream().mapToDouble(e -> e.utilizationFor(resource)).sum() / brokers.size();
        resourceUtilDistLowerBound = Math.max(0, meanResourceUtil * (1 - this.utilizationAvgDeviation));
        resourceUtilDistUpperBound = meanResourceUtil * (1 + this.utilizationAvgDeviation);
    }

    private boolean requireLessLoad(double util) {
        return util > resourceUtilDistUpperBound;
    }

    private boolean requireMoreLoad(double util) {
        return util < resourceUtilDistLowerBound;
    }

    @Override
    public boolean isBrokerAcceptable(BrokerUpdater.Broker broker) {
        double util = broker.utilizationFor(resource());
        if (util < this.utilizationDetectThreshold) {
            return true;
        }
        return !requireLessLoad(util) && !requireMoreLoad(util);
    }

    @Override
    public double brokerScore(BrokerUpdater.Broker broker) {
        double utilMeanDeviationAbs = Math.abs(meanResourceUtil - broker.utilizationFor(resource()));
        return GoalUtils.normalize(utilMeanDeviationAbs, 1, 0, true);
    }
}
