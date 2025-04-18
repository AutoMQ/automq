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
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;

import org.apache.kafka.common.Reconfigurable;

import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface Goal extends Reconfigurable, Comparable<Goal> {
    Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);

    List<Action> doOptimize(List<BrokerUpdater.Broker> eligibleBrokers, ClusterModelSnapshot cluster,
                            Collection<Goal> goalsByPriority, Collection<Goal> optimizedGoals,
                            Map<String, Set<String>> goalsByGroup);

    default List<Action> optimize(ClusterModelSnapshot cluster, Collection<Goal> goalsByPriority,
                                  Collection<Goal> optimizedGoal) {
        Map<String, Set<String>> goalsByGroup = goalsByPriority.stream()
                .collect(Collectors.groupingBy(Goal::group, Collectors.mapping(Goal::name, Collectors.toSet())));
        return optimize(cluster, goalsByPriority, optimizedGoal, goalsByGroup);
    }

    default List<Action> optimize(ClusterModelSnapshot cluster, Collection<Goal> goalsByPriority,
                                  Collection<Goal> optimizedGoal, Map<String, Set<String>> goalsByGroup) {
        for (Goal goal : goalsByPriority) {
            if (!goal.isInitialized()) {
                LOGGER.error("Goal {} is not initialized, maybe invoke initialize() before use.", goal.name());
                return Collections.emptyList();
            }
        }
        return doOptimize(getEligibleBrokers(cluster), cluster, goalsByPriority, optimizedGoal, goalsByGroup);
    }

    default List<BrokerUpdater.Broker> getEligibleBrokers(ClusterModelSnapshot cluster) {
        return cluster.brokers().stream().filter(this::isEligibleBroker).collect(Collectors.toList());
    }

    default boolean isEligibleBroker(BrokerUpdater.Broker broker) {
        return broker.getMetricVersion().isGoalSupported(this) && !broker.isMetricsOutOfDate();
    }

    default void initialize(ClusterModelSnapshot cluster) {
        initialize(getEligibleBrokers(cluster));
    }

    void initialize(Collection<BrokerUpdater.Broker> brokers);

    boolean isInitialized();

    boolean isHardGoal();

    String group();

    double weight();

    List<BrokerUpdater.Broker> getBrokersToOptimize(Collection<BrokerUpdater.Broker> brokers);

    String name();

    /**
     * Get the acceptance score of the goal if the action applied to the given cluster.
     *
     * @param action  action to apply to the cluster
     * @param cluster cluster to apply the action
     * @return action acceptance score, 0 for not accepted
     */
    double actionAcceptanceScore(Action action, ClusterModelSnapshot cluster);

    @Override
    default int compareTo(Goal other) {
        return Boolean.compare(other.isHardGoal(), this.isHardGoal());
    }
}
