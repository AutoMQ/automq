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

import com.automq.stream.utils.LogContext;
import java.util.Collections;
import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface Goal extends Configurable, Comparable<Goal> {
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

    void validateReconfiguration(Map<String, ?> configs) throws ConfigException;
}
