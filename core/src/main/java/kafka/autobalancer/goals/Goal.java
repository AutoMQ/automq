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
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface Goal extends Configurable, Comparable<Goal> {

    List<Action> doOptimize(List<BrokerUpdater.Broker> brokersToOptimize, ClusterModelSnapshot cluster,
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
        goalsByPriority.forEach(e -> e.initialize(cluster.brokers()));
        return doOptimize(getBrokersToOptimize(cluster), cluster, goalsByPriority, optimizedGoal, goalsByGroup);
    }

    void initialize(Collection<BrokerUpdater.Broker> brokers);

    boolean isHardGoal();

    String group();

    double weight();

    List<BrokerUpdater.Broker> getBrokersToOptimize(ClusterModelSnapshot cluster);

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
