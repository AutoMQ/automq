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
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import org.apache.kafka.common.Configurable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface Goal extends Configurable, Comparable<Goal> {

    List<Action> doOptimize(Set<BrokerUpdater.Broker> eligibleBrokers, ClusterModelSnapshot cluster, Collection<Goal> goalsByPriority);

    default List<Action> optimize(ClusterModelSnapshot cluster, Collection<Goal> goalsByPriority) {
        Set<BrokerUpdater.Broker> eligibleBrokers = getEligibleBrokers(cluster);
        goalsByPriority.forEach(e -> e.initialize(eligibleBrokers));
        return doOptimize(eligibleBrokers, cluster, goalsByPriority);
    }

    void initialize(Set<BrokerUpdater.Broker> brokers);

    void onBalanceFailed(BrokerUpdater.Broker broker);

    boolean isBrokerAcceptable(BrokerUpdater.Broker broker);

    GoalType type();

    Set<BrokerUpdater.Broker> getEligibleBrokers(ClusterModelSnapshot cluster);

    String name();

    double brokerScore(BrokerUpdater.Broker broker);

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
        return Integer.compare(other.type().priority(), this.type().priority());
    }
}
