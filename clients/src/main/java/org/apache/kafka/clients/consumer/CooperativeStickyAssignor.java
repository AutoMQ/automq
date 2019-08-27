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
package org.apache.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.common.TopicPartition;

/**
 * A cooperative version of the {@link AbstractStickyAssignor AbstractStickyAssignor}. This follows the same (sticky)
 * assignment logic as {@link StickyAssignor StickyAssignor} but allows for cooperative rebalancing while the
 * {@link StickyAssignor StickyAssignor} follows the eager rebalancing protocol. See
 * {@link ConsumerPartitionAssignor.RebalanceProtocol} for an explanation of the rebalancing protocols.
 * <p>
 * Users should prefer this assignor for newer clusters.
 * <p>
 * To turn on cooperative rebalancing you must set all your consumers to use this {@code PartitionAssignor},
 * or implement a custom one that returns {@code RebalanceProtocol.COOPERATIVE} in
 * {@link CooperativeStickyAssignor#supportedProtocols supportedProtocols()}.
 * <p>
 * IMPORTANT: if upgrading from 2.3 or earlier, you must follow a specific upgrade path in order to safely turn on
 * cooperative rebalancing. See the <a href="https://kafka.apache.org/documentation/#upgrade_240_notable">upgrade guide</a> for details.
 */
public class CooperativeStickyAssignor extends AbstractStickyAssignor {

    @Override
    public String name() {
        return "cooperative-sticky";
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return Arrays.asList(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER);
    }

    @Override
    protected MemberData memberData(Subscription subscription) {
        return new MemberData(subscription.ownedPartitions(), Optional.empty());
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {

        final Map<String, List<TopicPartition>> assignments = super.assign(partitionsPerTopic, subscriptions);
        adjustAssignment(subscriptions, assignments);
        return assignments;
    }

    // Following the cooperative rebalancing protocol requires removing partitions that must first be revoked from the assignment
    private void adjustAssignment(final Map<String, Subscription> subscriptions,
                                  final Map<String, List<TopicPartition>> assignments) {

        Map<TopicPartition, String> allAddedPartitions = new HashMap<>();
        Set<TopicPartition> allRevokedPartitions = new HashSet<>();

        for (final Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
            String consumer = entry.getKey();

            List<TopicPartition> ownedPartitions = subscriptions.get(consumer).ownedPartitions();
            List<TopicPartition> assignedPartitions = entry.getValue();

            List<TopicPartition> addedPartitions = new ArrayList<>(assignedPartitions);
            addedPartitions.removeAll(ownedPartitions);
            for (TopicPartition tp : addedPartitions) {
                allAddedPartitions.put(tp, consumer);
            }

            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);
            allRevokedPartitions.addAll(revokedPartitions);
        }

        // remove any partitions to be revoked from the current assignment
        for (TopicPartition tp : allRevokedPartitions) {
            // if partition is being migrated to another consumer, don't assign it there yet
            if (allAddedPartitions.containsKey(tp)) {
                String assignedConsumer = allAddedPartitions.get(tp);
                assignments.get(assignedConsumer).remove(tp);
            }
        }
    }

}
