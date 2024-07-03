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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * The assignment specification for a modern group.
 */
public class GroupSpecImpl implements GroupSpec {
    /**
     * Member subscription metadata keyed by member Id.
     */
    private final Map<String, MemberSubscriptionAndAssignmentImpl> members;

    /**
     * The subscription type of the group.
     */
    private final SubscriptionType subscriptionType;

    /**
     * Reverse lookup map representing topic partitions with
     * their current member assignments.
     */
    private final Map<Uuid, Map<Integer, String>> invertedMemberAssignment;

    public GroupSpecImpl(
        Map<String, MemberSubscriptionAndAssignmentImpl> members,
        SubscriptionType subscriptionType,
        Map<Uuid, Map<Integer, String>> invertedMemberAssignment
    ) {
        this.members = Objects.requireNonNull(members);
        this.subscriptionType = Objects.requireNonNull(subscriptionType);
        this.invertedMemberAssignment = Objects.requireNonNull(invertedMemberAssignment);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> memberIds() {
        return members.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SubscriptionType subscriptionType() {
        return subscriptionType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPartitionAssigned(Uuid topicId, int partitionId) {
        Map<Integer, String> partitionMap = invertedMemberAssignment.get(topicId);
        if (partitionMap == null) {
            return false;
        }
        return partitionMap.containsKey(partitionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberSubscription memberSubscription(String memberId) {
        MemberSubscription memberSubscription = members.get(memberId);
        if (memberSubscription == null) {
            throw new IllegalArgumentException("Member Id " + memberId + " not found.");
        }
        return memberSubscription;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberAssignment memberAssignment(String memberId) {
        MemberSubscriptionAndAssignmentImpl member = members.get(memberId);
        if (member == null) {
            return new MemberAssignmentImpl(Collections.emptyMap());
        }
        return member;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupSpecImpl that = (GroupSpecImpl) o;
        return subscriptionType == that.subscriptionType &&
            members.equals(that.members) &&
            invertedMemberAssignment.equals(that.invertedMemberAssignment);
    }

    @Override
    public int hashCode() {
        int result = members.hashCode();
        result = 31 * result + subscriptionType.hashCode();
        result = 31 * result + invertedMemberAssignment.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "GroupSpecImpl(members=" + members +
            ", subscriptionType=" + subscriptionType +
            ", invertedMemberAssignment=" + invertedMemberAssignment +
            ')';
    }
}
