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
package org.apache.kafka.coordinator.group.modern.share;

import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;

import java.util.Objects;

/**
 * The ShareGroupAssignmentBuilder class encapsulates the reconciliation engine of the
 * share group protocol. Given the current state of a member and a desired or target
 * assignment state, the state machine takes the necessary steps to converge them.
 */
public class ShareGroupAssignmentBuilder {
    /**
     * The share group member which is reconciled.
     */
    private final ShareGroupMember member;

    /**
     * The target assignment epoch.
     */
    private int targetAssignmentEpoch;

    /**
     * The target assignment.
     */
    private Assignment targetAssignment;

    /**
     * Constructs the ShareGroupAssignmentBuilder based on the current state of the
     * provided share group member.
     *
     * @param member The share group member that must be reconciled.
     */
    public ShareGroupAssignmentBuilder(ShareGroupMember member) {
        this.member = Objects.requireNonNull(member);
    }

    /**
     * Sets the target assignment epoch and the target assignment that the
     * share group member must be reconciled to.
     *
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @return This object.
     */
    public ShareGroupAssignmentBuilder withTargetAssignment(
        int targetAssignmentEpoch,
        Assignment targetAssignment
    ) {
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        this.targetAssignment = Objects.requireNonNull(targetAssignment);
        return this;
    }

    /**
     * Builds the next state for the member or keep the current one if it
     * is not possible to move forward with the current state.
     *
     * @return A new ShareGroupMember or the current one.
     */
    public ShareGroupMember build() {
        // A new target assignment has been installed, we need to restart
        // the reconciliation loop from the beginning.
        if (targetAssignmentEpoch != member.memberEpoch()) {
            // We transition to the target epoch. The transition to the new state is done
            // when the member is updated.
            return new ShareGroupMember.Builder(member)
                .setState(MemberState.STABLE)
                .setAssignedPartitions(targetAssignment.partitions())
                .updateMemberEpoch(targetAssignmentEpoch)
                .build();
        }

        return member;
    }
}
