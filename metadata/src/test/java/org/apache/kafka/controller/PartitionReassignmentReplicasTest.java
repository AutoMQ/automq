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

package org.apache.kafka.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.placement.PartitionAssignment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class PartitionReassignmentReplicasTest {
    @Test
    public void testNoneAddedOrRemoved() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(3, 2, 1)), new PartitionAssignment(Arrays.asList(3, 2, 1)));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(3, 2, 1), replicas.replicas());
    }

    @Test
    public void testAdditions() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(3, 2, 1)), new PartitionAssignment(Arrays.asList(3, 6, 2, 1, 5)));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Arrays.asList(5, 6), replicas.adding());
        assertEquals(Arrays.asList(3, 6, 2, 1, 5), replicas.replicas());
    }

    @Test
    public void testRemovals() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(3, 2, 1, 0)), new PartitionAssignment(Arrays.asList(3, 1)));
        assertEquals(Arrays.asList(0, 2), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(3, 1, 0, 2), replicas.replicas());
    }

    @Test
    public void testAdditionsAndRemovals() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(3, 2, 1, 0)), new PartitionAssignment(Arrays.asList(7, 3, 1, 9)));
        assertEquals(Arrays.asList(0, 2), replicas.removing());
        assertEquals(Arrays.asList(7, 9), replicas.adding());
        assertEquals(Arrays.asList(7, 3, 1, 9, 0, 2), replicas.replicas());
    }

    @Test
    public void testRearrangement() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(3, 2, 1, 0)), new PartitionAssignment(Arrays.asList(0, 1, 3, 2)));
        assertEquals(Collections.emptyList(), replicas.removing());
        assertEquals(Collections.emptyList(), replicas.adding());
        assertEquals(Arrays.asList(0, 1, 3, 2), replicas.replicas());
    }

    @Test
    public void testDoesNotCompleteReassignment() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(0, 1, 2)), new PartitionAssignment(Arrays.asList(3, 4, 5)));
        assertTrue(replicas.isReassignmentInProgress());
        Optional<PartitionReassignmentReplicas.CompletedReassignment> reassignmentOptional =
            replicas.maybeCompleteReassignment(Arrays.asList(0, 1, 2, 3, 4));
        assertFalse(reassignmentOptional.isPresent());
    }

    @Test
    public void testDoesNotCompleteReassignmentIfNoneOngoing() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            Collections.emptyList(),
            Collections.emptyList(),
            Arrays.asList(0, 1, 2)
        );
        assertFalse(replicas.isReassignmentInProgress());

        Optional<PartitionReassignmentReplicas.CompletedReassignment> reassignmentOptional =
            replicas.maybeCompleteReassignment(Arrays.asList(0, 1, 2));
        assertFalse(reassignmentOptional.isPresent());
    }

    @Test
    public void testDoesCompleteReassignmentAllNewReplicas() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(0, 1, 2)), new PartitionAssignment(Arrays.asList(3, 4, 5)));
        assertTrue(replicas.isReassignmentInProgress());
        Optional<PartitionReassignmentReplicas.CompletedReassignment> reassignmentOptional =
            replicas.maybeCompleteReassignment(Arrays.asList(0, 1, 2, 3, 4, 5));
        assertTrue(reassignmentOptional.isPresent());
        PartitionReassignmentReplicas.CompletedReassignment completedReassignment = reassignmentOptional.get();
        assertEquals(Arrays.asList(3, 4, 5), completedReassignment.isr);
        assertEquals(Arrays.asList(3, 4, 5), completedReassignment.replicas);
    }

    @Test
    public void testDoesCompleteReassignmentSomeNewReplicas() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(0, 1, 2)), new PartitionAssignment(Arrays.asList(0, 1, 3)));
        assertTrue(replicas.isReassignmentInProgress());
        Optional<PartitionReassignmentReplicas.CompletedReassignment> reassignmentOptional =
            replicas.maybeCompleteReassignment(Arrays.asList(0, 1, 2, 3));
        assertTrue(reassignmentOptional.isPresent());
        PartitionReassignmentReplicas.CompletedReassignment completedReassignment = reassignmentOptional.get();
        assertEquals(Arrays.asList(0, 1, 3), completedReassignment.isr);
        assertEquals(Arrays.asList(0, 1, 3), completedReassignment.replicas);
    }

    @Test
    public void testIsReassignmentInProgress() {
        assertTrue(PartitionReassignmentReplicas.isReassignmentInProgress(
            new PartitionRegistration.Builder().
                setReplicas(new int[]{0, 1, 3, 2}).
                setIsr(new int[]{0, 1, 3, 2}).
                setRemovingReplicas(new int[]{2}).
                setAddingReplicas(new int[]{3}).
                setLeader(0).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).
                build()));
        assertTrue(PartitionReassignmentReplicas.isReassignmentInProgress(
            new PartitionRegistration.Builder().
                setReplicas(new int[]{0, 1, 3, 2}).
                setIsr(new int[]{0, 1, 3, 2}).
                setRemovingReplicas(new int[]{2}).
                setLeader(0).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).
                build()));
        assertTrue(PartitionReassignmentReplicas.isReassignmentInProgress(
            new PartitionRegistration.Builder().
                setReplicas(new int[]{0, 1, 3, 2}).
                setIsr(new int[]{0, 1, 3, 2}).
                setAddingReplicas(new int[]{3}).
                setLeader(0).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).
                build()));
        assertFalse(PartitionReassignmentReplicas.isReassignmentInProgress(
            new PartitionRegistration.Builder().
                setReplicas(new int[]{0, 1, 2}).
                setIsr(new int[]{0, 1, 2}).
                setLeader(0).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).
                build()));
    }

    @Test
    public void testOriginalReplicas() {
        PartitionReassignmentReplicas replicas = new PartitionReassignmentReplicas(
            new PartitionAssignment(Arrays.asList(0, 1, 2)), new PartitionAssignment(Arrays.asList(0, 1, 3)));
        assertEquals(Arrays.asList(0, 1, 2), replicas.originalReplicas());
    }
}
