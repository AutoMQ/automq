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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;

/**
 * PartitionChangeBuilder handles changing partition registrations.
 */
public class PartitionChangeBuilder {
    private static final Logger log = LoggerFactory.getLogger(PartitionChangeBuilder.class);

    public static boolean changeRecordIsNoOp(PartitionChangeRecord record) {
        if (record.isr() != null) return false;
        if (record.leader() != NO_LEADER_CHANGE) return false;
        if (record.replicas() != null) return false;
        if (record.removingReplicas() != null) return false;
        if (record.addingReplicas() != null) return false;
        if (record.leaderRecoveryState() != LeaderRecoveryState.NO_CHANGE) return false;
        return true;
    }

    /**
     * Election types.
     */
    public enum Election {
        /**
         * Perform leader election to keep the partition online. Elect the preferred replica if it is in the ISR.
         */
        PREFERRED,
        /**
         * Perform leader election from the ISR to keep the partition online.
         */
        ONLINE,
        /**
         * Prefer replicas in the ISR but keep the partition online even if it requires picking a leader that is not in the ISR.
         */
        UNCLEAN
    }

    private final PartitionRegistration partition;
    private final Uuid topicId;
    private final int partitionId;
    private final IntPredicate isAcceptableLeader;
    private final MetadataVersion metadataVersion;
    private List<Integer> targetIsr;
    private List<Integer> targetReplicas;
    private List<Integer> targetRemoving;
    private List<Integer> targetAdding;
    private Election election = Election.ONLINE;
    private LeaderRecoveryState targetLeaderRecoveryState;
    private boolean zkMigrationEnabled;


    public PartitionChangeBuilder(
        PartitionRegistration partition,
        Uuid topicId,
        int partitionId,
        IntPredicate isAcceptableLeader,
        MetadataVersion metadataVersion
    ) {
        this.partition = partition;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.isAcceptableLeader = isAcceptableLeader;
        this.metadataVersion = metadataVersion;
        this.zkMigrationEnabled = false;

        this.targetIsr = Replicas.toList(partition.isr);
        this.targetReplicas = Replicas.toList(partition.replicas);
        this.targetRemoving = Replicas.toList(partition.removingReplicas);
        this.targetAdding = Replicas.toList(partition.addingReplicas);
        this.targetLeaderRecoveryState = partition.leaderRecoveryState;
    }

    public PartitionChangeBuilder setTargetIsr(List<Integer> targetIsr) {
        this.targetIsr = targetIsr;
        return this;
    }

    public PartitionChangeBuilder setTargetIsrWithBrokerStates(List<BrokerState> targetIsrWithEpoch) {
        return setTargetIsr(
            targetIsrWithEpoch
              .stream()
              .map(brokerState -> brokerState.brokerId())
              .collect(Collectors.toList())
        );
    }

    public PartitionChangeBuilder setTargetReplicas(List<Integer> targetReplicas) {
        this.targetReplicas = targetReplicas;
        return this;
    }

    public PartitionChangeBuilder setElection(Election election) {
        this.election = election;
        return this;
    }

    public PartitionChangeBuilder setTargetRemoving(List<Integer> targetRemoving) {
        this.targetRemoving = targetRemoving;
        return this;
    }

    public PartitionChangeBuilder setTargetAdding(List<Integer> targetAdding) {
        this.targetAdding = targetAdding;
        return this;
    }

    public PartitionChangeBuilder setTargetLeaderRecoveryState(LeaderRecoveryState targetLeaderRecoveryState) {
        this.targetLeaderRecoveryState = targetLeaderRecoveryState;
        return this;
    }

    public PartitionChangeBuilder setZkMigrationEnabled(boolean zkMigrationEnabled) {
        this.zkMigrationEnabled = zkMigrationEnabled;
        return this;
    }

    // VisibleForTesting
    static class ElectionResult {
        final int node;
        final boolean unclean;

        private ElectionResult(int node, boolean unclean) {
            this.node = node;
            this.unclean = unclean;
        }
    }

    // VisibleForTesting
    /**
     * Perform leader election based on the partition state and leader election type.
     *
     * See documentation for the Election type to see more details on the election types supported.
     */
    ElectionResult electLeader() {
        if (election == Election.PREFERRED) {
            return electPreferredLeader();
        }

        return electAnyLeader();
    }

    /**
     * Assumes that the election type is Election.PREFERRED
     */
    private ElectionResult electPreferredLeader() {
        int preferredReplica = targetReplicas.get(0);
        if (isValidNewLeader(preferredReplica)) {
            return new ElectionResult(preferredReplica, false);
        }

        if (isValidNewLeader(partition.leader)) {
            // Don't consider a new leader since the current leader meets all the constraints
            return new ElectionResult(partition.leader, false);
        }

        Optional<Integer> onlineLeader = targetReplicas.stream()
            .skip(1)
            .filter(this::isValidNewLeader)
            .findFirst();
        if (onlineLeader.isPresent()) {
            return new ElectionResult(onlineLeader.get(), false);
        }

        return new ElectionResult(NO_LEADER, false);
    }

    /**
     * Assumes that the election type is either Election.ONLINE or Election.UNCLEAN
     */
    private ElectionResult electAnyLeader() {
        if (isValidNewLeader(partition.leader)) {
            // Don't consider a new leader since the current leader meets all the constraints
            return new ElectionResult(partition.leader, false);
        }

        Optional<Integer> onlineLeader = targetReplicas.stream()
            .filter(this::isValidNewLeader)
            .findFirst();
        if (onlineLeader.isPresent()) {
            return new ElectionResult(onlineLeader.get(), false);
        }

        if (election == Election.UNCLEAN) {
            // Attempt unclean leader election
            Optional<Integer> uncleanLeader = targetReplicas.stream()
                .filter(replica -> isAcceptableLeader.test(replica))
                .findFirst();
            if (uncleanLeader.isPresent()) {
                return new ElectionResult(uncleanLeader.get(), true);
            }
        }

        return new ElectionResult(NO_LEADER, false);
    }

    private boolean isValidNewLeader(int replica) {
        return targetIsr.contains(replica) && isAcceptableLeader.test(replica);
    }

    private void tryElection(PartitionChangeRecord record) {
        ElectionResult electionResult = electLeader();
        if (electionResult.node != partition.leader) {
            // generating log messages for partition elections can get expensive on large clusters,
            // so only log clean elections at TRACE level; log unclean elections at INFO level
            // to ensure the message is emitted since an unclean election can lead to data loss.
            if (electionResult.unclean) {
                log.info("Setting new leader for topicId {}, partition {} to {} using an unclean election",
                    topicId, partitionId, electionResult.node);
            } else {
                log.trace("Setting new leader for topicId {}, partition {} to {} using a clean election",
                    topicId, partitionId, electionResult.node);
            }
            record.setLeader(electionResult.node);
            if (electionResult.unclean) {
                // If the election was unclean, we have to forcibly set the ISR to just the
                // new leader. This can result in data loss!
                record.setIsr(Collections.singletonList(electionResult.node));
                if (partition.leaderRecoveryState != LeaderRecoveryState.RECOVERING &&
                    metadataVersion.isLeaderRecoverySupported()) {
                    // And mark the leader recovery state as RECOVERING
                    record.setLeaderRecoveryState(LeaderRecoveryState.RECOVERING.value());
                }
            }
        } else {
            log.trace("Failed to find a new leader with current state: {}", this);
        }
    }

    /**
     * Trigger a leader epoch bump if one is needed.
     *
     * We need to bump the leader epoch if:
     * 1. The leader changed, or
     * 2. The new replica list does not contain all the nodes that the old replica list did.
     *
     * Changes that do NOT fall in any of these categories will increase the partition epoch, but
     * not the leader epoch. Note that if the leader epoch increases, the partition epoch will
     * always increase as well; there is no case where the partition epoch increases more slowly
     * than the leader epoch.
     *
     * If the PartitionChangeRecord sets the leader field to something other than
     * NO_LEADER_CHANGE, a leader epoch bump will automatically occur. That takes care of
     * case 1. In this function, we check for cases 2 and 3, and handle them by manually
     * setting record.leader to the current leader.
     *
     * In MV before 3.6 there was a bug (KAFKA-15021) in the brokers' replica manager
     * that required that the leader epoch be bump whenever the ISR shrank. In MV 3.6 this leader
     * bump is not required when the ISR shrinks. Note, that the leader epoch is never increased if
     * the ISR expanded.
     *
     * In MV 3.6 and beyond, if the controller is in ZK migration mode, the leader epoch must
     * be bumped during ISR shrink for compatability with ZK brokers.
     */
    void triggerLeaderEpochBumpIfNeeded(PartitionChangeRecord record) {
        if (record.leader() == NO_LEADER_CHANGE) {
            boolean bumpLeaderEpochOnIsrShrink = metadataVersion.isLeaderEpochBumpRequiredOnIsrShrink() || zkMigrationEnabled;

            if (!Replicas.contains(targetReplicas, partition.replicas)) {
                // Reassignment
                record.setLeader(partition.leader);
            } else if (bumpLeaderEpochOnIsrShrink && !Replicas.contains(targetIsr, partition.isr)) {
                // ISR shrink
                record.setLeader(partition.leader);
            }
        }
    }

    private void completeReassignmentIfNeeded() {
        PartitionReassignmentReplicas reassignmentReplicas =
            new PartitionReassignmentReplicas(
                targetRemoving,
                targetAdding,
                targetReplicas);

        Optional<PartitionReassignmentReplicas.CompletedReassignment> completedReassignmentOpt =
            reassignmentReplicas.maybeCompleteReassignment(targetIsr);
        if (!completedReassignmentOpt.isPresent()) {
            return;
        }

        PartitionReassignmentReplicas.CompletedReassignment completedReassignment = completedReassignmentOpt.get();

        targetIsr = completedReassignment.isr;
        targetReplicas = completedReassignment.replicas;
        targetRemoving = Collections.emptyList();
        targetAdding = Collections.emptyList();
    }

    public Optional<ApiMessageAndVersion> build() {
        PartitionChangeRecord record = new PartitionChangeRecord().
            setTopicId(topicId).
            setPartitionId(partitionId);

        completeReassignmentIfNeeded();

        tryElection(record);

        triggerLeaderEpochBumpIfNeeded(record);

        if (record.isr() == null && !targetIsr.isEmpty() && !targetIsr.equals(Replicas.toList(partition.isr))) {
            // Set the new ISR if it is different from the current ISR and unclean leader election didn't already set it.
            record.setIsr(targetIsr);
        }

        setAssignmentChanges(record);

        if (targetLeaderRecoveryState != partition.leaderRecoveryState) {
            record.setLeaderRecoveryState(targetLeaderRecoveryState.value());
        }

        if (changeRecordIsNoOp(record)) {
            return Optional.empty();
        } else {
            return Optional.of(new ApiMessageAndVersion(record, (short) 0));
        }
    }

    private void setAssignmentChanges(PartitionChangeRecord record) {
        if (!targetReplicas.isEmpty() && !targetReplicas.equals(Replicas.toList(partition.replicas))) {
            record.setReplicas(targetReplicas);
        }
        if (!targetRemoving.equals(Replicas.toList(partition.removingReplicas))) {
            record.setRemovingReplicas(targetRemoving);
        }
        if (!targetAdding.equals(Replicas.toList(partition.addingReplicas))) {
            record.setAddingReplicas(targetAdding);
        }
    }

    @Override
    public String toString() {
        return "PartitionChangeBuilder(" +
            "partition=" + partition +
            ", topicId=" + topicId +
            ", partitionId=" + partitionId +
            ", isAcceptableLeader=" + isAcceptableLeader +
            ", targetIsr=" + targetIsr +
            ", targetReplicas=" + targetReplicas +
            ", targetRemoving=" + targetRemoving +
            ", targetAdding=" + targetAdding +
            ", election=" + election +
            ", targetLeaderRecoveryState=" + targetLeaderRecoveryState +
            ')';
    }
}
