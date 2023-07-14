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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.CurrentAssignmentBuilder;
import org.apache.kafka.coordinator.group.consumer.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.group.runtime.CoordinatorTimer;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentTombstoneRecord;

/**
 * The GroupMetadataManager manages the metadata of all generic and consumer groups. It holds
 * the hard and the soft state of the groups. This class has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class GroupMetadataManager {

    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private Time time = null;
        private CoordinatorTimer<Record> timer = null;
        private List<PartitionAssignor> assignors = null;
        private int consumerGroupMaxSize = Integer.MAX_VALUE;
        private int consumerGroupSessionTimeoutMs = 45000;
        private int consumerGroupHeartbeatIntervalMs = 5000;
        private int consumerGroupMetadataRefreshIntervalMs = Integer.MAX_VALUE;
        private MetadataImage metadataImage = null;

        Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        Builder withTimer(CoordinatorTimer<Record> timer) {
            this.timer = timer;
            return this;
        }

        Builder withAssignors(List<PartitionAssignor> assignors) {
            this.assignors = assignors;
            return this;
        }

        Builder withConsumerGroupMaxSize(int consumerGroupMaxSize) {
            this.consumerGroupMaxSize = consumerGroupMaxSize;
            return this;
        }

        Builder withConsumerGroupSessionTimeout(int consumerGroupSessionTimeoutMs) {
            this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
            return this;
        }

        Builder withConsumerGroupHeartbeatInterval(int consumerGroupHeartbeatIntervalMs) {
            this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
            return this;
        }

        Builder withConsumerGroupMetadataRefreshIntervalMs(int consumerGroupMetadataRefreshIntervalMs) {
            this.consumerGroupMetadataRefreshIntervalMs = consumerGroupMetadataRefreshIntervalMs;
            return this;
        }

        Builder withMetadataImage(MetadataImage metadataImage) {
            this.metadataImage = metadataImage;
            return this;
        }

        GroupMetadataManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (time == null) time = Time.SYSTEM;

            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (assignors == null || assignors.isEmpty())
                throw new IllegalArgumentException("Assignors must be set before building.");

            return new GroupMetadataManager(
                snapshotRegistry,
                logContext,
                time,
                timer,
                assignors,
                metadataImage,
                consumerGroupMaxSize,
                consumerGroupSessionTimeoutMs,
                consumerGroupHeartbeatIntervalMs,
                consumerGroupMetadataRefreshIntervalMs
            );
        }
    }

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The system timer.
     */
    private final CoordinatorTimer<Record> timer;

    /**
     * The supported partition assignors keyed by their name.
     */
    private final Map<String, PartitionAssignor> assignors;

    /**
     * The default assignor used.
     */
    private final PartitionAssignor defaultAssignor;

    /**
     * The generic and consumer groups keyed by their name.
     */
    private final TimelineHashMap<String, Group> groups;

    /**
     * The group ids keyed by topic names.
     */
    private final TimelineHashMap<String, TimelineHashSet<String>> groupsByTopics;

    /**
     * The maximum number of members allowed in a single consumer group.
     */
    private final int consumerGroupMaxSize;

    /**
     * The heartbeat interval for consumer groups.
     */
    private final int consumerGroupHeartbeatIntervalMs;

    /**
     * The session timeout for consumer groups.
     */
    private final int consumerGroupSessionTimeoutMs;

    /**
     * The metadata refresh interval.
     */
    private final int consumerGroupMetadataRefreshIntervalMs;

    /**
     * The metadata image.
     */
    private MetadataImage metadataImage;

    private GroupMetadataManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        Time time,
        CoordinatorTimer<Record> timer,
        List<PartitionAssignor> assignors,
        MetadataImage metadataImage,
        int consumerGroupMaxSize,
        int consumerGroupSessionTimeoutMs,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupMetadataRefreshIntervalMs
    ) {
        this.log = logContext.logger(GroupMetadataManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.time = time;
        this.timer = timer;
        this.metadataImage = metadataImage;
        this.assignors = assignors.stream().collect(Collectors.toMap(PartitionAssignor::name, Function.identity()));
        this.defaultAssignor = assignors.get(0);
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
        this.groupsByTopics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupMetadataRefreshIntervalMs = consumerGroupMetadataRefreshIntervalMs;
    }

    /**
     * @return The current metadata image used by the group metadata manager.
     */
    public MetadataImage image() {
        return metadataImage;
    }

    /**
     * Gets or maybe creates a consumer group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     *
     * Package private for testing.
     */
    ConsumerGroup getOrMaybeCreateConsumerGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found.", groupId));
        }

        if (group == null) {
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId);
            groups.put(groupId, consumerGroup);
            return consumerGroup;
        } else {
            if (group.type() == Group.GroupType.CONSUMER) {
                return (ConsumerGroup) group;
            } else {
                // We don't support upgrading/downgrading between protocols at the moment so
                // we throw an exception if a group exists with the wrong type.
                throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group.", groupId));
            }
        }
    }

    /**
     * Removes the group.
     *
     * @param groupId The group id.
     */
    private void removeGroup(
        String groupId
    ) {
        groups.remove(groupId);
    }

    /**
     * Throws an InvalidRequestException if the value is non-null and empty.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfEmptyString(
        String value,
        String error
    ) throws InvalidRequestException {
        if (value != null && value.isEmpty()) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is non-null.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfNotNull(
        Object value,
        String error
    ) throws InvalidRequestException {
        if (value != null) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Validates the request.
     *
     * @param request The request to validate.
     *
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private void throwIfConsumerGroupHeartbeatRequestIsInvalid(
        ConsumerGroupHeartbeatRequestData request
    ) throws InvalidRequestException, UnsupportedAssignorException {
        throwIfEmptyString(request.groupId(), "GroupId can't be empty.");
        throwIfEmptyString(request.instanceId(), "InstanceId can't be empty.");
        throwIfEmptyString(request.rackId(), "RackId can't be empty.");
        throwIfNotNull(request.subscribedTopicRegex(), "SubscribedTopicRegex is not supported yet.");
        throwIfNotNull(request.clientAssignors(), "Client side assignors are not supported yet.");

        if (request.memberEpoch() > 0 || request.memberEpoch() == -1) {
            throwIfEmptyString(request.memberId(), "MemberId can't be empty.");
        } else if (request.memberEpoch() == 0) {
            if (request.rebalanceTimeoutMs() == -1) {
                throw new InvalidRequestException("RebalanceTimeoutMs must be provided in first request.");
            }
            if (request.topicPartitions() == null || !request.topicPartitions().isEmpty()) {
                throw new InvalidRequestException("TopicPartitions must be empty when (re-)joining.");
            }
            if (request.subscribedTopicNames() == null || request.subscribedTopicNames().isEmpty()) {
                throw new InvalidRequestException("SubscribedTopicNames must be set in first request.");
            }
        } else {
            throw new InvalidRequestException("MemberEpoch is invalid.");
        }

        if (request.serverAssignor() != null && !assignors.containsKey(request.serverAssignor())) {
            throw new UnsupportedAssignorException("ServerAssignor " + request.serverAssignor()
                + " is not supported. Supported assignors: " + String.join(", ", assignors.keySet())
                + ".");
        }
    }

    /**
     * Verifies that the partitions currently owned by the member (the ones set in the
     * request) matches the ones that the member should own. It matches if the consumer
     * only owns partitions which are in the assigned partitions. It does not match if
     * it owns any other partitions.
     *
     * @param ownedTopicPartitions  The partitions provided by the consumer in the request.
     * @param target                The partitions that the member should have.
     *
     * @return A boolean indicating whether the owned partitions are a subset or not.
     */
    private boolean isSubset(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        Map<Uuid, Set<Integer>> target
    ) {
        if (ownedTopicPartitions == null) return false;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
            Set<Integer> partitions = target.get(topicPartitions.topicId());
            if (partitions == null) return false;
            for (Integer partitionId : topicPartitions.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }

    /**
     * Checks whether the consumer group can accept a new member or not based on the
     * max group size defined.
     *
     * @param group     The consumer group.
     * @param memberId  The member id.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    private void throwIfConsumerGroupIsFull(
        ConsumerGroup group,
        String memberId
    ) throws GroupMaxSizeReachedException {
        // If the consumer group has reached its maximum capacity, the member is rejected if it is not
        // already a member of the consumer group.
        if (group.numMembers() >= consumerGroupMaxSize && (memberId.isEmpty() || !group.hasMember(memberId))) {
            throw new GroupMaxSizeReachedException("The consumer group has reached its maximum capacity of "
                + consumerGroupMaxSize + " members.");
        }
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The consumer group member.
     * @param receivedMemberEpoch   The member epoch.
     * @param ownedTopicPartitions  The owned partitions.
     *
     * @throws NotCoordinatorException if the provided epoch is ahead of the epoch known
     *                                 by this coordinator. This suggests that the member
     *                                 got a higher epoch from another coordinator.
     * @throws FencedMemberEpochException if the provided epoch is behind the epoch known
     *                                    by this coordinator.
     */
    private void throwIfMemberEpochIsInvalid(
        ConsumerGroupMember member,
        int receivedMemberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The consumer group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch() || !isSubset(ownedTopicPartitions, member.assignedPartitions())) {
                throw new FencedMemberEpochException("The consumer group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createResponseAssignment(
        ConsumerGroupMember member
    ) {
        ConsumerGroupHeartbeatResponseData.Assignment assignment = new ConsumerGroupHeartbeatResponseData.Assignment()
            .setAssignedTopicPartitions(fromAssignmentMap(member.assignedPartitions()));

        if (member.state() == ConsumerGroupMember.MemberState.ASSIGNING) {
            assignment.setPendingTopicPartitions(fromAssignmentMap(member.partitionsPendingAssignment()));
        }

        return assignment;
    }

    private List<ConsumerGroupHeartbeatResponseData.TopicPartitions> fromAssignmentMap(
        Map<Uuid, Set<Integer>> assignment
    ) {
        return assignment.entrySet().stream()
            .map(keyValue -> new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(keyValue.getKey())
                .setPartitions(new ArrayList<>(keyValue.getValue())))
            .collect(Collectors.toList());
    }

    private OptionalInt ofSentinel(int value) {
        return value != -1 ? OptionalInt.of(value) : OptionalInt.empty();
    }

    /**
     * Handles a regular heartbeat from a consumer group member. It mainly consists of
     * three parts:
     * 1) The member is created or updated. The group epoch is bumped if the member
     *    has been created or updated.
     * 2) The target assignment for the consumer group is updated if the group epoch
     *    is larger than the current target assignment epoch.
     * 3) The member's assignment is reconciled with the target assignment.
     *
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param instanceId            The instance id from the request or null.
     * @param rackId                The rack id from the request or null.
     * @param rebalanceTimeoutMs    The rebalance timeout from the request or -1.
     * @param clientId              The client id.
     * @param clientHost            The client host.
     * @param subscribedTopicNames  The list of subscribed topic names from the request
     *                              of null.
     * @param subscribedTopicRegex  The regular expression based subscription from the
     *                              request or null.
     * @param assignorName          The assignor name from the request or null.
     * @param ownedTopicPartitions  The list of owned partitions from the request or null.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> consumerGroupHeartbeat(
        String groupId,
        String memberId,
        int memberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String assignorName,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<Record> records = new ArrayList<>();

        // Get or create the consumer group.
        boolean createIfNotExists = memberEpoch == 0;
        final ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, createIfNotExists);
        throwIfConsumerGroupIsFull(group, memberId);

        // Get or create the member.
        if (memberId.isEmpty()) memberId = Uuid.randomUuid().toString();
        final ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, createIfNotExists);
        throwIfMemberEpochIsInvalid(member, memberEpoch, ownedTopicPartitions);

        if (memberEpoch == 0) {
            log.info("[GroupId {}] Member {} joins the consumer group.", groupId, memberId);
        }

        // 1. Create or update the member. If the member is new or has changed, a ConsumerGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ConsumerGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ConsumerGroupMetadataValue record to the partition.
        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.ofNullable(instanceId))
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(rebalanceTimeoutMs))
            .maybeUpdateServerAssignorName(Optional.ofNullable(assignorName))
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscribedTopicNames))
            .maybeUpdateSubscribedTopicRegex(Optional.ofNullable(subscribedTopicRegex))
            .setClientId(clientId)
            .setClientHost(clientHost)
            .build();

        boolean bumpGroupEpoch = false;
        if (!updatedMember.equals(member)) {
            records.add(newMemberSubscriptionRecord(groupId, updatedMember));

            if (!updatedMember.subscribedTopicNames().equals(member.subscribedTopicNames())) {
                log.info("[GroupId {}] Member {} updated its subscribed topics to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicNames());
                bumpGroupEpoch = true;
            }

            if (!updatedMember.subscribedTopicRegex().equals(member.subscribedTopicRegex())) {
                log.info("[GroupId {}] Member {} updated its subscribed regex to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicRegex());
                bumpGroupEpoch = true;
            }
        }

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            subscriptionMetadata = group.computeSubscriptionMetadata(
                member,
                updatedMember,
                metadataImage.topics()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                log.info("[GroupId {}] Computed new subscription metadata: {}.",
                    groupId, subscriptionMetadata);
                bumpGroupEpoch = true;
                records.add(newGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
            }

            group.setMetadataRefreshDeadline(currentTimeMs + consumerGroupMetadataRefreshIntervalMs, groupEpoch);
        }

        // 2. Update the target assignment if the group epoch is larger than the target assignment epoch. The
        // delta between the existing and the new target assignment is persisted to the partition.
        int targetAssignmentEpoch = group.assignmentEpoch();
        Assignment targetAssignment = group.targetAssignment(memberId);
        if (groupEpoch > targetAssignmentEpoch) {
            String preferredServerAssignor = group.computePreferredServerAssignor(
                member,
                updatedMember
            ).orElse(defaultAssignor.name());

            try {
                TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                    new TargetAssignmentBuilder(groupId, groupEpoch, assignors.get(preferredServerAssignor))
                        .withMembers(group.members())
                        .withSubscriptionMetadata(subscriptionMetadata)
                        .withTargetAssignment(group.targetAssignment())
                        .addOrUpdateMember(memberId, updatedMember)
                        .build();

                log.info("[GroupId {}] Computed a new target assignment for epoch {}: {}.",
                    groupId, groupEpoch, assignmentResult.targetAssignment());

                records.addAll(assignmentResult.records());
                targetAssignment = assignmentResult.targetAssignment().get(memberId);
                targetAssignmentEpoch = groupEpoch;
            } catch (PartitionAssignorException ex) {
                String msg = String.format("Failed to compute a new target assignment for epoch %d: %s",
                    groupEpoch, ex.getMessage());
                log.error("[GroupId {}] {}.", groupId, msg);
                throw new UnknownServerException(msg, ex);
            }
        }

        // 3. Reconcile the member's assignment with the target assignment. This is only required if
        // the member is not stable or if a new target assignment has been installed.
        boolean assignmentUpdated = false;
        if (updatedMember.state() != ConsumerGroupMember.MemberState.STABLE || updatedMember.targetMemberEpoch() != targetAssignmentEpoch) {
            ConsumerGroupMember prevMember = updatedMember;
            updatedMember = new CurrentAssignmentBuilder(updatedMember)
                .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
                .withCurrentPartitionEpoch(group::currentPartitionEpoch)
                .withOwnedTopicPartitions(ownedTopicPartitions)
                .build();

            // Checking the reference is enough here because a new instance
            // is created only when the state has changed.
            if (updatedMember != prevMember) {
                assignmentUpdated = true;
                records.add(newCurrentAssignmentRecord(groupId, updatedMember));

                log.info("[GroupId {}] Member {} transitioned from {} to {}.",
                    groupId, memberId, member.currentAssignmentSummary(), updatedMember.currentAssignmentSummary());

                if (updatedMember.state() == ConsumerGroupMember.MemberState.REVOKING) {
                    scheduleConsumerGroupRevocationTimeout(
                        groupId,
                        memberId,
                        updatedMember.rebalanceTimeoutMs(),
                        updatedMember.memberEpoch()
                    );
                } else {
                    cancelConsumerGroupRevocationTimeout(groupId, memberId);
                }
            }
        }

        scheduleConsumerGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ConsumerGroupHeartbeatResponseData response = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(consumerGroupHeartbeatIntervalMs);

        // The assignment is only provided in the following cases:
        // 1. The member reported its owned partitions;
        // 2. The member just joined or rejoined to group (epoch equals to zero);
        // 3. The member's assignment has been updated.
        if (ownedTopicPartitions != null || memberEpoch == 0 || assignmentUpdated) {
            response.setAssignment(createResponseAssignment(updatedMember));
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Handles leave request from a consumer group member.
     * @param groupId       The group id from the request.
     * @param memberId      The member id from the request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> consumerGroupLeave(
        String groupId,
        String memberId
    ) throws ApiException {
        ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, false);
        ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);

        log.info("[GroupId " + groupId + "] Member " + memberId + " left the consumer group.");

        List<Record> records = consumerGroupFenceMember(group, member);
        return new CoordinatorResult<>(records, new ConsumerGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(-1));
    }

    /**
     * Fences a member from a consumer group.
     *
     * @param group       The group.
     * @param member      The member.
     *
     * @return A list of records to be applied to the state.
     */
    private List<Record> consumerGroupFenceMember(
        ConsumerGroup group,
        ConsumerGroupMember member
    ) {
        List<Record> records = new ArrayList<>();

        // Write tombstones for the member. The order matters here.
        records.add(newCurrentAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newTargetAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newMemberSubscriptionTombstoneRecord(group.groupId(), member.memberId()));

        // We update the subscription metadata without the leaving member.
        Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
            member,
            null,
            metadataImage.topics()
        );

        if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
            log.info("[GroupId {}] Computed new subscription metadata: {}.",
                group.groupId(), subscriptionMetadata);
            records.add(newGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
        }

        // We bump the group epoch.
        int groupEpoch = group.groupEpoch() + 1;
        records.add(newGroupEpochRecord(group.groupId(), groupEpoch));

        // Cancel all the timers of the member.
        cancelConsumerGroupSessionTimeout(group.groupId(), member.memberId());
        cancelConsumerGroupRevocationTimeout(group.groupId(), member.memberId());

        return records;
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleConsumerGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        String key = consumerGroupSessionTimeoutKey(groupId, memberId);
        timer.schedule(key, consumerGroupSessionTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, false);
                ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);
                log.info("[GroupId {}] Member {} fenced from the group because its session expired.",
                    groupId, memberId);
                return consumerGroupFenceMember(group, member);
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return Collections.emptyList();
        });
    }

    /**
     * Cancels the session timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupSessionTimeoutKey(groupId, memberId));
    }

    /**
     * Schedules a revocation timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param revocationTimeoutMs   The revocation timeout.
     * @param expectedMemberEpoch   The expected member epoch.
     */
    private void scheduleConsumerGroupRevocationTimeout(
        String groupId,
        String memberId,
        long revocationTimeoutMs,
        int expectedMemberEpoch
    ) {
        String key = consumerGroupRevocationTimeoutKey(groupId, memberId);
        timer.schedule(key, revocationTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, false);
                ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);

                if (member.state() != ConsumerGroupMember.MemberState.REVOKING ||
                    member.memberEpoch() != expectedMemberEpoch) {
                    log.debug("[GroupId {}] Ignoring revocation timeout for {} because the member " +
                        "state does not match the expected state.", groupId, memberId);
                    return Collections.emptyList();
                }

                log.info("[GroupId {}] Member {} fenced from the group because " +
                    "it failed to revoke partitions within {}ms.", groupId, memberId, revocationTimeoutMs);
                return consumerGroupFenceMember(group, member);
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {}} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return Collections.emptyList();
        });
    }

    /**
     * Cancels the revocation timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupRevocationTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupRevocationTimeoutKey(groupId, memberId));
    }

    /**
     * Handles a ConsumerGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ConsumerGroupHeartbeat request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) throws ApiException {
        throwIfConsumerGroupHeartbeatRequestIsInvalid(request);

        if (request.memberEpoch() == -1) {
            // -1 means that the member wants to leave the group.
            return consumerGroupLeave(
                request.groupId(),
                request.memberId()
            );
        } else {
            // Otherwise, it is a regular heartbeat.
            return consumerGroupHeartbeat(
                request.groupId(),
                request.memberId(),
                request.memberEpoch(),
                request.instanceId(),
                request.rackId(),
                request.rebalanceTimeoutMs(),
                context.clientId(),
                context.clientAddress.toString(),
                request.subscribedTopicNames(),
                request.subscribedTopicRegex(),
                request.serverAssignor(),
                request.topicPartitions()
            );
        }
    }

    /**
     * Replays ConsumerGroupMemberMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription part of the member or
     * delete the member.
     *
     * @param key   A ConsumerGroupMemberMetadataKey key.
     * @param value A ConsumerGroupMemberMetadataValue record.
     */
    public void replay(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, value != null);
        Set<String> oldSubscribedTopicNames = new HashSet<>(consumerGroup.subscribedTopicNames());

        if (value != null) {
            ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, true);
            consumerGroup.updateMember(new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, false);
            if (oldMember.memberEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupCurrentMemberAssignmentValue tombstone.");
            }
            if (consumerGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            consumerGroup.removeMember(memberId);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, consumerGroup.subscribedTopicNames());
    }

    /**
     * @return The set of groups subscribed to the topic.
     */
    public Set<String> groupsSubscribedToTopic(String topicName) {
        Set<String> groups = groupsByTopics.get(topicName);
        return groups != null ? groups : Collections.emptySet();
    }

    /**
     * Subscribes a group to a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    private void subscribeGroupToTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics
            .computeIfAbsent(topicName, __ -> new TimelineHashSet<>(snapshotRegistry, 1))
            .add(groupId);
    }

    /**
     * Unsubscribes a group from a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    private void unsubscribeGroupFromTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics.computeIfPresent(topicName, (__, groupIds) -> {
            groupIds.remove(groupId);
            return groupIds.isEmpty() ? null : groupIds;
        });
    }

    /**
     * Updates the group by topics mapping.
     *
     * @param groupId               The group id.
     * @param oldSubscribedTopics   The old group subscriptions.
     * @param newSubscribedTopics   The new group subscriptions.
     */
    private void updateGroupsByTopics(
        String groupId,
        Set<String> oldSubscribedTopics,
        Set<String> newSubscribedTopics
    ) {
        if (oldSubscribedTopics.isEmpty()) {
            newSubscribedTopics.forEach(topicName ->
                subscribeGroupToTopic(groupId, topicName)
            );
        } else if (newSubscribedTopics.isEmpty()) {
            oldSubscribedTopics.forEach(topicName ->
                unsubscribeGroupFromTopic(groupId, topicName)
            );
        } else {
            oldSubscribedTopics.forEach(topicName -> {
                if (!newSubscribedTopics.contains(topicName)) {
                    unsubscribeGroupFromTopic(groupId, topicName);
                }
            });
            newSubscribedTopics.forEach(topicName -> {
                if (!oldSubscribedTopics.contains(topicName)) {
                    subscribeGroupToTopic(groupId, topicName);
                }
            });
        }
    }

    /**
     * Replays ConsumerGroupMetadataKey/Value to update the hard state of
     * the consumer group. It updates the group epoch of the consumer
     * group or deletes the consumer group.
     *
     * @param key   A ConsumerGroupMetadataKey key.
     * @param value A ConsumerGroupMetadataValue record.
     */
    public void replay(
        ConsumerGroupMetadataKey key,
        ConsumerGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, true);
            consumerGroup.setGroupEpoch(value.epoch());
        } else {
            ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);
            if (!consumerGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + consumerGroup.members().size() + " members.");
            }
            if (!consumerGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the target assignment still has " + consumerGroup.targetAssignment().size()
                    + " members.");
            }
            if (consumerGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            removeGroup(groupId);
        }

    }

    /**
     * Replays ConsumerGroupPartitionMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription metadata of the consumer
     * group.
     *
     * @param key   A ConsumerGroupPartitionMetadataKey key.
     * @param value A ConsumerGroupPartitionMetadataValue record.
     */
    public void replay(
        ConsumerGroupPartitionMetadataKey key,
        ConsumerGroupPartitionMetadataValue value
    ) {
        String groupId = key.groupId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);

        if (value != null) {
            Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
            value.topics().forEach(topicMetadata -> {
                subscriptionMetadata.put(topicMetadata.topicName(), TopicMetadata.fromRecord(topicMetadata));
            });
            consumerGroup.setSubscriptionMetadata(subscriptionMetadata);
        } else {
            consumerGroup.setSubscriptionMetadata(Collections.emptyMap());
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the consumer group. It updates the target assignment of the member or deletes it.
     *
     * @param key   A ConsumerGroupTargetAssignmentMemberKey key.
     * @param value A ConsumerGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMemberKey key,
        ConsumerGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);

        if (value != null) {
            consumerGroup.updateTargetAssignment(memberId, Assignment.fromRecord(value));
        } else {
            consumerGroup.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the consumer group. It updates the target assignment epoch or set it to -1 to signal
     * that it has been deleted.
     *
     * @param key   A ConsumerGroupTargetAssignmentMetadataKey key.
     * @param value A ConsumerGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMetadataKey key,
        ConsumerGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);

        if (value != null) {
            consumerGroup.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            if (!consumerGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                    + " but the assignment still has " + consumerGroup.targetAssignment().size() + " members.");
            }
            consumerGroup.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays ConsumerGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the consumer group. It updates the assignment of a member or deletes it.
     *
     * @param key   A ConsumerGroupCurrentMemberAssignmentKey key.
     * @param value A ConsumerGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        ConsumerGroupCurrentMemberAssignmentKey key,
        ConsumerGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);
        ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, false);

        if (value != null) {
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            consumerGroup.updateMember(newMember);
        } else {
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .setMemberEpoch(-1)
                .setPreviousMemberEpoch(-1)
                .setTargetMemberEpoch(-1)
                .setAssignedPartitions(Collections.emptyMap())
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .setPartitionsPendingAssignment(Collections.emptyMap())
                .build();
            consumerGroup.updateMember(newMember);
        }
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        metadataImage = newImage;

        // Notify all the groups subscribed to the created, updated or
        // deleted topics.
        Optional.ofNullable(delta.topicsDelta()).ifPresent(topicsDelta -> {
            Set<String> allGroupIds = new HashSet<>();
            topicsDelta.changedTopics().forEach((topicId, topicDelta) -> {
                String topicName = topicDelta.name();
                allGroupIds.addAll(groupsSubscribedToTopic(topicName));
            });
            topicsDelta.deletedTopicIds().forEach(topicId -> {
                TopicImage topicImage = delta.image().topics().getTopic(topicId);
                allGroupIds.addAll(groupsSubscribedToTopic(topicImage.name()));
            });
            allGroupIds.forEach(groupId -> {
                Group group = groups.get(groupId);
                if (group != null && group.type() == Group.GroupType.CONSUMER) {
                    ((ConsumerGroup) group).requestMetadataRefresh();
                }
            });
        });
    }

    /**
     * The coordinator has been loaded. Session timeouts are registered
     * for all members.
     */
    public void onLoaded() {
        groups.forEach((groupId, group) -> {
            switch (group.type()) {
                case CONSUMER:
                    ConsumerGroup consumerGroup = (ConsumerGroup) group;
                    log.info("Loaded consumer group {} with {} members.", groupId, consumerGroup.members().size());
                    consumerGroup.members().forEach((memberId, member) -> {
                        log.debug("Loaded member {} in consumer group {}.", memberId, groupId);
                        scheduleConsumerGroupSessionTimeout(groupId, memberId);
                        if (member.state() == ConsumerGroupMember.MemberState.REVOKING) {
                            scheduleConsumerGroupRevocationTimeout(
                                groupId,
                                memberId,
                                member.rebalanceTimeoutMs(),
                                member.memberEpoch()
                            );
                        }
                    });
                    break;

                case GENERIC:
                    GenericGroup genericGroup = (GenericGroup) group;
                    log.info("Loaded generic group {} with {} members.", groupId, genericGroup.allMembers().size());
                    break;
            }
        });
    }

    public static String consumerGroupSessionTimeoutKey(String groupId, String memberId) {
        return "session-timeout-" + groupId + "-" + memberId;
    }

    public static String consumerGroupRevocationTimeoutKey(String groupId, String memberId) {
        return "revocation-timeout-" + groupId + "-" + memberId;
    }
}
