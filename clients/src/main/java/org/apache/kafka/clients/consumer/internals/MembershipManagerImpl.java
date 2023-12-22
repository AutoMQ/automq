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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.Utils.TopicIdPartitionComparator;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryProvider;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED;

/**
 * Group manager for a single consumer that has a group id defined in the config
 * {@link ConsumerConfig#GROUP_ID_CONFIG}, to use the Kafka-based offset management capability,
 * and the consumer group protocol to get automatically assigned partitions when calling the
 * subscribe API.
 *
 * <p/>
 *
 * While the subscribe API hasn't been called (or if the consumer called unsubscribe), this manager
 * will only be responsible for keeping the member in the {@link MemberState#UNSUBSCRIBED} state,
 * where it can commit offsets to the group identified by the {@link #groupId()}, without joining
 * the group.
 *
 * <p/>
 *
 * If the consumer subscribe API is called, this manager will use the {@link #groupId()} to join the
 * consumer group, and based on the consumer group protocol heartbeats, will handle the full
 * lifecycle of the member as it joins the group, reconciles assignments, handles fencing and
 * fatal errors, and leaves the group.
 *
 * <p/>
 *
 * Reconciliation process:<p/>
 * The member accepts all assignments received from the broker, resolves topic names from
 * metadata, reconciles the resolved assignments, and keeps the unresolved to be reconciled when
 * discovered with a metadata update. Reconciliations of resolved assignments are executed
 * sequentially and acknowledged to the server as they complete. The reconciliation process
 * involves multiple async operations, so the member will continue to heartbeat while these
 * operations complete, to make sure that the member stays in the group while reconciling.
 *
 * <p/>
 *
 * Reconciliation steps:
 * <ol>
 *     <li>Resolve topic names for all topic IDs received in the target assignment. Topic names
 *     found in metadata are then ready to be reconciled. Topic IDs not found are kept as
 *     unresolved, and the member request metadata updates until it resolves them (or the broker
 *     removes it from the target assignment.</li>
 *     <li>Commit offsets if auto-commit is enabled.</li>
 *     <li>Invoke the user-defined onPartitionsRevoked listener.</li>
 *     <li>Invoke the user-defined onPartitionsAssigned listener.</li>
 *     <li>When the above steps complete, the member acknowledges the reconciled assignment,
 *     which is the subset of the target that was resolved from metadata and actually reconciled.
 *     The ack is performed by sending a heartbeat request back to the broker, including the
 *     reconciled assignment.</li>
 * </ol>
 *
 * Note that user-defined callbacks are triggered from this manager that runs in the
 * BackgroundThread, but executed in the Application Thread, where a failure will be returned to
 * the user if the callbacks fail. This manager is only concerned about the callbacks completion to
 * know that it can proceed with the reconciliation.
 */
public class MembershipManagerImpl implements MembershipManager, ClusterResourceListener {

    /**
     * TopicPartition comparator based on topic name and partition id.
     */
    final static TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new TopicPartitionComparator();

    /**
     * TopicIdPartition comparator based on topic name and partition id (ignoring ID while sorting,
     * as this is sorted mainly for logging purposes).
     */
    final static TopicIdPartitionComparator TOPIC_ID_PARTITION_COMPARATOR = new TopicIdPartitionComparator();

    /**
     * Group ID of the consumer group the member will be part of, provided when creating the current
     * membership manager.
     */
    private final String groupId;

    /**
     * Group instance ID to be used by the member, provided when creating the current membership manager.
     */
    private final Optional<String> groupInstanceId;

    /**
     * Rebalance timeout. To be used as time limit for the commit request issued
     * when a new assignment is received, that is retried until it succeeds, fails with a
     * non-retriable error, it the time limit expires.
     */
    private final int rebalanceTimeoutMs;

    /**
     * Member ID assigned by the server to the member, received in a heartbeat response when
     * joining the group specified in {@link #groupId}
     */
    private String memberId = "";

    /**
     * Current epoch of the member. It will be set to 0 by the member, and provided to the server
     * on the heartbeat request, to join the group. It will be then maintained by the server,
     * incremented as the member reconciles and acknowledges the assignments it receives. It will
     * be reset to 0 if the member gets fenced.
     */
    private int memberEpoch = 0;

    /**
     * Current state of this member as part of the consumer group, as defined in {@link MemberState}
     */
    private MemberState state;

    /**
     * Name of the server-side assignor this member has configured to use. It will be sent
     * out to the server on the {@link ConsumerGroupHeartbeatRequest}. If not defined, the server
     * will select the assignor implementation to use.
     */
    private final Optional<String> serverAssignor;

    /**
     * Assignment that the member received from the server and successfully processed.
     */
    private Map<Uuid, SortedSet<Integer>> currentAssignment;

    /**
     * Subscription state object holding the current assignment the member has for the topics it
     * subscribed to.
     */
    private final SubscriptionState subscriptions;

    /**
     * Metadata that allows us to create the partitions needed for {@link ConsumerRebalanceListener}.
     */
    private final ConsumerMetadata metadata;

    /**
     * Logger.
     */
    private final Logger log;

    /**
     * Manager to perform commit requests needed before revoking partitions (if auto-commit is
     * enabled)
     */
    private final CommitRequestManager commitRequestManager;

    /**
     * Local cache of assigned topic IDs and names. Topics are added here when received in a
     * target assignment, as we discover their names in the Metadata cache, and removed when the
     * topic is not in the subscription anymore. The purpose of this cache is to avoid metadata
     * requests in cases where a currently assigned topic is in the target assignment (new
     * partition assigned, or revoked), but it is not present the Metadata cache at that moment.
     * The cache is cleared when the subscription changes ({@link #transitionToJoining()}, the
     * member fails ({@link #transitionToFatal()} or leaves the group ({@link #leaveGroup()}).
     */
    private final Map<Uuid, String> assignedTopicNamesCache;

    /**
     * Topic IDs received in a target assignment for which we haven't found topic names yet.
     * Items are added to this set every time a target assignment is received. Items are removed
     * when metadata is found for the topic. This is where the member collects all assignments
     * received from the broker, even though they may not be ready to reconcile due to missing
     * metadata.
     */
    private final Map<Uuid, SortedSet<Integer>> assignmentUnresolved;

    /**
     * Assignment received for which topic names have been resolved, so it's ready to be
     * reconciled. Items are added to this set when received in a target assignment (if metadata
     * available), or when a metadata update is received. This is where the member keeps all the
     * assignment ready to reconcile, even though the reconciliation might need to wait if there
     * is already another on in process.
     */
    private final SortedSet<TopicIdPartition> assignmentReadyToReconcile;

    /**
     * If there is a reconciliation running (triggering commit, callbacks) for the
     * assignmentReadyToReconcile. This will be true if {@link #reconcile()} has been triggered
     * after receiving a heartbeat response, or a metadata update.
     */
    private boolean reconciliationInProgress;

    /**
     * Epoch the member had when the reconciliation in progress started. This is used to identify if
     * the member has rejoined while it was reconciling an assignment (in which case the result
     * of the reconciliation is not applied.)
     */
    private int memberEpochOnReconciliationStart;

    /**
     * If the member is currently leaving the group after a call to {@link #leaveGroup()}}, this
     * will have a future that will complete when the ongoing leave operation completes
     * (callbacks executed and heartbeat request to leave is sent out). This will be empty is the
     * member is not leaving.
     */
    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty();

    /**
     * True if the member has registered to be notified when the cluster metadata is updated.
     * This is initially false, as the member that is not part of a consumer group does not
     * require metadata updated. This becomes true the first time the member joins on the
     * {@link #transitionToJoining()}
     */
    private boolean isRegisteredForMetadataUpdates;

    /**
     * Registered listeners that will be notified whenever the memberID/epoch gets updated (valid
     * values received from the broker, or values cleared due to member leaving the group, getting
     * fenced or failing).
     */
    private final List<MemberStateListener> stateUpdatesListeners;

    /**
     * Optional client telemetry reporter which sends client telemetry data to the broker. This
     * will be empty if the client telemetry feature is not enabled. This is provided to update
     * the group member id label when the member joins the group.
     */
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    /**
     * Serves as the conduit by which we can report events to the application thread. This is needed as we send
     * {@link ConsumerRebalanceListenerCallbackNeededEvent callbacks} and, if needed,
     * {@link ErrorBackgroundEvent errors} to the application thread.
     */
    private final BackgroundEventHandler backgroundEventHandler;

    public MembershipManagerImpl(String groupId,
                                 Optional<String> groupInstanceId,
                                 int rebalanceTimeoutMs,
                                 Optional<String> serverAssignor,
                                 SubscriptionState subscriptions,
                                 CommitRequestManager commitRequestManager,
                                 ConsumerMetadata metadata,
                                 LogContext logContext,
                                 Optional<ClientTelemetryReporter> clientTelemetryReporter,
                                 BackgroundEventHandler backgroundEventHandler) {
        this.groupId = groupId;
        this.state = MemberState.UNSUBSCRIBED;
        this.serverAssignor = serverAssignor;
        this.groupInstanceId = groupInstanceId;
        this.subscriptions = subscriptions;
        this.commitRequestManager = commitRequestManager;
        this.metadata = metadata;
        this.assignedTopicNamesCache = new HashMap<>();
        this.assignmentUnresolved = new HashMap<>();
        this.assignmentReadyToReconcile = new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR);
        this.currentAssignment = new HashMap<>();
        this.log = logContext.logger(MembershipManagerImpl.class);
        this.stateUpdatesListeners = new ArrayList<>();
        this.clientTelemetryReporter = clientTelemetryReporter;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.backgroundEventHandler = backgroundEventHandler;
    }

    /**
     * Update the member state, setting it to the nextState only if it is a valid transition.
     *
     * @throws IllegalStateException If transitioning from the member {@link #state} to the
     *                               nextState is not allowed as defined in {@link MemberState}.
     */
    private void transitionTo(MemberState nextState) {
        if (!state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
            throw new IllegalStateException(String.format("Invalid state transition from %s to %s",
                    state, nextState));
        }
        log.trace("Member {} with epoch {} transitioned from {} to {}.", memberId, memberEpoch, state, nextState);
        this.state = nextState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String groupId() {
        return groupId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String memberId() {
        return memberId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int memberEpoch() {
        return memberEpoch;
    }

    @Override
    public boolean isStaled() {
        return state == MemberState.STALE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatResponseReceived(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(response.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
        }

        // Update the group member id label in the client telemetry reporter if the member id has
        // changed. Initially the member id is empty, and it is updated when the member joins the
        // group. This is done here to avoid updating the label on every heartbeat response. Also
        // check if the member id is null, as the schema defines it as nullable.
        if (response.memberId() != null && !response.memberId().equals(memberId)) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.updateMetricsLabels(
                Collections.singletonMap(ClientTelemetryProvider.GROUP_MEMBER_ID, response.memberId())));
        }

        this.memberId = response.memberId();
        updateMemberEpoch(response.memberEpoch());

        ConsumerGroupHeartbeatResponseData.Assignment assignment = response.assignment();

        if (assignment != null) {
            if (!state.canHandleNewAssignment()) {
                // New assignment received but member is in a state where it cannot take new
                // assignments (ex. preparing to leave the group)
                log.debug("Ignoring new assignment {} received from server because member is in {} state.",
                    assignment, state);
                return;
            }
            processAssignmentReceived(assignment);

        } else if (allPendingAssignmentsReconciled()) {
            transitionTo(MemberState.STABLE);
        }
    }

    /**
     * This will process the assignment received if it is different from the member's current
     * assignment. If a new assignment is received, this will try to resolve the topic names from
     * metadata, reconcile the resolved assignment, and keep the unresolved to be reconciled when
     * metadata is discovered.
     *
     * @param assignment Assignment received from the broker.
     */
    private void processAssignmentReceived(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        replaceUnresolvedAssignmentWithNewAssignment(assignment);
        if (!assignmentUnresolved.equals(currentAssignment)) {
            // Transition the member to RECONCILING when receiving a new target
            // assignment from the broker, different from the current assignment. Note that the
            // reconciliation might not be triggered just yet because of missing metadata.
            transitionTo(MemberState.RECONCILING);
            assignmentReadyToReconcile.clear();
            resolveMetadataForUnresolvedAssignment();
            reconcile();
        } else {
            // Same assignment received, nothing to reconcile.
            log.debug("Target assignment {} received from the broker is equals to the member " +
                    "current assignment {}. Nothing to reconcile.",
                assignmentUnresolved, currentAssignment);
            // Make sure we transition the member back to STABLE if it was RECONCILING (ex.
            // member was RECONCILING unresolved assignments that were just removed by the
            // broker), or JOINING (member joining received empty assignment).
            if (state == MemberState.RECONCILING || state == MemberState.JOINING) {
                transitionTo(MemberState.STABLE);
            }
        }
    }

    /**
     * Overwrite collection of unresolved topic Ids with the new target assignment. This will
     * effectively achieve the following:
     *
     *    - all topics received in assignment will try to be resolved to find their topic names
     *
     *    - any topic received in a previous assignment that was still unresolved, and that is
     *    not included in the assignment anymore, will be removed from the unresolved collection.
     *    This should be the case when a topic is sent in an assignment, deleted right after, and
     *    removed from the assignment the next time a broker sends one to the member.
     *
     * @param assignment Target assignment received from the broker.
     */
    private void replaceUnresolvedAssignmentWithNewAssignment(
            ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        assignmentUnresolved.clear();
        assignment.topicPartitions().forEach(topicPartitions ->
            assignmentUnresolved.put(topicPartitions.topicId(), new TreeSet<>(topicPartitions.partitions())));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToFenced() {
        if (state == MemberState.PREPARE_LEAVING) {
            log.debug("Member {} with epoch {} got fenced but it is already preparing to leave " +
                    "the group, so it will stop sending heartbeat and won't attempt to rejoin.",
                memberId, memberEpoch);
            // Transition to UNSUBSCRIBED, ensuring that the member (that is not part of the
            // group anymore from the broker point of view) will stop sending heartbeats while it
            // completes the ongoing leaving operation.
            transitionTo(MemberState.UNSUBSCRIBED);
            return;
        }

        if (state == MemberState.LEAVING) {
            log.debug("Member {} with epoch {} got fenced but it is already leaving the group " +
                    "with state {}, so it won't attempt to rejoin.", memberId, memberEpoch, state);
            return;
        }
        if (state == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fenced but it already left the group, so it " +
                    "won't attempt to rejoin.", memberId, memberEpoch);
            return;
        }
        transitionTo(MemberState.FENCED);
        resetEpoch();
        log.debug("Member {} with epoch {} transitioned to {} state. It will release its " +
                "assignment and rejoin the group.", memberId, memberEpoch, MemberState.FENCED);

        // Release assignment
        CompletableFuture<Void> callbackResult = invokeOnPartitionsLostCallback(subscriptions.assignedPartitions());
        callbackResult.whenComplete((result, error) -> {
            if (error != null) {
                log.error("onPartitionsLost callback invocation failed while releasing assignment" +
                        " after member got fenced. Member will rejoin the group anyways.", error);
            }
            updateSubscription(new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR), true);
            transitionToJoining();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToFatal() {
        MemberState previousState = state;
        transitionTo(MemberState.FATAL);
        log.error("Member {} with epoch {} transitioned to {} state", memberId, memberEpoch, MemberState.FATAL);
        notifyEpochChange(Optional.empty(), Optional.empty());

        if (previousState == MemberState.UNSUBSCRIBED) {
            log.debug("Member {} with epoch {} got fatal error from the broker but it already " +
                    "left the group, so onPartitionsLost callback won't be triggered.", memberId, memberEpoch);
            return;
        }

        // Release assignment
        CompletableFuture<Void> callbackResult = invokeOnPartitionsLostCallback(subscriptions.assignedPartitions());
        callbackResult.whenComplete((result, error) -> {
            if (error != null) {
                log.error("onPartitionsLost callback invocation failed while releasing assignment" +
                        "after member failed with fatal error.", error);
            }
            updateSubscription(new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR), true);
        });
    }

    /**
     * {@inheritDoc}
     */
    public void onSubscriptionUpdated() {
        if (state == MemberState.UNSUBSCRIBED) {
            transitionToJoining();
        }
    }

    /**
     * Update a new assignment by setting the assigned partitions in the member subscription.
     *
     * @param assignedPartitions Topic partitions to take as the new subscription assignment
     * @param clearAssignments True if the pending assignments and metadata cache should be cleared
     */
    private void updateSubscription(SortedSet<TopicIdPartition> assignedPartitions,
                                    boolean clearAssignments) {
        Collection<TopicPartition> assignedTopicPartitions = toTopicPartitionSet(assignedPartitions);
        subscriptions.assignFromSubscribed(assignedTopicPartitions);
        // Make assignment effective on the member group manager.
        updateCurrentAssignment(assignedPartitions);
        if (clearAssignments) {
            clearPendingAssignmentsAndLocalNamesCache();
        }
    }

    /**
     * Transition to the {@link MemberState#JOINING} state, indicating that the member will
     * try to join the group on the next heartbeat request. This is expected to be invoked when
     * the user calls the subscribe API, or when the member wants to rejoin after getting fenced.
     * Visible for testing.
     */
    @Override
    public void transitionToJoining() {
        if (state == MemberState.FATAL) {
            log.warn("No action taken to join the group with the updated subscription because " +
                    "the member is in FATAL state");
            return;
        }
        resetEpoch();
        transitionTo(MemberState.JOINING);
        clearPendingAssignmentsAndLocalNamesCache();
        registerForMetadataUpdates();
    }

    /**
     * Register to get notified when the cluster metadata is updated, via the
     * {@link #onUpdate(ClusterResource)}. Register only if the manager is not register already.
     */
    private void registerForMetadataUpdates() {
        if (!isRegisteredForMetadataUpdates) {
            this.metadata.addClusterUpdateListener(this);
            isRegisteredForMetadataUpdates = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> leaveGroup() {
        if (state == MemberState.UNSUBSCRIBED || state == MemberState.FATAL) {
            // Member is not part of the group. No-op and return completed future to avoid
            // unnecessary transitions.
            return CompletableFuture.completedFuture(null);
        }

        if (state == MemberState.PREPARE_LEAVING || state == MemberState.LEAVING) {
            // Member already leaving. No-op and return existing leave group future that will
            // complete when the ongoing leave operation completes.
            return leaveGroupInProgress.get();
        }

        transitionTo(MemberState.PREPARE_LEAVING);
        CompletableFuture<Void> leaveResult = new CompletableFuture<>();
        leaveGroupInProgress = Optional.of(leaveResult);

        CompletableFuture<Void> callbackResult = invokeOnPartitionsRevokedOrLostToReleaseAssignment();
        callbackResult.whenComplete((result, error) -> {
            // Clear the subscription, no matter if the callback execution failed or succeeded.
            updateSubscription(new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR), true);

            // Transition to ensure that a heartbeat request is sent out to effectively leave the
            // group (even in the case where the member had no assignment to release or when the
            // callback execution failed.)
            transitionToSendingLeaveGroup();
        });

        // Return future to indicate that the leave group is done when the callbacks
        // complete, and the transition to send the heartbeat has been made.
        return leaveResult;
    }

    /**
     * Release member assignment by calling the user defined callbacks for onPartitionsRevoked or
     * onPartitionsLost.
     * <ul>
     *     <li>If the member is part of the group (epoch > 0), this will invoke onPartitionsRevoked.
     *     This will be the case when releasing assignment because the member is intentionally
     *     leaving the group (after a call to unsubscribe)</li>
     *
     *     <li>If the member is not part of the group (epoch <=0), this will invoke onPartitionsLost.
     *     This will be the case when releasing assignment after being fenced .</li>
     * </ul>
     *
     * @return Future that will complete when the callback execution completes.
     */
    private CompletableFuture<Void> invokeOnPartitionsRevokedOrLostToReleaseAssignment() {
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        droppedPartitions.addAll(subscriptions.assignedPartitions());

        CompletableFuture<Void> callbackResult;
        if (droppedPartitions.isEmpty()) {
            // No assignment to release.
            callbackResult = CompletableFuture.completedFuture(null);
        } else {
            // Release assignment.
            if (memberEpoch > 0) {
                // Member is part of the group. Invoke onPartitionsRevoked.
                callbackResult = revokePartitions(droppedPartitions);
            } else {
                // Member is not part of the group anymore. Invoke onPartitionsLost.
                callbackResult = invokeOnPartitionsLostCallback(droppedPartitions);
            }
        }
        return callbackResult;
    }

    /**
     * Reset member epoch to the value required for the leave the group heartbeat request, and
     * transition to the {@link MemberState#LEAVING} state so that a heartbeat
     * request is sent out with it.
     * Visible for testing.
     */
    void transitionToSendingLeaveGroup() {
        if (state == MemberState.FATAL) {
            log.warn("Member {} with epoch {} won't send leave group request because it is in " +
                    "FATAL state", memberId, memberEpoch);
            return;
        }
        if (state == MemberState.UNSUBSCRIBED) {
            log.warn("Member {} won't send leave group request because it is already out of the group.",
                memberId);
            return;
        }
        int leaveEpoch = groupInstanceId.isPresent() ?
                ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH :
                ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
        updateMemberEpoch(leaveEpoch);
        currentAssignment = new HashMap<>();
        transitionTo(MemberState.LEAVING);
    }

    /**
     * Call all listeners that are registered to get notified when the member epoch is updated.
     * This also includes the latest member ID in the notification. If the member fails or leaves
     * the group, this will be invoked with empty epoch and member ID.
     */
    private void notifyEpochChange(Optional<Integer> epoch, Optional<String> memberId) {
        stateUpdatesListeners.forEach(stateListener -> stateListener.onMemberEpochUpdated(epoch, memberId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldHeartbeatNow() {
        MemberState state = state();
        return state == MemberState.ACKNOWLEDGING || state == MemberState.LEAVING || state == MemberState.JOINING;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatRequestSent() {
        MemberState state = state();
        if (isStaled()) {
            log.debug("Member {} is staled and is therefore leaving the group.  It will rejoin upon the next poll.", memberEpoch);
            // TODO: Integrate partition revocation/loss callback
            transitionToJoining();
            return;
        }

        if (state == MemberState.ACKNOWLEDGING) {
            if (allPendingAssignmentsReconciled()) {
                transitionTo(MemberState.STABLE);
            } else {
                log.debug("Member {} with epoch {} transitioned to {} after a heartbeat was sent " +
                        "to ack a previous reconciliation. New assignments are ready to " +
                        "be reconciled.", memberId, memberEpoch, MemberState.RECONCILING);
                transitionTo(MemberState.RECONCILING);
            }
        } else if (state == MemberState.LEAVING) {
            transitionToUnsubscribed();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatRequestSkipped() {
        if (state == MemberState.LEAVING) {
            log.debug("Heartbeat for leaving group could not be sent. Member {} with epoch {} will transition to {}.",
                    memberId, memberEpoch, MemberState.UNSUBSCRIBED);
            transitionToUnsubscribed();
        }
    }

    private void transitionToUnsubscribed() {
        transitionTo(MemberState.UNSUBSCRIBED);
        leaveGroupInProgress.get().complete(null);
        leaveGroupInProgress = Optional.empty();
    }

    /**
     * @return True if there are no assignments waiting to be resolved from metadata or reconciled.
     */
    private boolean allPendingAssignmentsReconciled() {
        return assignmentUnresolved.isEmpty() && assignmentReadyToReconcile.isEmpty();
    }

    @Override
    public boolean shouldSkipHeartbeat() {
        MemberState state = state();
        return state == MemberState.UNSUBSCRIBED || state == MemberState.FATAL;
    }

    /**
     * Sets the epoch to the leave group epoch and clears the assignments. The member will rejoin with
     * the existing subscriptions on the next time user polls.
     */
    @Override
    public void transitionToStale() {
        memberEpoch = ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
        // Clear the current assignment and subscribed partitions before member sending the leave group
        updateSubscription(new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR), true);
        transitionTo(MemberState.STALE);
    }

    /**
     * Reconcile the assignment that has been received from the server and for which topic names
     * are resolved, kept in the {@link #assignmentReadyToReconcile}. This will commit if needed,
     * trigger the callbacks and update the subscription state. Note that only one reconciliation
     * can be in progress at a time. If there is already another one in progress when this is
     * triggered, it will be no-op, and the assignment will be reconciled on the next
     * reconciliation loop.
     */
    boolean reconcile() {
        if (reconciliationInProgress) {
            log.debug("Ignoring reconciliation attempt. Another reconciliation is already in progress. Assignment " +
                    assignmentReadyToReconcile + " will be handled in the next reconciliation loop.");
            return false;
        }

        // Make copy of the assignment to reconcile as it could change as new assignments or metadata updates are received
        SortedSet<TopicIdPartition> assignedTopicIdPartitions = new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR);
        assignedTopicIdPartitions.addAll(assignmentReadyToReconcile);

        SortedSet<TopicPartition> ownedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        ownedPartitions.addAll(subscriptions.assignedPartitions());

        // Keep copy of assigned TopicPartitions created from the TopicIdPartitions that are
        // being reconciled. Needed for interactions with the centralized subscription state that
        // does not support topic IDs yet, and for the callbacks.
        SortedSet<TopicPartition> assignedTopicPartitions = toTopicPartitionSet(assignedTopicIdPartitions);

        // Check same assignment. Based on topic names for now, until topic IDs are properly
        // supported in the centralized subscription state object. Note that this check is
        // required to make sure that reconciliation is not triggered if the assignment ready to
        // be reconciled is the same as the current one (even though the member may remain
        // in RECONCILING state if it has some unresolved assignments).
        boolean sameAssignmentReceived = assignedTopicPartitions.equals(ownedPartitions);

        if (sameAssignmentReceived) {
            log.debug("Ignoring reconciliation attempt. Target assignment ready to reconcile {} " +
                    "is equal to the member current assignment {}.", assignedTopicPartitions, ownedPartitions);
            return false;
        }

        markReconciliationInProgress();

        // Partitions to assign (not previously owned)
        SortedSet<TopicPartition> addedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        addedPartitions.addAll(assignedTopicPartitions);
        addedPartitions.removeAll(ownedPartitions);

        // Partitions to revoke
        SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        revokedPartitions.addAll(ownedPartitions);
        revokedPartitions.removeAll(assignedTopicPartitions);

        log.info("Updating assignment with\n" +
                        "\tAssigned partitions:                       {}\n" +
                        "\tCurrent owned partitions:                  {}\n" +
                        "\tAdded partitions (assigned - owned):       {}\n" +
                        "\tRevoked partitions (owned - assigned):     {}\n",
                assignedTopicIdPartitions,
                ownedPartitions,
                addedPartitions,
                revokedPartitions
        );

        // Commit offsets if auto-commit enabled before reconciling a new assignment. Request will
        // be retried until it succeeds, fails with non-retriable error, or timer expires.
        CompletableFuture<Void> commitResult;

        // Issue a commit request that will be retried until it succeeds, fails with a
        // non-retriable error, or the time limit expires. Retry on stale member epoch error, in a
        // best effort to commit the offsets in the case where the epoch might have changed while
        // the current reconciliation is in process. Note this is using the rebalance timeout as
        // it is the limit enforced by the broker to complete the reconciliation process.
        commitResult = commitRequestManager.maybeAutoCommitAllConsumedNow(
            Optional.of((long) rebalanceTimeoutMs),
            true);

        // Execute commit -> onPartitionsRevoked -> onPartitionsAssigned.
        commitResult.whenComplete((commitReqResult, commitReqError) -> {
            if (commitReqError != null) {
                // The call to commit, that includes retry logic for retriable errors, failed to
                // complete within the time boundaries (fatal error or retriable that did not
                // recover). Proceed with the revocation.
                log.error("Auto-commit request before reconciling new assignment failed. " +
                    "Will proceed with the reconciliation anyway.", commitReqError);
            } else {
                log.debug("Auto-commit before reconciling new assignment completed successfully.");
            }

            revokeAndAssign(assignedTopicIdPartitions, revokedPartitions, addedPartitions);
        });

        return true;
    }

    /**
     * Trigger onPartitionsRevoked callbacks if any partitions where revoked. If it succeeds,
     * proceed to trigger the onPartitionsAssigned (even if no new partitions were added), and
     * then complete the reconciliation by updating the assignment and making the appropriate state
     * transition. Note that if any of the 2 callbacks fails, the reconciliation should fail.
     */
    private void revokeAndAssign(SortedSet<TopicIdPartition> assignedTopicIdPartitions,
                                 SortedSet<TopicPartition> revokedPartitions,
                                 SortedSet<TopicPartition> addedPartitions) {
        CompletableFuture<Void> revocationResult;
        if (!revokedPartitions.isEmpty()) {
            revocationResult = revokePartitions(revokedPartitions);
        } else {
            revocationResult = CompletableFuture.completedFuture(null);
        }

        // Future that will complete when the full reconciliation process completes (revocation
        // and assignment, executed sequentially).
        CompletableFuture<Void> reconciliationResult =
            revocationResult.thenCompose(__ -> {
                boolean memberHasRejoined = memberEpochOnReconciliationStart != memberEpoch;
                if (state == MemberState.RECONCILING && !memberHasRejoined) {
                    // Reschedule the auto commit starting from now that member has a new assignment.
                    commitRequestManager.resetAutoCommitTimer();

                    // Apply assignment
                    return assignPartitions(assignedTopicIdPartitions, addedPartitions);
                } else {
                    log.debug("Revocation callback completed but the member already " +
                        "transitioned out of the reconciling state for epoch {} into " +
                        "{} state with epoch {}. Interrupting reconciliation as it's " +
                        "not relevant anymore,", memberEpochOnReconciliationStart, state, memberEpoch);
                    String reason = interruptedReconciliationErrorMessage();
                    CompletableFuture<Void> res = new CompletableFuture<>();
                    res.completeExceptionally(new KafkaException("Interrupting reconciliation" +
                        " after revocation. " + reason));
                    return res;
                }
            });

        reconciliationResult.whenComplete((result, error) -> {
            markReconciliationCompleted();
            if (error != null) {
                // Leaving member in RECONCILING state after callbacks fail. The member
                // won't send the ack, and the expectation is that the broker will kick the
                // member out of the group after the rebalance timeout expires, leading to a
                // RECONCILING -> FENCED transition.
                log.error("Reconciliation failed.", error);
            } else {
                boolean memberHasRejoined = memberEpochOnReconciliationStart != memberEpoch;
                if (state == MemberState.RECONCILING && !memberHasRejoined) {
                    // Make assignment effective on the broker by transitioning to send acknowledge.
                    transitionTo(MemberState.ACKNOWLEDGING);

                    // Indicate that we completed reconciling a subset of the assignment ready to
                    // reconcile (new assignments might have been received or discovered in
                    // metadata).
                    assignmentReadyToReconcile.removeAll(assignedTopicIdPartitions);
                } else {
                    String reason = interruptedReconciliationErrorMessage();
                    log.error("Interrupting reconciliation after partitions assigned callback " +
                        "completed. " + reason);
                }
            }
        });
    }

    // Visible for testing.
    void updateCurrentAssignment(Set<TopicIdPartition> assignedTopicIdPartitions) {
        currentAssignment.clear();
        assignedTopicIdPartitions.forEach(topicIdPartition -> {
            Uuid topicId = topicIdPartition.topicId();
            currentAssignment.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
        });
    }

    /**
     * Build set of {@link TopicPartition} from the given set of {@link TopicIdPartition}.
     */
    private SortedSet<TopicPartition> toTopicPartitionSet(SortedSet<TopicIdPartition> topicIdPartitions) {
        SortedSet<TopicPartition> result = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        topicIdPartitions.forEach(topicIdPartition -> result.add(topicIdPartition.topicPartition()));
        return result;
    }

    /**
     * @return Reason for interrupting a reconciliation progress when callbacks complete.
     */
    private String interruptedReconciliationErrorMessage() {
        String reason;
        if (state != MemberState.RECONCILING) {
            reason = "The member already transitioned out of the reconciling state into " + state;
        } else {
            reason = "The member has re-joined the group.";
        }
        return reason;
    }

    /**
     *  Visible for testing.
     */
    void markReconciliationInProgress() {
        reconciliationInProgress = true;
        memberEpochOnReconciliationStart = memberEpoch;
    }

    /**
     *  Visible for testing.
     */
    void markReconciliationCompleted() {
        reconciliationInProgress = false;
    }

    /**
     * Build set of TopicPartition (topic name and partition id) from the target assignment
     * received from the broker (topic IDs and list of partitions).
     *
     * <p>
     * This will:
     *
     * <ol type="1">
     *     <li>Try to find topic names in the metadata cache</li>
     *     <li>For topics not found in metadata, try to find names in the local topic names cache
     *     (contains topic id and names currently assigned and resolved)</li>
     *     <li>If there are topics that are not in metadata cache or in the local cached
     *     of topic names assigned to this member, request a metadata update, and continue
     *     resolving names as the cache is updated.
     *     </li>
     * </ol>
     */
    private void resolveMetadataForUnresolvedAssignment() {
        assignmentReadyToReconcile.clear();
        // Try to resolve topic names from metadata cache or subscription cache, and move
        // assignments from the "unresolved" collection, to the "readyToReconcile" one.
        Iterator<Map.Entry<Uuid, SortedSet<Integer>>> it = assignmentUnresolved.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Uuid, SortedSet<Integer>> e = it.next();
            Uuid topicId = e.getKey();
            SortedSet<Integer> topicPartitions = e.getValue();

            Optional<String> nameFromMetadata = findTopicNameInGlobalOrLocalCache(topicId);
            nameFromMetadata.ifPresent(resolvedTopicName -> {
                // Name resolved, so assignment is ready for reconciliation.
                addToAssignmentReadyToReconcile(topicId, resolvedTopicName, topicPartitions);
                it.remove();
            });
        }

        if (!assignmentUnresolved.isEmpty()) {
            log.debug("Topic Ids {} received in target assignment were not found in metadata and " +
                    "are not currently assigned. Requesting a metadata update now to resolve " +
                    "topic names.", assignmentUnresolved.keySet());
            metadata.requestUpdate(true);
        }
    }

    /**
     * Look for topic in the global metadata cache. If found, add it to the local cache and
     * return it. If not found, look for it in the local metadata cache. Return empty if not
     * found in any of the two.
     */
    private Optional<String> findTopicNameInGlobalOrLocalCache(Uuid topicId) {
        String nameFromMetadataCache = metadata.topicNames().getOrDefault(topicId, null);
        if (nameFromMetadataCache != null) {
            // Add topic name to local cache, so it can be reused if included in a next target
            // assignment if metadata cache not available.
            assignedTopicNamesCache.put(topicId, nameFromMetadataCache);
            return Optional.of(nameFromMetadataCache);
        } else {
            // Topic ID was not found in metadata. Check if the topic name is in the local
            // cache of topics currently assigned. This will avoid a metadata request in the
            // case where the metadata cache may have been flushed right before the
            // revocation of a previously assigned topic.
            String nameFromSubscriptionCache = assignedTopicNamesCache.getOrDefault(topicId, null);
            return Optional.ofNullable(nameFromSubscriptionCache);
        }
    }

    /**
     * Build a TopicPartition for each of the partitions included in the heartbeat topicPartitions,
     * and using the given topic name. Add the created TopicPartition to the
     * {@link #assignmentReadyToReconcile}.
     */
    private void addToAssignmentReadyToReconcile(Uuid topicId, String topicName, SortedSet<Integer> topicPartitions) {
        topicPartitions.forEach(tp -> {
            TopicIdPartition topicIdPartition = new TopicIdPartition(
                    topicId,
                    new TopicPartition(topicName, tp));
            assignmentReadyToReconcile.add(topicIdPartition);
        });
    }

    /**
     * Revoke partitions. This will:
     * <ul>
     *     <li>Trigger an async commit offsets request if auto-commit enabled.</li>
     *     <li>Invoke the onPartitionsRevoked callback if the user has registered it.</li>
     * </ul>
     *
     * This will wait on the commit request to finish before invoking the callback. If the commit
     * request fails, this will proceed to invoke the user callbacks anyway,
     * returning a future that will complete or fail depending on the callback execution only.
     *
     * @param revokedPartitions Partitions to revoke.
     * @return Future that will complete when the commit request and user callback completes.
     * Visible for testing
     */
    CompletableFuture<Void> revokePartitions(Set<TopicPartition> revokedPartitions) {
        log.info("Revoking previously assigned partitions {}", Utils.join(revokedPartitions, ", "));

        logPausedPartitionsBeingRevoked(revokedPartitions);

        // Mark partitions as pending revocation to stop fetching from the partitions (no new
        // fetches sent out, and no in-flight fetches responses processed).
        markPendingRevocationToPauseFetching(revokedPartitions);

        // Future that will complete when the revocation completes (including offset commit
        // request and user callback execution).
        CompletableFuture<Void> revocationResult = new CompletableFuture<>();

        // At this point we expect to be in a middle of a revocation triggered from RECONCILING
        // or PREPARE_LEAVING, but it could be the case that the member received a fatal error
        // while waiting for the commit to complete. Check if that's the case and abort the
        // revocation.
        if (state == MemberState.FATAL) {
            String errorMsg = String.format("Member %s with epoch %s received a fatal error " +
                "while waiting for a revocation commit to complete. Will abort revocation " +
                "without triggering user callback.", memberId, memberEpoch);
            log.debug(errorMsg);
            revocationResult.completeExceptionally(new KafkaException(errorMsg));
            return revocationResult;
        }

        CompletableFuture<Void> userCallbackResult = invokeOnPartitionsRevokedCallback(revokedPartitions);
        userCallbackResult.whenComplete((callbackResult, callbackError) -> {
            if (callbackError != null) {
                log.error("onPartitionsRevoked callback invocation failed for partitions {}",
                    revokedPartitions, callbackError);
                revocationResult.completeExceptionally(callbackError);
            } else {
                revocationResult.complete(null);
            }

        });
        return revocationResult;
    }


    /**
     * Make new assignment effective and trigger onPartitionsAssigned callback for the partitions
     * added. This will also update the local topic names cache, removing from it all topics that
     * are not assigned to the member anymore.
     *
     * @param assignedPartitions New assignment that will be updated in the member subscription
     *                           state.
     * @param addedPartitions    Partitions contained in the new assignment that were not owned by
     *                           the member before. These will be provided to the
     *                           onPartitionsAssigned callback.
     * @return Future that will complete when the callback execution completes.
     */
    private CompletableFuture<Void> assignPartitions(
            SortedSet<TopicIdPartition> assignedPartitions,
            SortedSet<TopicPartition> addedPartitions) {

        // Make assignment effective on the client by updating the subscription state.
        updateSubscription(assignedPartitions, false);

        // Invoke user call back.
        CompletableFuture<Void> result = invokeOnPartitionsAssignedCallback(addedPartitions);

        // Clear topic names cache, removing topics that are not assigned to the member anymore.
        Set<String> assignedTopics = assignedPartitions.stream().map(TopicIdPartition::topic).collect(Collectors.toSet());
        assignedTopicNamesCache.values().retainAll(assignedTopics);

        return result;
    }

    /**
     * Mark partitions as 'pending revocation', to effectively stop fetching while waiting for
     * the commit offsets request to complete, and ensure the application's position don't get
     * ahead of the committed positions. This mark will ensure that:
     * <ul>
     *     <li>No new fetches will be sent out for the partitions being revoked</li>
     *     <li>Previous in-flight fetch requests that may complete while the partitions are being revoked won't be processed.</li>
     * </ul>
     */
    private void markPendingRevocationToPauseFetching(Set<TopicPartition> partitionsToRevoke) {
        // When asynchronously committing offsets prior to the revocation of a set of partitions, there will be a
        // window of time between when the offset commit is sent and when it returns and revocation completes. It is
        // possible for pending fetches for these partitions to return during this time, which means the application's
        // position may get ahead of the committed position prior to revocation. This can cause duplicate consumption.
        // To prevent this, we mark the partitions as "pending revocation," which stops the Fetcher from sending new
        // fetches or returning data from previous fetches to the user.
        log.debug("Marking partitions pending for revocation: {}", partitionsToRevoke);
        subscriptions.markPendingRevocation(partitionsToRevoke);
    }

    private CompletableFuture<Void> invokeOnPartitionsRevokedCallback(Set<TopicPartition> partitionsRevoked) {
        // This should not trigger the callback if partitionsRevoked is empty, to keep the
        // current behaviour.
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (!partitionsRevoked.isEmpty() && listener.isPresent()) {
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_REVOKED, partitionsRevoked);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> invokeOnPartitionsAssignedCallback(Set<TopicPartition> partitionsAssigned) {
        // This should always trigger the callback, even if partitionsAssigned is empty, to keep
        // the current behaviour.
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (listener.isPresent()) {
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_ASSIGNED, partitionsAssigned);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    // Visible for testing
    CompletableFuture<Void> invokeOnPartitionsLostCallback(Set<TopicPartition> partitionsLost) {
        // This should not trigger the callback if partitionsLost is empty, to keep the current
        // behaviour.
        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (!partitionsLost.isEmpty() && listener.isPresent()) {
            return enqueueConsumerRebalanceListenerCallback(ON_PARTITIONS_LOST, partitionsLost);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Enqueue a {@link ConsumerRebalanceListenerCallbackNeededEvent} to trigger the execution of the
     * appropriate {@link ConsumerRebalanceListener} {@link ConsumerRebalanceListenerMethodName method} on the
     * application thread.
     *
     * <p/>
     *
     * Because the reconciliation process (run in the background thread) will be blocked by the application thread
     * until it completes this, we need to provide a {@link CompletableFuture} by which to remember where we left off.
     *
     * @param methodName Callback method that needs to be executed on the application thread
     * @param partitions Partitions to supply to the callback method
     * @return Future that will be chained within the rest of the reconciliation logic
     */
    private CompletableFuture<Void> enqueueConsumerRebalanceListenerCallback(ConsumerRebalanceListenerMethodName methodName,
                                                                             Set<TopicPartition> partitions) {
        SortedSet<TopicPartition> sortedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        sortedPartitions.addAll(partitions);
        CompletableBackgroundEvent<Void> event = new ConsumerRebalanceListenerCallbackNeededEvent(methodName, sortedPartitions);
        backgroundEventHandler.add(event);
        log.debug("The event to trigger the {} method execution was enqueued successfully", methodName.fullyQualifiedMethodName());
        return event.future();
    }

    @Override
    public void consumerRebalanceListenerCallbackCompleted(ConsumerRebalanceListenerCallbackCompletedEvent event) {
        ConsumerRebalanceListenerMethodName methodName = event.methodName();
        Optional<KafkaException> error = event.error();
        CompletableFuture<Void> future = event.future();

        if (error.isPresent()) {
            Exception e = error.get();
            log.warn(
                "The {} method completed with an error ({}); signaling to continue to the next phase of rebalance",
                methodName.fullyQualifiedMethodName(),
                e.getMessage()
            );

            future.completeExceptionally(e);
        } else {
            log.debug(
                "The {} method completed successfully; signaling to continue to the next phase of rebalance",
                methodName.fullyQualifiedMethodName()
            );

            future.complete(null);
        }
    }

    /**
     * Log partitions being revoked that were already paused, since the pause flag will be
     * effectively lost.
     */
    private void logPausedPartitionsBeingRevoked(Set<TopicPartition> partitionsToRevoke) {
        Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
        revokePausedPartitions.retainAll(partitionsToRevoke);
        if (!revokePausedPartitions.isEmpty()) {
            log.info("The pause flag in partitions [{}] will be removed due to revocation.", Utils.join(revokePausedPartitions, ", "));
        }
    }

    /**
     * Discard assignments received that have not been reconciled yet (waiting for metadata
     * or the next reconciliation loop). Remove all elements from the topic names cache.
     */
    private void clearPendingAssignmentsAndLocalNamesCache() {
        assignmentUnresolved.clear();
        assignmentReadyToReconcile.clear();
        assignedTopicNamesCache.clear();
    }

    private void resetEpoch() {
        updateMemberEpoch(ConsumerGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH);
    }

    private void updateMemberEpoch(int newEpoch) {
        boolean newEpochReceived = this.memberEpoch != newEpoch;
        this.memberEpoch = newEpoch;
        // Simply notify based on epoch change only, given that the member will never receive a
        // new member ID without an epoch (member ID is only assigned when it joins the group).
        if (newEpochReceived) {
            if (memberEpoch > 0) {
                notifyEpochChange(Optional.of(memberEpoch), Optional.ofNullable(memberId));
            } else {
                notifyEpochChange(Optional.empty(), Optional.empty());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberState state() {
        return state;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> serverAssignor() {
        return this.serverAssignor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Uuid, SortedSet<Integer>> currentAssignment() {
        return this.currentAssignment;
    }


    /**
     * @return Set of topic IDs received in a target assignment that have not been reconciled yet
     * because topic names are not in metadata. Visible for testing.
     */
    Set<Uuid> topicsWaitingForMetadata() {
        return Collections.unmodifiableSet(assignmentUnresolved.keySet());
    }

    /**
     * @return Topic partitions received in a target assignment that have been resolved in
     * metadata and are ready to be reconciled. Visible for testing.
     */
    Set<TopicIdPartition> assignmentReadyToReconcile() {
        return Collections.unmodifiableSet(assignmentReadyToReconcile);
    }

    /**
     * @return If there is a reconciliation in process now. Note that reconciliation is triggered
     * by a call to {@link #reconcile()}. Visible for testing.
     */
    boolean reconciliationInProgress() {
        return reconciliationInProgress;
    }

    /**
     * When cluster metadata is updated, try to resolve topic names for topic IDs received in
     * assignment that hasn't been resolved yet.
     * <ul>
     *     <li>Try to find topic names for all unresolved assignments</li>
     *     <li>Add discovered topic names to the local topic names cache</li>
     *     <li>If any topics are resolved, trigger a reconciliation process</li>
     *     <li>If some topics still remain unresolved, request another metadata update</li>
     * </ul>
     */
    @Override
    public void onUpdate(ClusterResource clusterResource) {
        resolveMetadataForUnresolvedAssignment();
        if (!assignmentReadyToReconcile.isEmpty()) {
            reconcile();
        }
    }

    /**
     * Register a new listener that will be invoked whenever the member state changes, or a new
     * member ID or epoch is received.
     *
     * @param listener Listener to invoke.
     */
    @Override
    public void registerStateListener(MemberStateListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("State updates listener cannot be null");
        }
        this.stateUpdatesListeners.add(listener);
    }
}
