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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer.invokeRebalanceCallbacks;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MembershipManagerImplTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int REBALANCE_TIMEOUT = 100;
    private static final int MEMBER_EPOCH = 1;

    private final LogContext logContext = new LogContext();
    private SubscriptionState subscriptionState;
    private ConsumerMetadata metadata;

    private CommitRequestManager commitRequestManager;

    private ConsumerTestBuilder testBuilder;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private BackgroundEventHandler backgroundEventHandler;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder(ConsumerTestBuilder.createDefaultGroupInformation());
        metadata = testBuilder.metadata;
        subscriptionState = testBuilder.subscriptions;
        commitRequestManager = testBuilder.commitRequestManager.orElseThrow(IllegalStateException::new);
        backgroundEventQueue = testBuilder.backgroundEventQueue;
        backgroundEventHandler = testBuilder.backgroundEventHandler;
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null) {
            testBuilder.close();
        }
    }

    private MembershipManagerImpl createMembershipManagerJoiningGroup() {
        MembershipManagerImpl manager = spy(new MembershipManagerImpl(
                GROUP_ID, Optional.empty(), REBALANCE_TIMEOUT, Optional.empty(),
                subscriptionState, commitRequestManager, metadata, logContext, Optional.empty(),
                backgroundEventHandler));
        manager.transitionToJoining();
        return manager;
    }

    private MembershipManagerImpl createMembershipManagerJoiningGroup(String groupInstanceId) {
        MembershipManagerImpl manager = spy(new MembershipManagerImpl(
                GROUP_ID, Optional.ofNullable(groupInstanceId), REBALANCE_TIMEOUT, Optional.empty(),
                subscriptionState, commitRequestManager, metadata, logContext, Optional.empty(),
                backgroundEventHandler));
        manager.transitionToJoining();
        return manager;
    }

    private MembershipManagerImpl createMembershipManagerJoiningGroup(String groupInstanceId,
                                                                      String serverAssignor) {
        MembershipManagerImpl manager = new MembershipManagerImpl(
                GROUP_ID, Optional.ofNullable(groupInstanceId), REBALANCE_TIMEOUT,
                Optional.ofNullable(serverAssignor), subscriptionState, commitRequestManager,
                metadata, logContext, Optional.empty(), backgroundEventHandler);
        manager.transitionToJoining();
        return manager;
    }

    @Test
    public void testMembershipManagerServerAssignor() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(Optional.empty(), membershipManager.serverAssignor());

        membershipManager = createMembershipManagerJoiningGroup("instance1", "Uniform");
        assertEquals(Optional.of("Uniform"), membershipManager.serverAssignor());
    }

    @Test
    public void testMembershipManagerInitSupportsEmptyGroupInstanceId() {
        createMembershipManagerJoiningGroup();
        createMembershipManagerJoiningGroup(null, null);
    }

    @Test
    public void testMembershipManagerRegistersForClusterMetadataUpdatesOnFirstJoin() {
        // First join should register to get metadata updates
        MembershipManagerImpl manager = new MembershipManagerImpl(
                GROUP_ID, Optional.empty(), REBALANCE_TIMEOUT, Optional.empty(),
                subscriptionState, commitRequestManager, metadata, logContext, Optional.empty(),
                backgroundEventHandler);
        manager.transitionToJoining();
        verify(metadata).addClusterUpdateListener(manager);
        clearInvocations(metadata);

        // Following joins should not register again.
        receiveEmptyAssignment(manager);
        mockLeaveGroup();
        manager.leaveGroup();
        assertEquals(MemberState.LEAVING, manager.state());
        manager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, manager.state());
        manager.transitionToJoining();
        verify(metadata, never()).addClusterUpdateListener(manager);
    }

    @Test
    public void testReconcilingWhenReceivingAssignmentFoundInMetadata() {
        MembershipManager membershipManager = mockJoinAndReceiveAssignment(true);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // When the ack is sent the member should go back to STABLE
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testTransitionToReconcilingOnlyIfAssignmentReceived() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithoutAssignment =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(responseWithoutAssignment.data());
        assertNotEquals(MemberState.RECONCILING, membershipManager.state());

        ConsumerGroupHeartbeatResponse responseWithAssignment =
                createConsumerGroupHeartbeatResponse(createAssignment(true));
        membershipManager.onHeartbeatResponseReceived(responseWithAssignment.data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    @Test
    public void testMemberIdAndEpochResetOnFencedMembers() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        mockMemberHasAutoAssignedPartition();

        membershipManager.transitionToFenced();
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(0, membershipManager.memberEpoch());
    }

    @Test
    public void testTransitionToFatal() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());

        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testTransitionToFailedWhenTryingToJoin() {
        MembershipManagerImpl membershipManager = new MembershipManagerImpl(
                GROUP_ID, Optional.empty(), REBALANCE_TIMEOUT, Optional.empty(),
                subscriptionState, commitRequestManager, metadata, logContext, Optional.empty(),
                backgroundEventHandler);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        membershipManager.transitionToJoining();

        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsStable() {
        MembershipManager membershipManager = createMemberInStableState();
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testListenersGetNotifiedOnTransitionsToFatal() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        mockStableMember(membershipManager);
        verify(listener).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), Optional.of(MEMBER_ID));
        clearInvocations(listener);

        // Transition to FAILED before getting member ID/epoch
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        verify(listener).onMemberEpochUpdated(Optional.empty(), Optional.empty());
    }

    @Test
    public void testListenersGetNotifiedOnTransitionsToLeavingGroup() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        mockStableMember(membershipManager);
        verify(listener).onMemberEpochUpdated(Optional.of(MEMBER_EPOCH), Optional.of(MEMBER_ID));
        clearInvocations(listener);

        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        verify(listener).onMemberEpochUpdated(Optional.empty(), Optional.empty());
    }

    @Test
    public void testListenersGetNotifiedOfMemberEpochUpdatesOnlyIfItChanges() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        MemberStateListener listener = mock(MemberStateListener.class);
        membershipManager.registerStateListener(listener);
        int epoch = 5;

        membershipManager.onHeartbeatResponseReceived(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(epoch));

        verify(listener).onMemberEpochUpdated(Optional.of(epoch), Optional.of(MEMBER_ID));
        clearInvocations(listener);

        membershipManager.onHeartbeatResponseReceived(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(epoch));
        verify(listener, never()).onMemberEpochUpdated(any(), any());
    }

    private void mockStableMember(MembershipManagerImpl membershipManager) {
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testFencingWhenStateIsReconciling() {
        MembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    /**
     * Members keep sending heartbeats while preparing to leave, so they could get fenced. This
     * tests ensures that if a member gets fenced while preparing to leave, it will directly
     * transition to UNSUBSCRIBE (no callbacks triggered or rejoin as in regular fencing
     * scenarios), and when the PREPARE_LEAVING completes it remains UNSUBSCRIBED (no last
     * heartbeat sent).
     */
    @Test
    public void testFencingWhenStateIsPrepareLeaving() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent =
            mockPrepareLeavingStuckOnUserCallback(membershipManager, invoker);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());

        // Get fenced while preparing to leave the group. Member should ignore the fence
        // (no callbacks or rejoin) and should transition to UNSUBSCRIBED to
        // effectively stop sending heartbeats.
        clearInvocations(subscriptionState);
        membershipManager.transitionToFenced();
        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // When PREPARE_LEAVING completes, the member should remain UNSUBSCRIBED (no last HB sent
        // because member is already out of the group in the broker).
        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(membershipManager.shouldSkipHeartbeat());
    }

    @Test
    public void testNewAssignmentIgnoredWhenStateIsPrepareLeaving() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent =
            mockPrepareLeavingStuckOnUserCallback(membershipManager, invoker);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());

        // Get new assignment while preparing to leave the group. Member should continue leaving
        // the group, ignoring the new assignment received.
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, "topic1",
            Collections.emptyList());
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertTrue(membershipManager.assignmentReadyToReconcile().isEmpty());
        assertTrue(membershipManager.topicsWaitingForMetadata().isEmpty());
        verify(membershipManager, never()).markReconciliationInProgress();

        // When callback completes member should transition to LEAVING.
        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.LEAVING, membershipManager.state());
    }

    @Test
    public void testFencingWhenStateIsLeaving() {
        MembershipManagerImpl membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Get fenced while leaving. Member should not trigger any callback or try to
        // rejoin, and should continue leaving the group as it was before getting fenced.
        clearInvocations(subscriptionState);
        membershipManager.transitionToFenced();
        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.LEAVING, membershipManager.state());

        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // Last heartbeat sent is expected to fail, leading to a call to transitionToFenced. That
        // should be no-op because the member already left.
        clearInvocations(subscriptionState);
        membershipManager.transitionToFenced();
        testFenceIsNoOp(membershipManager);
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
    }

    @Test
    public void testLeaveGroupEpoch() {
        // Static member should leave the group with epoch -2.
        MembershipManagerImpl membershipManager = createMemberInStableState("instance1");
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertEquals(ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH,
                membershipManager.memberEpoch());

        // Dynamic member should leave the group with epoch -1.
        membershipManager = createMemberInStableState(null);
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertEquals(ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH,
                membershipManager.memberEpoch());
    }

    /**
     * This is the case where a member is stuck reconciling and transitions out of the RECONCILING
     * state (due to failure). When the reconciliation completes it should not be applied because
     * it is not relevant anymore (it should not update the assignment on the member or send ack).
     */
    @Test
    public void testDelayedReconciliationResultDiscardedIfMemberNotInReconcilingStateAnymore() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        List<TopicIdPartition> owned = Collections.singletonList(
            new TopicIdPartition(topicId1, new TopicPartition(topic1, 0)));
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId1, topic1, owned);

        // Reconciliation that does not complete stuck on revocation commit.
        CompletableFuture<Void> commitResult = mockEmptyAssignmentAndRevocationStuckOnCommit(membershipManager);

        // Member received fatal error while reconciling.
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Complete commit request.
        commitResult.complete(null);

        // Member should not update the subscription or send ack when the delayed reconciliation
        // completes.
        verify(subscriptionState, never()).assignFromSubscribed(anySet());
        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting on
     * metadata, commit or callbacks), and it rejoins (due to fence or unsubscribe/subscribe). If
     * the reconciliation of A completes it should not be applied (it should not update the
     * assignment on the member or send ack).
     */
    @Test
    public void testDelayedReconciliationResultDiscardedIfMemberRejoins() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        List<TopicIdPartition> owned = Collections.singletonList(new TopicIdPartition(topicId1,
            new TopicPartition(topic1, 0)));
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId1, topic1, owned);

        // Reconciliation that does not complete stuck on revocation commit.
        CompletableFuture<Void> commitResult =
                mockNewAssignmentAndRevocationStuckOnCommit(membershipManager, topicId1, topic1,
                        Arrays.asList(1, 2), true);
        Set<TopicIdPartition> assignment1 = topicIdPartitionsSet(topicId1, topic1, 1, 2);
        assertEquals(assignment1, membershipManager.assignmentReadyToReconcile());

        // Get fenced and rejoin while still reconciling. Get new assignment to reconcile after
        // rejoining.
        testFencedMemberReleasesAssignmentAndTransitionsToJoining(membershipManager);
        clearInvocations(subscriptionState);

        // Get new assignment A2 after rejoining. This should not trigger a reconciliation just
        // yet because there is another on in progress, but should keep the new assignment ready
        // to be reconciled next.
        Uuid topicId3 = Uuid.randomUuid();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId3, "topic3", owned);
        receiveAssignmentAfterRejoin(topicId3, Collections.singletonList(5), membershipManager);
        verifyReconciliationNotTriggered(membershipManager);
        Set<TopicIdPartition> assignmentAfterRejoin = topicIdPartitionsSet(topicId3, "topic3", 5);
        assertEquals(assignmentAfterRejoin, membershipManager.assignmentReadyToReconcile());

        // Reconciliation completes when the member has already re-joined the group. Should not
        // update the subscription state or send ack.
        commitResult.complete(null);
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
        assertNotEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // Assignment received after rejoining should be ready to reconcile on the next
        // reconciliation loop.
        assertEquals(assignmentAfterRejoin, membershipManager.assignmentReadyToReconcile());
    }

    /**
     * This is the case where a member is stuck reconciling an assignment A (waiting on
     * metadata, commit or callbacks), and the target assignment changes (due to new topics added
     * to metadata, or new assignment received from broker). If the reconciliation of A completes
     * it should be applied (should update the assignment on the member and send ack), and then
     * the reconciliation of assignment B will be processed and applied in the next
     * reconciliation loop.
     */
    @Test
    public void testDelayedReconciliationResultAppliedWhenTargetChangedWithMetadataUpdate() {
        // Member receives and reconciles topic1-partition0
        Uuid topicId1 = Uuid.randomUuid();
        String topic1 = "topic1";
        MembershipManagerImpl membershipManager =
                mockMemberSuccessfullyReceivesAndAcksAssignment(topicId1, topic1, Collections.singletonList(0));
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
        clearInvocations(membershipManager, subscriptionState);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(new TopicPartition(topic1, 0)));

        // New assignment revoking the partitions owned and adding a new one (not in metadata).
        // Reconciliation triggered for topic 1 (stuck on revocation commit) and topic2 waiting
        // for metadata.
        Uuid topicId2 = Uuid.randomUuid();
        String topic2 = "topic2";
        CompletableFuture<Void> commitResult =
                mockNewAssignmentAndRevocationStuckOnCommit(membershipManager, topicId2, topic2,
                        Arrays.asList(1, 2), false);
        verify(metadata).requestUpdate(anyBoolean());
        assertEquals(Collections.singleton(topicId2), membershipManager.topicsWaitingForMetadata());

        // Metadata discovered for topic2 while reconciliation in progress to revoke topic1.
        // Should not trigger a new reconciliation because there is one already in progress.
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId2, topic2), true);
        membershipManager.onUpdate(null);
        assertEquals(Collections.emptySet(), membershipManager.topicsWaitingForMetadata());
        verifyReconciliationNotTriggered(membershipManager);

        // Reconciliation in progress completes. Should be applied revoking topic 1 only. Newly
        // discovered topic2 will be reconciled in the next reconciliation loop.
        commitResult.complete(null);

        // Member should update the subscription and send ack when the delayed reconciliation
        // completes.
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());

        // Pending assignment that was discovered in metadata should be ready to reconcile in the
        // next reconciliation loop.
        Set<TopicIdPartition> topic2Assignment = topicIdPartitionsSet(topicId2, topic2, 1, 2);
        assertEquals(topic2Assignment, membershipManager.assignmentReadyToReconcile());
    }

    @Test
    public void testLeaveGroupWhenStateIsStable() {
        MembershipManager membershipManager = createMemberInStableState();
        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenStateIsReconciling() {
        MembershipManager membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenMemberOwnsAssignment() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        List<TopicIdPartition> assignedPartitions = Arrays.asList(
            new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
            new TopicIdPartition(topicId, new TopicPartition(topicName, 1)));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);

        assertEquals(1, membershipManager.currentAssignment().size());

        testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenMemberAlreadyLeaving() {
        MembershipManager membershipManager = createMemberInStableState();

        // First leave attempt. Should trigger the callbacks and stay LEAVING until
        // callbacks complete and the heartbeat is sent out.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        assertFalse(leaveResult1.isDone());
        assertEquals(MemberState.LEAVING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Second leave attempt while the first one has not completed yet. Should not
        // trigger any callbacks, and return a future that will complete when the ongoing first
        // leave operation completes.
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
        verify(subscriptionState, never()).rebalanceListener();
        assertFalse(leaveResult2.isDone());

        // Complete first leave group operation. Should also complete the second leave group.
        membershipManager.onHeartbeatRequestSent();
        assertTrue(leaveResult1.isDone());
        assertFalse(leaveResult1.isCompletedExceptionally());
        assertTrue(leaveResult2.isDone());
        assertFalse(leaveResult2.isCompletedExceptionally());

        // Subscription should have been updated only once with the first leave group.
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testLeaveGroupWhenMemberAlreadyLeft() {
        MembershipManager membershipManager = createMemberInStableState();

        // Leave group triggered and completed
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult1 = membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leaveResult1.isDone());
        assertFalse(leaveResult1.isCompletedExceptionally());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        clearInvocations(subscriptionState);

        // Call to leave group again, when member already left. Should be no-op (no callbacks,
        // no assignment updated)
        mockLeaveGroup();
        CompletableFuture<Void> leaveResult2 = membershipManager.leaveGroup();
        assertTrue(leaveResult2.isDone());
        assertFalse(leaveResult2.isCompletedExceptionally());
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        verify(subscriptionState, never()).rebalanceListener();
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
    }

    @Test
    public void testFatalFailureWhenStateIsUnjoined() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        assertEquals(MemberState.JOINING, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsStable() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);
    }

    @Test
    public void testFatalFailureWhenStateIsPrepareLeaving() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();
        ConsumerRebalanceListenerCallbackCompletedEvent callbackEvent =
            mockPrepareLeavingStuckOnUserCallback(membershipManager, invoker);
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());

        testStateUpdateOnFatalFailure(membershipManager);

        // When callback completes member should abort the leave operation and remain in FATAL.
        completeCallback(callbackEvent, membershipManager);
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFatalFailureWhenStateIsLeaving() {
        MembershipManagerImpl membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Get fatal failure while waiting to send the heartbeat to leave. Member should
        // transition to FATAL, so the last heartbeat for leaving won't be sent because the member
        // already failed.
        testStateUpdateOnFatalFailure(membershipManager);

        assertEquals(MemberState.FATAL, membershipManager.state());
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testFatalFailureWhenMemberAlreadyLeft() {
        MembershipManagerImpl membershipManager = createMemberInStableState();

        // Start leaving group.
        mockLeaveGroup();
        membershipManager.leaveGroup();
        assertEquals(MemberState.LEAVING, membershipManager.state());

        // Last heartbeat sent.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());

        // Fatal error received in response for the last heartbeat. Member should remain in FATAL
        // state but no callbacks should be triggered because the member already left the group.
        MockRebalanceListener rebalanceListener = new MockRebalanceListener();
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(rebalanceListener));
        membershipManager.transitionToFatal();
        assertEquals(0, rebalanceListener.lostCount);

        assertEquals(MemberState.FATAL, membershipManager.state());
    }

    @Test
    public void testUpdateStateFailsOnResponsesWithErrors() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        // Updating state with a heartbeat response containing errors cannot be performed and
        // should fail.
        ConsumerGroupHeartbeatResponse unknownMemberResponse =
                createConsumerGroupHeartbeatResponseWithError();
        assertThrows(IllegalArgumentException.class,
                () -> membershipManager.onHeartbeatResponseReceived(unknownMemberResponse.data()));
    }

    /**
     * This test should be the case when an assignment is sent to the member, and it cannot find
     * it in metadata (permanently, ex. topic deleted). The member will keep the assignment as
     * waiting for metadata, but the following assignment received from the broker will not
     * contain the deleted topic. The member will discard the assignment that was pending and
     * proceed with the reconciliation of the new assignment.
     */
    @Test
    public void testNewAssignmentReplacesPreviousOneWaitingOnMetadata() {
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsWaitingForMetadata().isEmpty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsWaitingForMetadata().isEmpty());

        // New target assignment received while there is another one waiting to be resolved
        // and reconciled. This assignment does not include the previous one that is waiting
        // for metadata, so the member will discard the topics that were waiting for metadata, and
        // reconcile the new assignment.
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);
        Set<TopicPartition> expectedAssignment = Collections.singleton(new TopicPartition(topicName, 0));
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(subscriptionState).assignFromSubscribed(expectedAssignment);

        // When ack for the reconciled assignment is sent, member should go back to STABLE
        // because the first assignment that was not resolved should have been discarded
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertTrue(membershipManager.topicsWaitingForMetadata().isEmpty());
    }

    /**
     * This test ensures that member goes back to STABLE when the broker sends assignment that
     * removes the unresolved target the client has, without triggering a reconciliation. In this
     * case the member should discard the assignment that was unresolved and go back to STABLE with
     * nothing to reconcile.
     */
    @Test
    public void testNewEmptyAssignmentReplacesPreviousOneWaitingOnMetadata() {
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsWaitingForMetadata().isEmpty());

        // When the ack is sent nothing should change. Member still has nothing to reconcile,
        // only topics waiting for metadata.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsWaitingForMetadata().isEmpty());

        // New target assignment received while there is another one waiting to be resolved
        // and reconciled. This assignment does not include the previous one that is waiting
        // for metadata, so the member will discard the topics that were waiting for metadata, and
        // reconcile the new assignment.
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveEmptyAssignment(membershipManager);
        assertEquals(MemberState.STABLE, membershipManager.state());
        verify(subscriptionState, never()).assignFromSubscribed(any());
    }

    @Test
    public void testNewAssignmentNotInMetadataReplacesPreviousOneWaitingOnMetadata() {
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsWaitingForMetadata().isEmpty());

        // New target assignment (not found in metadata) received while there is another one
        // waiting to be resolved and reconciled. This assignment does not include the previous
        // one that is waiting for metadata, so the member will discard the topics that were
        // waiting for metadata, and just keep the new one as unresolved.
        Uuid topicId = Uuid.randomUuid();
        when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsWaitingForMetadata().isEmpty());
        assertEquals(topicId, membershipManager.topicsWaitingForMetadata().iterator().next());
    }

    /**
     *  This ensures that the client reconciles target assignments as soon as they are discovered
     *  in metadata, without depending on the broker to re-send the assignment.
     */
    @Test
    public void testUnresolvedTargetAssignmentIsReconciledWhenMetadataReceived() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        // Assignment not in metadata. Member cannot reconcile it yet, but keeps it to be
        // reconciled when metadata is discovered.
        Uuid topicId = Uuid.randomUuid();
        receiveAssignment(topicId, Collections.singletonList(1), membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.topicsWaitingForMetadata().isEmpty());

        // Metadata update received, including the missing topic name.
        String topicName = "topic1";
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty());
        membershipManager.onUpdate(null);

        // Assignment should have been reconciled.
        Set<TopicPartition> expectedAssignment = Collections.singleton(new TopicPartition(topicName, 1));
        verify(subscriptionState).assignFromSubscribed(expectedAssignment);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.topicsWaitingForMetadata().isEmpty());
    }

    /**
     * This test should be the case when an assignment is sent to the member, and it cannot find
     * it in metadata (temporarily). If the broker continues to send the assignment to the
     * member, this one should keep it waiting for metadata and continue to request updates.
     */
    @Test
    public void testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved() {
        // Assignment with 2 topics, only 1 found in metadata
        Uuid topic1 = Uuid.randomUuid();
        String topic1Name = "topic1";
        Uuid topic2 = Uuid.randomUuid();
        ConsumerGroupHeartbeatResponseData.Assignment assignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic1)
                                .setPartitions(Collections.singletonList(0)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic2)
                                .setPartitions(Arrays.asList(1, 3))
                ));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topic1, topic1Name));

        // Receive assignment partly in metadata - reconcile+ack what's in metadata, keep the
        // unresolved and request metadata update.
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(true, assignment);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(metadata).requestUpdate(anyBoolean());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsWaitingForMetadata());

        // When the ack is sent the member should go back to RECONCILING because it still has
        // unresolved assignment to be reconciled.
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Target assignment received again with the same unresolved topic. Client should keep it
        // as unresolved.
        clearInvocations(subscriptionState);
        membershipManager.onHeartbeatResponseReceived(createConsumerGroupHeartbeatResponse(assignment).data());
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(Collections.singleton(topic2), membershipManager.topicsWaitingForMetadata());
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenNoPartitionOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        List<TopicIdPartition> assignedPartitions = topicIdPartitions(topicId, topicName, 0, 1);
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
    }

    @Test
    public void testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicIdPartition ownedPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, 0));
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
                Collections.singletonList(ownedPartition));

        // New assignment received, adding partitions 1 and 2 to the previously owned partition 0.
        receiveAssignment(topicId, Arrays.asList(0, 1, 2), membershipManager);

        List<TopicIdPartition> assignedPartitions = new ArrayList<>();
        assignedPartitions.add(ownedPartition);
        assignedPartitions.addAll(topicIdPartitions(topicId, topicName, 1, 2));
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
    }

    @Test
    public void testReconciliationSkippedWhenSameAssignmentReceived() {
        // Member stable, no assignment
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        // Receive assignment different from what the member owns - should reconcile
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());
        List<TopicIdPartition> expectedAssignmentReconciled = topicIdPartitions(topicId, topicName, 0, 1);
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        verifyReconciliationTriggeredAndCompleted(membershipManager, expectedAssignmentReconciled);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        clearInvocations(subscriptionState, membershipManager);

        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());

        // Receive same assignment again - should not trigger reconciliation
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, expectedAssignmentReconciled);
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        // Verify new reconciliation was not triggered
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    @Test
    public void testReconcilePartitionsRevokedNoAutoCommitNoCallbacks() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, Uuid.randomUuid(), "topic1");

        mockRevocationNoCallbacks(false);

        receiveEmptyAssignment(membershipManager);

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, Uuid.randomUuid(), "topic1");

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);

        // Member stays in RECONCILING while the commit request hasn't completed.
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Partitions should be still owned by the member
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        // Complete commit request
        commitResult.complete(null);

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcilePartitionsRevokedWithFailedAutoCommitCompletesRevocationAnyway() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, Uuid.randomUuid(), "topic1");

        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);

        receiveEmptyAssignment(membershipManager);

        // Member stays in RECONCILING while the commit request hasn't completed.
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        // Partitions should be still owned by the member
        verify(subscriptionState, never()).assignFromSubscribed(anyCollection());

        // Complete commit request
        commitResult.completeExceptionally(new KafkaException("Commit request failed with " +
                "non-retriable error"));

        testRevocationOfAllPartitionsCompleted(membershipManager);
    }

    @Test
    public void testReconcileNewPartitionsAssignedAndRevoked() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";
        TopicIdPartition ownedPartition = new TopicIdPartition(topicId,
            new TopicPartition(topicName, 0));
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
            Collections.singletonList(ownedPartition));

        mockRevocationNoCallbacks(false);

        // New assignment received, revoking partition 0, and assigning new partitions 1 and 2.
        receiveAssignment(topicId, Arrays.asList(1, 2), membershipManager);

        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 1, 2), membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        verify(subscriptionState).assignFromSubscribed(anyCollection());
    }

    @Test
    public void testMetadataUpdatesReconcilesUnresolvedAssignments() {
        Uuid topicId = Uuid.randomUuid();

        // Assignment not in metadata
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false, targetAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Should not trigger reconciliation, and request a metadata update.
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsWaitingForMetadata());
        verify(metadata).requestUpdate(anyBoolean());

        String topicName = "topic1";
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), true);

        // When metadata is updated, the member should re-trigger reconciliation
        membershipManager.onUpdate(null);
        List<TopicIdPartition> expectedAssignmentReconciled = topicIdPartitions(topicId, topicName, 0, 1);
        verifyReconciliationTriggeredAndCompleted(membershipManager, expectedAssignmentReconciled);
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.topicsWaitingForMetadata().isEmpty());
    }

    @Test
    public void testMetadataUpdatesRequestsAnotherUpdateIfNeeded() {
        Uuid topicId = Uuid.randomUuid();

        // Assignment not in metadata
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(Arrays.asList(0, 1))));
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(false, targetAssignment);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Should not trigger reconciliation, and request a metadata update.
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsWaitingForMetadata());
        verify(metadata).requestUpdate(anyBoolean());

        // Metadata update received, but still without the unresolved topic in it. Should keep
        // the unresolved and request update again.
        when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        membershipManager.onUpdate(null);
        verifyReconciliationNotTriggered(membershipManager);
        assertEquals(Collections.singleton(topicId), membershipManager.topicsWaitingForMetadata());
        verify(metadata, times(2)).requestUpdate(anyBoolean());
    }

    @Test
    public void testRevokePartitionsUsesTopicNamesLocalCacheWhenMetadataNotAvailable() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic1";

        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName, Collections.emptyList());

        // Member received assignment to reconcile;

        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);

        // Member should complete reconciliation
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        List<Integer> partitions = Arrays.asList(0, 1);
        Set<TopicPartition> assignedPartitions =
            partitions.stream().map(p -> new TopicPartition(topicName, p)).collect(Collectors.toSet());
        Map<Uuid, SortedSet<Integer>> assignedTopicIdPartitions = Collections.singletonMap(topicId,
            new TreeSet<>(partitions));
        assertEquals(assignedTopicIdPartitions, membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        mockAckSent(membershipManager);
        when(subscriptionState.assignedPartitions()).thenReturn(assignedPartitions);

        // Revocation of topic not found in metadata cache
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        mockRevocationNoCallbacks(false);
        mockTopicNameInMetadataCache(Collections.singletonMap(topicId, topicName), false);

        // Revoke one of the 2 partitions
        receiveAssignment(topicId, Collections.singletonList(1), membershipManager);

        // Revocation should complete without requesting any metadata update given that the topic
        // received in target assignment should exist in local topic name cache.
        verify(metadata, never()).requestUpdate(anyBoolean());
        List<TopicIdPartition> remainingAssignment = topicIdPartitions(topicId, topicName, 1);

        testRevocationCompleted(membershipManager, remainingAssignment);
    }

    @Test
    public void testOnSubscriptionUpdatedTransitionsToJoiningOnlyIfNotInGroup() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        verify(membershipManager).transitionToJoining();
        clearInvocations(membershipManager);
        membershipManager.onSubscriptionUpdated();
        verify(membershipManager, never()).transitionToJoining();
    }

    @Test
    public void testListenerCallbacksBasic() {
        // Step 1: set up mocks
        MembershipManagerImpl membershipManager = createMemberInStableState();
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();

        when(subscriptionState.assignedPartitions()).thenReturn(Collections.emptySet());
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));

        // Step 2: put the state machine into the appropriate... state
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Arrays.asList(0, 1), membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(membershipManager.reconciliationInProgress());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 3: assign partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
                topicPartitions(topicName, 0, 1),
                true
        );

        assertFalse(membershipManager.reconciliationInProgress());

        // Step 4: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 0, 1), membershipManager.currentAssignment());

        assertEquals(0, listener.revokedCount());
        assertEquals(1, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        // Step 5: receive an empty assignment, which means we should call revoke
        when(subscriptionState.assignedPartitions()).thenReturn(topicPartitions(topicName, 0, 1));
        receiveEmptyAssignment(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertTrue(membershipManager.reconciliationInProgress());

        // Step 6: revoke partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
                topicPartitions(topicName, 0, 1),
                true
        );
        assertTrue(membershipManager.reconciliationInProgress());

        // Step 7: assign partitions should still be called, even though it's empty
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
                Collections.emptySortedSet(),
                true
        );
        assertFalse(membershipManager.reconciliationInProgress());

        // Step 8: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertFalse(membershipManager.reconciliationInProgress());

        assertEquals(1, listener.revokedCount());
        assertEquals(2, listener.assignedCount());
        assertEquals(0, listener.lostCount());
    }

    @Test
    public void testListenerCallbacksThrowsErrorOnPartitionsRevoked() {
        // Step 1: set up mocks
        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();

        MembershipManagerImpl membershipManager = createMemberInStableState();
        mockOwnedPartition(membershipManager, topicId, topicName);
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener(
                Optional.of(new IllegalArgumentException("Intentional onPartitionsRevoked() error")),
                Optional.empty(),
                Optional.empty()
        );
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());

        // Step 2: put the state machine into the appropriate... state
        receiveEmptyAssignment(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 0), membershipManager.currentAssignment());
        assertTrue(membershipManager.reconciliationInProgress());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 3: revoke partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
                topicPartitions(topicName, 0),
                true
        );

        assertFalse(membershipManager.reconciliationInProgress());
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Step 4: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        assertEquals(1, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());
    }

    @Test
    public void testListenerCallbacksThrowsErrorOnPartitionsAssigned() {
        // Step 1: set up mocks
        MembershipManagerImpl membershipManager = createMemberInStableState();
        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartition(membershipManager, topicId, topicName);
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener(
                Optional.empty(),
                Optional.of(new IllegalArgumentException("Intentional onPartitionsAssigned() error")),
                Optional.empty()
        );
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());

        // Step 2: put the state machine into the appropriate... state
        receiveEmptyAssignment(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertEquals(topicIdPartitionsMap(topicId, 0), membershipManager.currentAssignment());
        assertTrue(membershipManager.reconciliationInProgress());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 3: revoke partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
                topicPartitions("topic1", 0),
                true
        );

        assertTrue(membershipManager.reconciliationInProgress());

        // Step 4: assign partitions
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
                Collections.emptySortedSet(),
                true
        );

        assertFalse(membershipManager.reconciliationInProgress());
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        // Step 5: Send ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        assertEquals(1, listener.revokedCount());
        assertEquals(1, listener.assignedCount());
        assertEquals(0, listener.lostCount());
    }

    @Test
    public void testOnPartitionsLostNoError() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartition(membershipManager, topicId, topicName);
        testOnPartitionsLost(Optional.empty());
    }

    @Test
    public void testOnPartitionsLostError() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        String topicName = "topic1";
        Uuid topicId = Uuid.randomUuid();
        mockOwnedPartition(membershipManager, topicId, topicName);
        testOnPartitionsLost(Optional.of(new KafkaException("Intentional error for test")));
    }

    private void testOnPartitionsLost(Optional<RuntimeException> lostError) {
        // Step 1: set up mocks
        MembershipManagerImpl membershipManager = createMemberInStableState();
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener(
                Optional.empty(),
                Optional.empty(),
                lostError
        );
        ConsumerRebalanceListenerInvoker invoker = consumerRebalanceListenerInvoker();

        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());

        // Step 2: put the state machine into the appropriate... state
        membershipManager.transitionToFenced();
        assertEquals(MemberState.FENCED, membershipManager.state());
        assertEquals(Collections.emptyMap(), membershipManager.currentAssignment());
        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(0, listener.lostCount());

        // Step 3: invoke the callback
        performCallback(
                membershipManager,
                invoker,
                ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST,
                topicPartitions("topic1", 0),
                true
        );

        // Step 4: Receive ack and make sure we're done and our listener was called appropriately
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.JOINING, membershipManager.state());

        assertEquals(0, listener.revokedCount());
        assertEquals(0, listener.assignedCount());
        assertEquals(1, listener.lostCount());
    }

    private ConsumerRebalanceListenerInvoker consumerRebalanceListenerInvoker() {
        ConsumerCoordinatorMetrics coordinatorMetrics = new ConsumerCoordinatorMetrics(
                subscriptionState,
                new Metrics(),
                "test-");
        return new ConsumerRebalanceListenerInvoker(
                new LogContext(),
                subscriptionState,
                new MockTime(1),
                coordinatorMetrics
        );
    }

    private SortedSet<TopicPartition> topicPartitions(String topicName, int... partitions) {
        SortedSet<TopicPartition> topicPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());

        for (int partition : partitions)
            topicPartitions.add(new TopicPartition(topicName, partition));

        return topicPartitions;
    }

    private SortedSet<TopicIdPartition> topicIdPartitionsSet(Uuid topicId, String topicName, int... partitions) {
        SortedSet<TopicIdPartition> topicIdPartitions = new TreeSet<>(new Utils.TopicIdPartitionComparator());

        for (int partition : partitions)
            topicIdPartitions.add(new TopicIdPartition(topicId, new TopicPartition(topicName, partition)));

        return topicIdPartitions;
    }

    private List<TopicIdPartition> topicIdPartitions(Uuid topicId, String topicName, int... partitions) {
        return new ArrayList<>(topicIdPartitionsSet(topicId, topicName, partitions));
    }

    private Map<Uuid, SortedSet<Integer>> topicIdPartitionsMap(Uuid topicId, int... partitions) {
        SortedSet<Integer> topicIdPartitions = new TreeSet<>();

        for (int partition : partitions)
            topicIdPartitions.add(partition);

        return Collections.singletonMap(topicId, topicIdPartitions);
    }

    private ConsumerRebalanceListenerCallbackCompletedEvent performCallback(MembershipManagerImpl membershipManager,
                                 ConsumerRebalanceListenerInvoker invoker,
                                 ConsumerRebalanceListenerMethodName expectedMethodName,
                                 SortedSet<TopicPartition> expectedPartitions,
                                 boolean complete) {
        // We expect only our enqueued event in the background queue.
        assertEquals(1, backgroundEventQueue.size());
        assertNotNull(backgroundEventQueue.peek());
        assertInstanceOf(ConsumerRebalanceListenerCallbackNeededEvent.class, backgroundEventQueue.peek());
        ConsumerRebalanceListenerCallbackNeededEvent neededEvent = (ConsumerRebalanceListenerCallbackNeededEvent) backgroundEventQueue.poll();
        assertNotNull(neededEvent);
        assertEquals(expectedMethodName, neededEvent.methodName());
        assertEquals(expectedPartitions, neededEvent.partitions());

        ConsumerRebalanceListenerCallbackCompletedEvent invokedEvent = invokeRebalanceCallbacks(
                invoker,
                neededEvent.methodName(),
                neededEvent.partitions(),
                neededEvent.future()
        );

        if (complete) {
            completeCallback(invokedEvent, membershipManager);
        }
        return invokedEvent;
    }

    private void completeCallback(ConsumerRebalanceListenerCallbackCompletedEvent callbackCompletedEvent,
                                  MembershipManagerImpl membershipManager) {
        membershipManager.consumerRebalanceListenerCallbackCompleted(callbackCompletedEvent);
    }

    private void testFenceIsNoOp(MembershipManagerImpl membershipManager) {
        assertNotEquals(0, membershipManager.memberEpoch());
        verify(subscriptionState, never()).rebalanceListener();
    }

    @Test
    public void testTransitionToStaled() {
        MembershipManager membershipManager = memberJoinWithAssignment("topic", Uuid.randomUuid());
        membershipManager.transitionToStale();
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testHeartbeatSentOnStaledMember() {
        MembershipManagerImpl membershipManager = createMemberInStableState();
        subscriptionState.subscribe(Collections.singleton("topic"), Optional.empty());
        subscriptionState.assignFromSubscribed(Collections.singleton(new TopicPartition("topic", 0)));
        membershipManager.transitionToStale();
        membershipManager.onHeartbeatRequestSent();
        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(membershipManager.currentAssignment().isEmpty());
        assertTrue(subscriptionState.assignedPartitions().isEmpty());
    }

    @Test
    public void testMemberJoiningTransitionsToStableWhenReceivingEmptyAssignment() {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup(null);
        assertEquals(MemberState.JOINING, membershipManager.state());
        receiveEmptyAssignment(membershipManager);
        assertEquals(MemberState.STABLE, membershipManager.state());
    }

    private MembershipManagerImpl mockMemberSuccessfullyReceivesAndAcksAssignment(
            Uuid topicId, String topicName, List<Integer> partitions) {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        mockOwnedPartitionAndAssignmentReceived(membershipManager, topicId, topicName,
            Collections.emptyList());

        receiveAssignment(topicId, partitions, membershipManager);

        List<TopicIdPartition> assignedPartitions =
            partitions.stream().map(tp -> new TopicIdPartition(topicId,
                new TopicPartition(topicName, tp))).collect(Collectors.toList());
        verifyReconciliationTriggeredAndCompleted(membershipManager, assignedPartitions);
        return membershipManager;
    }

    private CompletableFuture<Void> mockEmptyAssignmentAndRevocationStuckOnCommit(
            MembershipManagerImpl membershipManager) {
        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);
        receiveEmptyAssignment(membershipManager);
        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return commitResult;
    }

    private CompletableFuture<Void> mockNewAssignmentAndRevocationStuckOnCommit(
            MembershipManagerImpl membershipManager, Uuid topicId, String topicName,
            List<Integer> partitions, boolean mockMetadata) {
        CompletableFuture<Void> commitResult = mockRevocationNoCallbacks(true);
        if (mockMetadata) {
            when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        }
        receiveAssignment(topicId, partitions, membershipManager);
        verifyReconciliationTriggered(membershipManager);
        clearInvocations(membershipManager);
        assertEquals(MemberState.RECONCILING, membershipManager.state());

        return commitResult;
    }

    private void verifyReconciliationTriggered(MembershipManagerImpl membershipManager) {
        verify(membershipManager).markReconciliationInProgress();
        assertEquals(MemberState.RECONCILING, membershipManager.state());
    }

    private void verifyReconciliationNotTriggered(MembershipManagerImpl membershipManager) {
        verify(membershipManager, never()).markReconciliationInProgress();
        verify(membershipManager, never()).markReconciliationCompleted();
    }

    private void verifyReconciliationTriggeredAndCompleted(MembershipManagerImpl membershipManager,
                                                           List<TopicIdPartition> expectedAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        verify(membershipManager).markReconciliationInProgress();
        verify(membershipManager).markReconciliationCompleted();
        assertFalse(membershipManager.reconciliationInProgress());

        // Assignment applied
        List<TopicPartition> expectedTopicPartitions = buildTopicPartitions(expectedAssignment);
        verify(subscriptionState).assignFromSubscribed(new HashSet<>(expectedTopicPartitions));
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = assignmentByTopicId(expectedAssignment);
        assertEquals(assignmentByTopicId, membershipManager.currentAssignment());

        verify(commitRequestManager).resetAutoCommitTimer();
    }

    private List<TopicPartition> buildTopicPartitions(List<TopicIdPartition> topicIdPartitions) {
        return topicIdPartitions.stream().map(TopicIdPartition::topicPartition).collect(Collectors.toList());
    }

    private void mockAckSent(MembershipManagerImpl membershipManager) {
        membershipManager.onHeartbeatRequestSent();
    }

    private void mockTopicNameInMetadataCache(Map<Uuid, String> topicNames, boolean isPresent) {
        if (isPresent) {
            when(metadata.topicNames()).thenReturn(topicNames);
        } else {
            when(metadata.topicNames()).thenReturn(Collections.emptyMap());
        }
    }

    private CompletableFuture<Void> mockRevocationNoCallbacks(boolean withAutoCommit) {
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());
        if (withAutoCommit) {
            when(commitRequestManager.autoCommitEnabled()).thenReturn(true);
            CompletableFuture<Void> commitResult = new CompletableFuture<>();
            when(commitRequestManager.maybeAutoCommitAllConsumedNow(any(), anyBoolean())).thenReturn(commitResult);
            return commitResult;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private void mockMemberHasAutoAssignedPartition() {
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());
    }

    private void testRevocationOfAllPartitionsCompleted(MembershipManagerImpl membershipManager) {
        testRevocationCompleted(membershipManager, Collections.emptyList());
    }

    private void testRevocationCompleted(MembershipManagerImpl membershipManager,
                                         List<TopicIdPartition> expectedCurrentAssignment) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = assignmentByTopicId(expectedCurrentAssignment);
        assertEquals(assignmentByTopicId, membershipManager.currentAssignment());
        assertFalse(membershipManager.reconciliationInProgress());

        verify(subscriptionState).markPendingRevocation(anySet());
        List<TopicPartition> expectedTopicPartitionAssignment =
                buildTopicPartitions(expectedCurrentAssignment);
        verify(subscriptionState).assignFromSubscribed(new HashSet<>(expectedTopicPartitionAssignment));
    }

    private Map<Uuid, SortedSet<Integer>> assignmentByTopicId(List<TopicIdPartition> topicIdPartitions) {
        Map<Uuid, SortedSet<Integer>> assignmentByTopicId = new HashMap<>();
        topicIdPartitions.forEach(topicIdPartition -> {
            Uuid topicId = topicIdPartition.topicId();
            assignmentByTopicId.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
        });
        return assignmentByTopicId;
    }

    private void mockOwnedPartitionAndAssignmentReceived(MembershipManagerImpl membershipManager,
                                                         Uuid topicId,
                                                         String topicName,
                                                         List<TopicIdPartition> previouslyOwned) {
        when(subscriptionState.assignedPartitions()).thenReturn(getTopicPartitions(previouslyOwned));
        membershipManager.updateCurrentAssignment(new HashSet<>(previouslyOwned));
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());
    }

    private Set<TopicPartition> getTopicPartitions(List<TopicIdPartition> topicIdPartitions) {
        return topicIdPartitions.stream().map(topicIdPartition ->
                new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()))
            .collect(Collectors.toSet());
    }

    private void mockOwnedPartition(MembershipManagerImpl membershipManager, Uuid topicId, String topic) {
        int partition = 0;
        TopicPartition previouslyOwned = new TopicPartition(topic, partition);
        membershipManager.updateCurrentAssignment(
            Collections.singleton(new TopicIdPartition(topicId, new TopicPartition(topic, partition))));
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(previouslyOwned));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
    }

    private MembershipManagerImpl mockJoinAndReceiveAssignment(boolean expectSubscriptionUpdated) {
        return mockJoinAndReceiveAssignment(expectSubscriptionUpdated, createAssignment(expectSubscriptionUpdated));
    }

    private MembershipManagerImpl mockJoinAndReceiveAssignment(boolean expectSubscriptionUpdated,
                                                               ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup();
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(assignment);
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.empty()).thenReturn(Optional.empty());

        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        if (expectSubscriptionUpdated) {
            verify(subscriptionState).assignFromSubscribed(anyCollection());
        } else {
            verify(subscriptionState, never()).assignFromSubscribed(anyCollection());
        }

        return membershipManager;
    }

    private MembershipManagerImpl createMemberInStableState() {
        return createMemberInStableState(null);
    }

    private MembershipManagerImpl createMemberInStableState(String groupInstanceId) {
        MembershipManagerImpl membershipManager = createMembershipManagerJoiningGroup(groupInstanceId);
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(null);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
        assertEquals(MemberState.STABLE, membershipManager.state());
        return membershipManager;
    }

    private void receiveAssignment(Uuid topicId, List<Integer> partitions, MembershipManager membershipManager) {
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(partitions)));
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
    }

    private void receiveAssignmentAfterRejoin(Uuid topicId, List<Integer> partitions, MembershipManager membershipManager) {
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.singletonList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topicId)
                                .setPartitions(partitions)));
        ConsumerGroupHeartbeatResponse heartbeatResponse =
                createConsumerGroupHeartbeatResponseWithBumpedEpoch(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
    }

    private void receiveEmptyAssignment(MembershipManager membershipManager) {
        // New empty assignment received, revoking owned partition.
        ConsumerGroupHeartbeatResponseData.Assignment targetAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Collections.emptyList());
        ConsumerGroupHeartbeatResponse heartbeatResponse = createConsumerGroupHeartbeatResponse(targetAssignment);
        membershipManager.onHeartbeatResponseReceived(heartbeatResponse.data());
    }

    /**
     * Fenced member should release assignment, reset epoch to 0, keep member ID, and transition
     * to JOINING to rejoin the group.
     */
    private void testFencedMemberReleasesAssignmentAndTransitionsToJoining(MembershipManager membershipManager) {
        mockMemberHasAutoAssignedPartition();

        membershipManager.transitionToFenced();

        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(0, membershipManager.memberEpoch());
        assertEquals(MemberState.JOINING, membershipManager.state());
    }

    /**
     * Member that intentionally leaves the group (via unsubscribe) should release assignment,
     * reset epoch to -1, keep member ID, and transition to {@link MemberState#LEAVING} to send out a
     * heartbeat with the leave epoch. Once the heartbeat request is sent out, the member should
     * transition to {@link MemberState#UNSUBSCRIBED}
     */
    private void testLeaveGroupReleasesAssignmentAndResetsEpochToSendLeaveGroup(MembershipManager membershipManager) {
        mockLeaveGroup();

        CompletableFuture<Void> leaveResult = membershipManager.leaveGroup();

        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertFalse(leaveResult.isDone(), "Leave group result should not complete until the " +
                "heartbeat request to leave is sent out.");

        membershipManager.onHeartbeatRequestSent();

        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertTrue(leaveResult.isDone());
        assertFalse(leaveResult.isCompletedExceptionally());
        assertEquals(MEMBER_ID, membershipManager.memberId());
        assertEquals(-1, membershipManager.memberEpoch());
        assertTrue(membershipManager.currentAssignment().isEmpty());
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
    }

    private void mockLeaveGroup() {
        mockMemberHasAutoAssignedPartition();
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
    }

    private ConsumerRebalanceListenerCallbackCompletedEvent mockPrepareLeavingStuckOnUserCallback(
        MembershipManagerImpl membershipManager,
        ConsumerRebalanceListenerInvoker invoker) {
        String topicName = "topic1";
        TopicPartition ownedPartition = new TopicPartition(topicName, 0);

        // Start leaving group, blocked waiting for callback to complete.
        CounterConsumerRebalanceListener listener = new CounterConsumerRebalanceListener();
        when(subscriptionState.assignedPartitions()).thenReturn(Collections.singleton(ownedPartition));
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        when(subscriptionState.rebalanceListener()).thenReturn(Optional.of(listener));
        doNothing().when(subscriptionState).markPendingRevocation(anySet());
        when(commitRequestManager.autoCommitEnabled()).thenReturn(false);
        membershipManager.leaveGroup();
        return performCallback(
            membershipManager,
            invoker,
            ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED,
            topicPartitions(ownedPartition.topic(), ownedPartition.partition()),
            false
        );
    }

    private void testStateUpdateOnFatalFailure(MembershipManagerImpl membershipManager) {
        String memberId = membershipManager.memberId();
        int lastEpoch = membershipManager.memberEpoch();
        when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true);
        membershipManager.transitionToFatal();
        assertEquals(MemberState.FATAL, membershipManager.state());
        // Should keep its last member id and epoch.
        assertEquals(memberId, membershipManager.memberId());
        assertEquals(lastEpoch, membershipManager.memberEpoch());
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponse(
            ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(MEMBER_EPOCH)
                .setAssignment(assignment));
    }

    /**
     * Create heartbeat response with the given assignment and a bumped epoch (incrementing by 1
     * as default but could be any increment). This will be used to mock when a member
     * receives a heartbeat response to the join request, and the response includes an assignment.
     */
    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponseWithBumpedEpoch(
            ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(MEMBER_EPOCH + 1)
                .setAssignment(assignment));
    }

    private ConsumerGroupHeartbeatResponse createConsumerGroupHeartbeatResponseWithError() {
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                .setMemberId(MEMBER_ID)
                .setMemberEpoch(5));
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createAssignment(boolean mockMetadata) {
        Uuid topic1 = Uuid.randomUuid();
        Uuid topic2 = Uuid.randomUuid();
        if (mockMetadata) {
            Map<Uuid, String> topicNames = new HashMap<>();
            topicNames.put(topic1, "topic1");
            topicNames.put(topic2, "topic2");
            when(metadata.topicNames()).thenReturn(topicNames);
        }
        return new ConsumerGroupHeartbeatResponseData.Assignment()
                .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic1)
                                .setPartitions(Arrays.asList(0, 1, 2)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                                .setTopicId(topic2)
                                .setPartitions(Arrays.asList(3, 4, 5))
                ));
    }

    private MembershipManager memberJoinWithAssignment(String topicName, Uuid topicId) {
        MembershipManagerImpl membershipManager = mockJoinAndReceiveAssignment(true);
        membershipManager.onHeartbeatRequestSent();
        when(metadata.topicNames()).thenReturn(Collections.singletonMap(topicId, topicName));
        receiveAssignment(topicId, Collections.singletonList(0), membershipManager);
        membershipManager.onHeartbeatRequestSent();
        assertFalse(membershipManager.currentAssignment().isEmpty());
        return membershipManager;
    }
}
