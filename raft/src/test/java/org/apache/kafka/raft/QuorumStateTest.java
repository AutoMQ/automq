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
package org.apache.kafka.raft;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.raft.internals.VoterSetTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumStateTest {
    private final int localId = 0;
    private final Uuid localDirectoryId = Uuid.randomUuid();
    private final ReplicaKey localVoterKey = ReplicaKey.of(
        localId,
        Optional.of(localDirectoryId)
    );
    private final int logEndEpoch = 0;
    private final MockQuorumStateStore store = new MockQuorumStateStore();
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;
    private final int fetchTimeoutMs = 10000;
    private final MockableRandom random = new MockableRandom(1L);
    private final BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);

    private QuorumState buildQuorumState(
        OptionalInt localId,
        VoterSet voterSet,
        short kraftVersion
    ) {
        return new QuorumState(
            localId,
            localDirectoryId,
            VoterSetTest.DEFAULT_LISTENER_NAME,
            () -> voterSet,
            () -> kraftVersion,
            electionTimeoutMs,
            fetchTimeoutMs,
            store,
            time,
            new LogContext(),
            random
        );
    }

    private QuorumState initializeEmptyState(VoterSet voters, short kraftVersion) {
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters.voterIds()), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }

    private Set<Integer> persistedVoters(Set<Integer> voters, short kraftVersion) {
        if (kraftVersion == 1) {
            return Collections.emptySet();
        }

        return voters;
    }

    private ReplicaKey persistedVotedKey(ReplicaKey replicaKey, short kraftVersion) {
        if (kraftVersion == 1) {
            return replicaKey;
        }

        return ReplicaKey.of(replicaKey.id(), Optional.empty());
    }

    private VoterSet localStandaloneVoterSet() {
        return VoterSetTest.voterSet(
            Collections.singletonMap(localId, VoterSetTest.voterNode(localVoterKey))
        );
    }

    private VoterSet localWithRemoteVoterSet(IntStream remoteIds, short kraftVersion) {
        boolean withDirectoryId = kraftVersion > 0;
        Map<Integer, VoterSet.VoterNode> voters = VoterSetTest.voterMap(remoteIds, withDirectoryId);
        voters.put(localId, VoterSetTest.voterNode(localVoterKey));

        return VoterSetTest.voterSet(voters);
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializePrimordialEpoch(short kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        assertTrue(state.isUnattached());
        assertEquals(0, state.epoch());
        state.transitionToCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializeAsUnattached(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(ElectionState.withUnknownLeader(epoch, voters.voterIds()), kraftVersion);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(epoch, unattachedState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializeAsFollower(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, node1, voters.voterIds()), kraftVersion);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(epoch, followerState.epoch());
        assertEquals(node1, followerState.leader().id());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializeAsVoted(short kraftVersion) {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Optional.of(Uuid.randomUuid()));

        int epoch = 5;
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, nodeKey1, nodeKey2));
        store.writeElectionState(
            ElectionState.withVotedCandidate(epoch, nodeKey1, voters.voterIds()),
            kraftVersion
        );

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isVoted());
        assertEquals(epoch, state.epoch());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(epoch, votedState.epoch());
        assertEquals(persistedVotedKey(nodeKey1, kraftVersion), votedState.votedKey());

        assertEquals(
            electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializeAsResignedCandidate(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        ElectionState election = ElectionState.withVotedCandidate(
            epoch,
            localVoterKey,
            voters.voterIds()
        );
        store.writeElectionState(election, kraftVersion);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isCandidate());
        assertEquals(epoch, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch, candidateState.epoch());
        assertEquals(
            ElectionState.withVotedCandidate(epoch, localVoterKey, voters.voterIds()),
            candidateState.election()
        );
        assertEquals(Utils.mkSet(node1, node2), candidateState.unrecordedVoters());
        assertEquals(Utils.mkSet(localId), candidateState.grantingVoters());
        assertEquals(Collections.emptySet(), candidateState.rejectingVoters());
        assertEquals(
            electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializeAsResignedLeader(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        ElectionState election = ElectionState.withElectedLeader(epoch, localId, voters.voterIds());
        store.writeElectionState(election, kraftVersion);

        // If we were previously a leader, we will start as resigned in order to ensure
        // a new leader gets elected. This ensures that records are always uniquely
        // defined by epoch and offset even accounting for the loss of unflushed data.

        // The election timeout should be reset after we become a candidate again
        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertFalse(state.isLeader());
        assertEquals(epoch, state.epoch());

        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(epoch, resignedState.epoch());
        assertEquals(election, resignedState.election());
        assertEquals(Utils.mkSet(node1, node2), resignedState.unackedVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            resignedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToCandidate(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        CandidateState candidate1 = state.candidateStateOrThrow();
        candidate1.recordRejectedVote(node2);

        // Check backoff behavior before transitioning
        int backoffMs = 500;
        candidate1.startBackingOff(time.milliseconds(), backoffMs);
        assertTrue(candidate1.isBackingOff());
        assertFalse(candidate1.isBackoffComplete(time.milliseconds()));

        time.sleep(backoffMs - 1);
        assertTrue(candidate1.isBackingOff());
        assertFalse(candidate1.isBackoffComplete(time.milliseconds()));

        time.sleep(1);
        assertTrue(candidate1.isBackingOff());
        assertTrue(candidate1.isBackoffComplete(time.milliseconds()));

        // The election timeout should be reset after we become a candidate again
        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidate2 = state.candidateStateOrThrow();
        assertEquals(2, state.epoch());
        assertEquals(Collections.singleton(localId), candidate2.grantingVoters());
        assertEquals(Collections.emptySet(), candidate2.rejectingVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            candidate2.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToResigned(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, () ->
            state.transitionToResigned(Collections.singletonList(localId)));
        assertTrue(state.isCandidate());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToLeader(short kraftVersion)  {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        state.transitionToLeader(0L, accumulator);
        LeaderState<?> leaderState = state.leaderStateOrThrow();
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch());
        assertEquals(Optional.empty(), leaderState.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToLeaderWithoutGrantedVote(short kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToFollower(short kraftVersion) {
        int otherNodeId = 1;

        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToFollower(5, voters.voterNode(otherNodeId, VoterSetTest.DEFAULT_LISTENER_NAME).get());
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    otherNodeId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToUnattached(short kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withUnknownLeader(
                    5,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToVoted(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToVoted(5, otherNodeKey);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());

        VotedState followerState = state.votedStateOrThrow();
        assertEquals(otherNodeKey, followerState.votedKey());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion))
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCandidateToAnyStateLowerEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );
        assertEquals(6, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    6,
                    persistedVotedKey(localVoterKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testLeaderToLeader(short kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testLeaderToResigned(short kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        state.transitionToResigned(Collections.singletonList(localId));
        assertTrue(state.isResigned());
        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(
            ElectionState.withElectedLeader(1, localId, voters.voterIds()),
            resignedState.election()
        );
        assertEquals(1, resignedState.epoch());
        assertEquals(Collections.emptySet(), resignedState.unackedVoters());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testLeaderToCandidate(short kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testLeaderToFollower(short kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);

        QuorumState state = initializeEmptyState(voters, kraftVersion);

        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToFollower(5, voters.voterNode(otherNodeId, VoterSetTest.DEFAULT_LISTENER_NAME).get());

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    otherNodeId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testLeaderToUnattached(short kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withUnknownLeader(
                    5,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testLeaderToVoted(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        state.transitionToVoted(5, otherNodeKey);

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(otherNodeKey, votedState.votedKey());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testLeaderToAnyStateLowerEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );
        assertEquals(6, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    6,
                    localId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCannotFollowOrVoteForSelf(short kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());
        QuorumState state = initializeEmptyState(voters, kraftVersion);

        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                0,
                voters.voterNode(localId, VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(0, localVoterKey));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToLeaderOrResigned(short kraftVersion) {
        ReplicaKey leaderKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        int epoch = 5;
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, leaderKey));
        store.writeElectionState(
            ElectionState.withVotedCandidate(epoch, leaderKey, voters.voterIds()),
            kraftVersion
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToVotedSameEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToVoted(5, otherNodeKey);

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );

        // Verify election timeout is reset when we vote for a candidate
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToVotedHigherEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, otherNodeKey);

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(8, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    8,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToCandidate(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();

        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(6, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToUnattached(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        long remainingElectionTimeMs = state.unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToFollowerSameEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(
            5,
            voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(
            voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME),
            Optional.of(followerState.leader())
        );
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToFollowerHigherEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(
            8,
            voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(
            voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME),
            Optional.of(followerState.leader())
        );
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnattachedToAnyStateLowerEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withUnknownLeader(
                    5,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToInvalidLeaderOrResigned(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, ReplicaKey.of(node1, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToCandidate(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, ReplicaKey.of(node1, Optional.empty()));

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(6, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToVotedSameEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, ReplicaKey.of(node1, Optional.of(Uuid.randomUuid())));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToVoted(8, ReplicaKey.of(node1, Optional.empty()))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToVoted(8, ReplicaKey.of(node2, Optional.empty()))
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToFollowerSameEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, ReplicaKey.of(node1, Optional.empty()));
        state.transitionToFollower(
            5,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME),
            Optional.of(followerState.leader())
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    node2,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToFollowerHigherEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, ReplicaKey.of(node1, Optional.empty()));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get(),
            followerState.leader()
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    8,
                    node2,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToUnattachedSameEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, ReplicaKey.of(node1, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(5));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToUnattachedHigherEpoch(short kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, ReplicaKey.of(otherNodeId, Optional.empty()));

        long remainingElectionTimeMs = state.votedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testVotedToAnyStateLowerEpoch(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, otherNodeKey);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToFollowerSameEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                8,
                voters.voterNode(node1, VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                8,
                voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME),
            Optional.of(followerState.leader())
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    8,
                    node2,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToFollowerHigherEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        state.transitionToFollower(
            9,
            voters.voterNode(node1, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(9, followerState.epoch());
        assertEquals(
            voters.voterNode(node1, VoterSetTest.DEFAULT_LISTENER_NAME),
            Optional.of(followerState.leader())
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    9,
                    node1,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToLeaderOrResigned(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToCandidate(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(9, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToUnattachedSameEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(8));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToUnattachedHigherEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToUnattached(9);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(9, unattachedState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToVotedSameEpoch(short kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(node2, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToVoted(8, ReplicaKey.of(node1, Optional.empty()))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToVoted(8, ReplicaKey.of(localId, Optional.empty()))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToVoted(8, ReplicaKey.of(node2, Optional.empty()))
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToVotedHigherEpoch(short kraftVersion) {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Optional.of(Uuid.randomUuid()));

        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, nodeKey1, nodeKey2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            voters.voterNode(nodeKey2.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);

        state.transitionToVoted(9, nodeKey1);
        assertTrue(state.isVoted());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(9, votedState.epoch());
        assertEquals(nodeKey1, votedState.votedKey());

        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFollowerToAnyStateLowerEpoch(short kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            5,
            voters.voterNode(otherNodeId, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToVoted(4, ReplicaKey.of(otherNodeId, Optional.empty()))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                voters.voterNode(otherNodeId, VoterSetTest.DEFAULT_LISTENER_NAME).get()
            )
        );
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    otherNodeId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testCanBecomeFollowerOfNonVoter(short kraftVersion) {
        int otherNodeId = 1;
        ReplicaKey nonVoterKey = ReplicaKey.of(2, Optional.of(Uuid.randomUuid()));
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        // Transition to voted
        state.transitionToVoted(4, nonVoterKey);
        assertTrue(state.isVoted());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(4, votedState.epoch());
        assertEquals(nonVoterKey, votedState.votedKey());

        // Transition to follower
        Node nonVoterNode = new Node(nonVoterKey.id(), "non-voter-host", 1234);
        state.transitionToFollower(4, nonVoterNode);
        assertEquals(
            new LeaderAndEpoch(OptionalInt.of(nonVoterKey.id()), 4),
            state.leaderAndEpoch()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testObserverCannotBecomeCandidateOrLeader(short kraftVersion) {
        boolean withDirectoryId = kraftVersion > 0;
        int otherNodeId = 1;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(otherNodeId), withDirectoryId)
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testObserverWithIdCanVote(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToVoted(5, otherNodeKey);
        assertTrue(state.isVoted());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testObserverFollowerToUnattached(short kraftVersion) {
        boolean withDirectoryId = kraftVersion > 0;
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(node1, node2), withDirectoryId)
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToFollower(
            2,
            voters.voterNode(node1, VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        state.transitionToUnattached(3);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(3, unattachedState.epoch());

        // Observers can remain in the unattached state indefinitely until a leader is found
        assertEquals(Long.MAX_VALUE, unattachedState.electionTimeoutMs());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testObserverUnattachedToFollower(short kraftVersion) {
        boolean withDirectoryId = kraftVersion > 0;
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(node1, node2), withDirectoryId)
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToUnattached(2);
        state.transitionToFollower(3, voters.voterNode(node1, VoterSetTest.DEFAULT_LISTENER_NAME).get());
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(3, followerState.epoch());
        assertEquals(
            voters.voterNode(node1, VoterSetTest.DEFAULT_LISTENER_NAME),
            Optional.of(followerState.leader())
        );
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializeWithCorruptedStore(short kraftVersion) {
        QuorumStateStore stateStore = Mockito.mock(QuorumStateStore.class);
        Mockito.doThrow(UncheckedIOException.class).when(stateStore).readElectionState();

        QuorumState state = buildQuorumState(
            OptionalInt.of(localId),
            localStandaloneVoterSet(),
            kraftVersion
        );

        int epoch = 2;
        state.initialize(new OffsetAndEpoch(0L, epoch));
        assertEquals(epoch, state.epoch());
        assertTrue(state.isUnattached());
        assertFalse(state.hasLeader());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testHasRemoteLeader(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        assertFalse(state.hasRemoteLeader());

        state.transitionToCandidate();
        assertFalse(state.hasRemoteLeader());

        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        assertFalse(state.hasRemoteLeader());

        state.transitionToUnattached(state.epoch() + 1);
        assertFalse(state.hasRemoteLeader());

        state.transitionToVoted(state.epoch() + 1, otherNodeKey);
        assertFalse(state.hasRemoteLeader());

        state.transitionToFollower(
            state.epoch() + 1,
            voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );
        assertTrue(state.hasRemoteLeader());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testHighWatermarkRetained(short kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Optional.of(Uuid.randomUuid()));
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToFollower(
            5,
            voters.voterNode(otherNodeKey.id(), VoterSetTest.DEFAULT_LISTENER_NAME).get()
        );

        FollowerState followerState = state.followerStateOrThrow();
        followerState.updateHighWatermark(OptionalLong.of(10L));

        Optional<LogOffsetMetadata> highWatermark = Optional.of(new LogOffsetMetadata(10L));
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToUnattached(6);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToVoted(7, otherNodeKey);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToCandidate();
        assertEquals(highWatermark, state.highWatermark());

        CandidateState candidateState = state.candidateStateOrThrow();
        candidateState.recordGrantedVote(otherNodeKey.id());
        assertTrue(candidateState.isVoteGranted());

        state.transitionToLeader(10L, accumulator);
        assertEquals(Optional.empty(), state.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testInitializeWithEmptyLocalId(short kraftVersion) {
        boolean withDirectoryId = kraftVersion > 0;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(0, 1), withDirectoryId)
        );
        QuorumState state = buildQuorumState(OptionalInt.empty(), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isObserver());
        assertFalse(state.isVoter());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToVoted(1, ReplicaKey.of(1, Optional.empty()))
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));

        state.transitionToFollower(1, voters.voterNode(1, VoterSetTest.DEFAULT_LISTENER_NAME).get());
        assertTrue(state.isFollower());

        state.transitionToUnattached(2);
        assertTrue(state.isUnattached());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testNoLocalIdInitializationFailsIfElectionStateHasVotedCandidate(short kraftVersion) {
        boolean withDirectoryId = kraftVersion > 0;
        int epoch = 5;
        int votedId = 1;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(0, votedId), withDirectoryId)
        );

        store.writeElectionState(
            ElectionState.withVotedCandidate(
                epoch,
                ReplicaKey.of(votedId, Optional.empty()),
                voters.voterIds()
            ),
            kraftVersion
        );

        QuorumState state2 = buildQuorumState(OptionalInt.empty(), voters, kraftVersion);
        assertThrows(IllegalStateException.class, () -> state2.initialize(new OffsetAndEpoch(0, 0)));
    }
}
