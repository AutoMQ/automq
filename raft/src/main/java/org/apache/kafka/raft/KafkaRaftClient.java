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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochResponse;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.RequestManager.ConnectionState;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;
import org.apache.kafka.raft.internals.LogOffset;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;

/**
 * This class implements a Kafkaesque version of the Raft protocol. Leader election
 * is more or less pure Raft, but replication is driven by replica fetching and we use Kafka's
 * log reconciliation protocol to truncate the log to a common point following each leader
 * election.
 *
 * Like Zookeeper, this protocol distinguishes between voters and observers. Voters are
 * the only ones who are eligible to handle protocol requests and they are the only ones
 * who take part in elections. The protocol does not yet support dynamic quorum changes.
 *
 * These are the APIs in this protocol:
 *
 * 1) {@link VoteRequestData}: Sent by valid voters when their election timeout expires and they
 *    become a candidate. This request includes the last offset in the log which electors use
 *    to tell whether or not to grant the vote.
 *
 * 2) {@link BeginQuorumEpochRequestData}: Sent by the leader of an epoch only to valid voters to
 *    assert its leadership of the new epoch. This request will be retried indefinitely for
 *    each voter until it acknowledges the request or a new election occurs.
 *
 *    This is not needed in usual Raft because the leader can use an empty data push
 *    to achieve the same purpose. The Kafka Raft implementation, however, is driven by
 *    fetch requests from followers, so there must be a way to find the new leader after
 *    an election has completed.
 *
 * 3) {@link EndQuorumEpochRequestData}: Sent by the leader of an epoch to valid voters in order to
 *    gracefully resign from the current epoch. This causes remaining voters to immediately
 *    begin a new election.
 *
 * 4) {@link FetchRequestData}: This is the same as the usual Fetch API in Kafka, but we piggyback
 *    some additional metadata on responses (i.e. current leader and epoch). Unlike partition replication,
 *    we also piggyback truncation detection on this API rather than through a separate truncation state.
 *
 */
public class KafkaRaftClient implements RaftClient {
    private final static int RETRY_BACKOFF_BASE_MS = 100;

    private final AtomicReference<GracefulShutdown> shutdown = new AtomicReference<>();
    private final Logger logger;
    private final Time time;
    private final int electionBackoffMaxMs;
    private final int fetchMaxWaitMs;
    private final KafkaRaftMetrics kafkaRaftMetrics;
    private final NetworkChannel channel;
    private final ReplicatedLog log;
    private final QuorumState quorum;
    private final Random random;
    private final RequestManager requestManager;
    private final FuturePurgatory<LogOffset> appendPurgatory;
    private final FuturePurgatory<LogOffset> fetchPurgatory;
    private final BlockingQueue<UnwrittenAppend> unwrittenAppends;

    public KafkaRaftClient(RaftConfig raftConfig,
                           NetworkChannel channel,
                           ReplicatedLog log,
                           QuorumState quorum,
                           Time time,
                           FuturePurgatory<LogOffset> fetchPurgatory,
                           FuturePurgatory<LogOffset> appendPurgatory,
                           LogContext logContext) {
        this(channel,
            log,
            quorum,
            time,
            new Metrics(time),
            fetchPurgatory,
            appendPurgatory,
            raftConfig.quorumVoterConnections(),
            raftConfig.electionBackoffMaxMs(),
            raftConfig.retryBackoffMs(),
            raftConfig.requestTimeoutMs(),
            1000,
            logContext,
            new Random());
    }

    public KafkaRaftClient(NetworkChannel channel,
                           ReplicatedLog log,
                           QuorumState quorum,
                           Time time,
                           Metrics metrics,
                           FuturePurgatory<LogOffset> fetchPurgatory,
                           FuturePurgatory<LogOffset> appendPurgatory,
                           Map<Integer, InetSocketAddress> voterAddresses,
                           int electionBackoffMaxMs,
                           int retryBackoffMs,
                           int requestTimeoutMs,
                           int fetchMaxWaitMs,
                           LogContext logContext,
                           Random random) {
        this.channel = channel;
        this.log = log;
        this.quorum = quorum;
        this.fetchPurgatory = fetchPurgatory;
        this.appendPurgatory = appendPurgatory;
        this.time = time;
        this.electionBackoffMaxMs = electionBackoffMaxMs;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
        this.logger = logContext.logger(KafkaRaftClient.class);
        this.random = random;
        this.requestManager = new RequestManager(voterAddresses.keySet(), retryBackoffMs, requestTimeoutMs, random);
        this.unwrittenAppends = new LinkedBlockingQueue<>();
        this.kafkaRaftMetrics = new KafkaRaftMetrics(metrics, "raft", quorum);
        kafkaRaftMetrics.updateNumUnknownVoterConnections(quorum.remoteVoters().size());

        for (Map.Entry<Integer, InetSocketAddress> voterAddressEntry : voterAddresses.entrySet()) {
            channel.updateEndpoint(voterAddressEntry.getKey(), voterAddressEntry.getValue());
        }
    }

    private void updateFollowerHighWatermark(
        FollowerState state,
        OptionalLong highWatermarkOpt,
        long currentTimeMs
    ) {
        highWatermarkOpt.ifPresent(highWatermark -> {
            long newHighWatermark = Math.min(endOffset().offset, highWatermark);
            if (state.updateHighWatermark(OptionalLong.of(newHighWatermark))) {
                updateHighWatermark(state, currentTimeMs);
            }
        });
    }

    private void updateLeaderEndOffsetAndTimestamp(
        LeaderState state,
        long currentTimeMs
    ) {
        final LogOffsetMetadata endOffsetMetadata = log.endOffset();

        if (state.updateLocalState(currentTimeMs, endOffsetMetadata)) {
            updateHighWatermark(state, currentTimeMs);
        }

        LogOffset endOffset = new LogOffset(endOffsetMetadata.offset, Isolation.UNCOMMITTED);
        fetchPurgatory.maybeComplete(endOffset, currentTimeMs);
    }

    private void updateHighWatermark(
        EpochState state,
        long currentTimeMs
    ) {
        state.highWatermark().ifPresent(highWatermark -> {
            logger.debug("High watermark updated to {}", highWatermark);
            log.updateHighWatermark(highWatermark);

            LogOffset offset = new LogOffset(highWatermark.offset, Isolation.COMMITTED);
            appendPurgatory.maybeComplete(offset, currentTimeMs);
            fetchPurgatory.maybeComplete(offset, currentTimeMs);
        });
    }

    @Override
    public LeaderAndEpoch currentLeaderAndEpoch() {
        return quorum.leaderAndEpoch();
    }

    @Override
    public void initialize() throws IOException {
        quorum.initialize(new OffsetAndEpoch(log.endOffset().offset, log.lastFetchedEpoch()));

        long currentTimeMs = time.milliseconds();
        if (quorum.isLeader()) {
            onBecomeLeader(currentTimeMs);
        } else if (quorum.isCandidate()) {
            onBecomeCandidate(currentTimeMs);
        } else if (quorum.isFollower()) {
            onBecomeFollower(currentTimeMs);
        }

        // When there is only a single voter, become candidate immediately
        if (quorum.isVoter()
            && quorum.remoteVoters().isEmpty()
            && !quorum.isLeader()
            && !quorum.isCandidate()) {
            transitionToCandidate(currentTimeMs);
        }
    }

    private OffsetAndEpoch endOffset() {
        return new OffsetAndEpoch(log.endOffset().offset, log.lastFetchedEpoch());
    }

    private void resetConnections() {
        requestManager.resetAll();
    }

    private void onBecomeLeader(long currentTimeMs) {
        LeaderState state = quorum.leaderStateOrThrow();

        log.initializeLeaderEpoch(quorum.epoch());

        // The high watermark can only be advanced once we have written a record
        // from the new leader's epoch. Hence we write a control message immediately
        // to ensure there is no delay committing pending data.
        appendLeaderChangeMessage(state, currentTimeMs);
        updateLeaderEndOffsetAndTimestamp(state, currentTimeMs);

        resetConnections();

        kafkaRaftMetrics.maybeUpdateElectionLatency(currentTimeMs);
    }

    private void appendLeaderChangeMessage(LeaderState state, long currentTimeMs) {
        List<Voter> voters = state.followers().stream()
            .map(follower -> new Voter().setVoterId(follower))
            .collect(Collectors.toList());

        LeaderChangeMessage leaderChangeMessage = new LeaderChangeMessage()
            .setLeaderId(state.election().leaderId())
            .setVoters(voters);

        MemoryRecords records = MemoryRecords.withLeaderChangeMessage(
            currentTimeMs, quorum.epoch(), leaderChangeMessage);

        appendAsLeader(records);
        flushLeaderLog(state, currentTimeMs);
    }

    private void flushLeaderLog(LeaderState state, long currentTimeMs) {
        log.flush();
        updateLeaderEndOffsetAndTimestamp(state, currentTimeMs);
    }

    private boolean maybeTransitionToLeader(CandidateState state, long currentTimeMs) throws IOException {
        if (state.isVoteGranted()) {
            long endOffset = log.endOffset().offset;
            quorum.transitionToLeader(endOffset);
            onBecomeLeader(currentTimeMs);
            return true;
        } else {
            return false;
        }
    }

    private void onBecomeCandidate(long currentTimeMs) throws IOException {
        CandidateState state = quorum.candidateStateOrThrow();
        if (!maybeTransitionToLeader(state, currentTimeMs)) {
            resetConnections();
            kafkaRaftMetrics.updateElectionStartMs(currentTimeMs);
        }
    }

    private void transitionToCandidate(long currentTimeMs) throws IOException {
        quorum.transitionToCandidate();
        onBecomeCandidate(currentTimeMs);
    }

    private void transitionToUnattached(int epoch) throws IOException {
        quorum.transitionToUnattached(epoch);
        resetConnections();
    }

    private void transitionToVoted(int candidateId, int epoch) throws IOException {
        quorum.transitionToVoted(epoch, candidateId);
        resetConnections();
    }

    private void onBecomeFollower(long currentTimeMs) {
        kafkaRaftMetrics.maybeUpdateElectionLatency(currentTimeMs);

        resetConnections();

        // After becoming a follower, we need to complete all pending fetches so that
        // they can be resent to the leader without waiting for their expiration
        fetchPurgatory.completeAllExceptionally(new NotLeaderOrFollowerException(
            "Cannot process the fetch request because the node is no longer the leader."));

        // Clearing the append purgatory should complete all future exceptionally since this node is no longer the leader
        appendPurgatory.completeAllExceptionally(new NotLeaderOrFollowerException(
            "Failed to receive sufficient acknowledgments for this append before leader change."));

        failPendingAppends(new NotLeaderOrFollowerException(
            "Append refused since this node is no longer the leader"));
    }

    private void transitionToFollower(
        int epoch,
        int leaderId,
        long currentTimeMs
    ) throws IOException {
        quorum.transitionToFollower(epoch, leaderId);
        onBecomeFollower(currentTimeMs);
    }

    private VoteResponseData buildVoteResponse(Errors partitionLevelError, boolean voteGranted) {
        return VoteResponse.singletonResponse(
            Errors.NONE,
            log.topicPartition(),
            partitionLevelError,
            quorum.epoch(),
            quorum.leaderIdOrNil(),
            voteGranted);
    }

    /**
     * Handle a Vote request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#INVALID_REQUEST} if the last epoch or offset are invalid
     */
    private VoteResponseData handleVoteRequest(
        RaftRequest.Inbound requestMetadata
    ) throws IOException {
        VoteRequestData request = (VoteRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return new VoteResponseData().setErrorCode(Errors.INVALID_REQUEST.code());
        }

        VoteRequestData.PartitionData partitionRequest =
            request.topics().get(0).partitions().get(0);

        int candidateId = partitionRequest.candidateId();
        int candidateEpoch = partitionRequest.candidateEpoch();

        int lastEpoch = partitionRequest.lastOffsetEpoch();
        long lastEpochEndOffset = partitionRequest.lastOffset();
        if (lastEpochEndOffset < 0 || lastEpoch < 0 || lastEpoch >= candidateEpoch) {
            return buildVoteResponse(Errors.INVALID_REQUEST, false);
        }

        Optional<Errors> errorOpt = validateVoterOnlyRequest(candidateId, candidateEpoch);
        if (errorOpt.isPresent()) {
            return buildVoteResponse(errorOpt.get(), false);
        }

        if (candidateEpoch > quorum.epoch()) {
            transitionToUnattached(candidateEpoch);
        }

        final boolean voteGranted;
        if (quorum.isLeader()) {
            logger.debug("Ignoring vote request {} with epoch {} since we are already leader on that epoch",
                    request, candidateEpoch);
            voteGranted = false;
        } else if (quorum.isCandidate()) {
            logger.debug("Ignoring vote request {} with epoch {} since we are already candidate on that epoch",
                    request, candidateEpoch);
            voteGranted = false;
        } else if (quorum.isFollower()) {
            FollowerState state = quorum.followerStateOrThrow();
            logger.debug("Rejecting vote request {} with epoch {} since we already have a leader {} on that epoch",
                request, candidateEpoch, state.leaderId());
            voteGranted = false;
        } else if (quorum.isVoted()) {
            VotedState state = quorum.votedStateOrThrow();
            voteGranted = state.votedId() == candidateId;

            if (!voteGranted) {
                logger.debug("Rejecting vote request {} with epoch {} since we already have voted for " +
                    "another candidate {} on that epoch", request, candidateEpoch, state.votedId());
            }
        } else if (quorum.isUnattached()) {
            OffsetAndEpoch lastEpochEndOffsetAndEpoch = new OffsetAndEpoch(lastEpochEndOffset, lastEpoch);
            voteGranted = lastEpochEndOffsetAndEpoch.compareTo(endOffset()) >= 0;

            if (voteGranted) {
                transitionToVoted(candidateId, candidateEpoch);
            }
        } else {
            throw new IllegalStateException("Unexpected quorum state " + quorum);
        }

        logger.info("Vote request {} is {}", request, voteGranted ? "granted" : "rejected");
        return buildVoteResponse(Errors.NONE, voteGranted);
    }

    private boolean handleVoteResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        int remoteNodeId = responseMetadata.sourceId();
        VoteResponseData response = (VoteResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        VoteResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        Errors error = Errors.forCode(partitionResponse.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(partitionResponse.leaderId());
        int responseEpoch = partitionResponse.leaderEpoch();

        Optional<Boolean> handled = maybeHandleCommonResponse(
            error, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        } else if (error == Errors.NONE) {
            if (quorum.isLeader()) {
                logger.debug("Ignoring vote response {} since we already became leader for epoch {}",
                    partitionResponse, quorum.epoch());
            } else if (quorum.isCandidate()) {
                CandidateState state = quorum.candidateStateOrThrow();
                if (partitionResponse.voteGranted()) {
                    state.recordGrantedVote(remoteNodeId);
                    maybeTransitionToLeader(state, currentTimeMs);
                } else {
                    state.recordRejectedVote(remoteNodeId);

                    // If our vote is rejected, we go immediately to the random backoff. This
                    // ensures that we are not stuck waiting for the election timeout when the
                    // vote has become gridlocked.
                    if (state.isVoteRejected() && !state.isBackingOff()) {
                        logger.info("Insufficient remaining votes to become leader (rejected by {}). " +
                            "We will backoff before retrying election again", state.rejectingVoters());

                        state.startBackingOff(
                            currentTimeMs,
                            binaryExponentialElectionBackoffMs(state.retries())
                        );
                    }

                }
            } else {
                logger.debug("Ignoring vote response {} since we are no longer a candidate in epoch {}",
                    partitionResponse, quorum.epoch());
            }
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private int binaryExponentialElectionBackoffMs(int retries) {
        if (retries <= 0) {
            throw new IllegalArgumentException("Retries " + retries + " should be larger than zero");
        }
        // upper limit exponential co-efficients at 20 to avoid overflow
        return Math.min(RETRY_BACKOFF_BASE_MS * random.nextInt(2 << Math.min(20, retries - 1)), electionBackoffMaxMs);
    }

    private int strictExponentialElectionBackoffMs(int positionInSuccessors, int totalNumSuccessors) {
        if (positionInSuccessors <= 0 || positionInSuccessors >= totalNumSuccessors) {
            throw new IllegalArgumentException("Position " + positionInSuccessors + " should be larger than zero" +
                    " and smaller than total number of successors " + totalNumSuccessors);
        }

        int retryBackOffBaseMs = electionBackoffMaxMs >> (totalNumSuccessors - 1);
        return Math.min(electionBackoffMaxMs, retryBackOffBaseMs << (positionInSuccessors - 1));
    }

    private BeginQuorumEpochResponseData buildBeginQuorumEpochResponse(Errors partitionLevelError) {
        return BeginQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            log.topicPartition(),
            partitionLevelError,
            quorum.epoch(),
            quorum.leaderIdOrNil());
    }

    /**
     * Handle a BeginEpoch request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private BeginQuorumEpochResponseData handleBeginQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) throws IOException {
        BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return new BeginQuorumEpochResponseData().setErrorCode(Errors.INVALID_REQUEST.code());
        }

        BeginQuorumEpochRequestData.PartitionData partitionRequest =
            request.topics().get(0).partitions().get(0);

        int requestLeaderId = partitionRequest.leaderId();
        int requestEpoch = partitionRequest.leaderEpoch();

        Optional<Errors> errorOpt = validateVoterOnlyRequest(requestLeaderId, requestEpoch);
        if (errorOpt.isPresent()) {
            return buildBeginQuorumEpochResponse(errorOpt.get());
        }

        maybeTransition(OptionalInt.of(requestLeaderId), requestEpoch, currentTimeMs);
        return buildBeginQuorumEpochResponse(Errors.NONE);
    }

    private boolean handleBeginQuorumEpochResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        int remoteNodeId = responseMetadata.sourceId();
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        BeginQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        Errors partitionError = Errors.forCode(partitionResponse.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(partitionResponse.leaderId());
        int responseEpoch = partitionResponse.leaderEpoch();

        Optional<Boolean> handled = maybeHandleCommonResponse(
            partitionError, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        } else if (partitionError == Errors.NONE) {
            if (quorum.isLeader()) {
                LeaderState state = quorum.leaderStateOrThrow();
                state.addEndorsementFrom(remoteNodeId);
            } else {
                logger.debug("Ignoring BeginQuorumEpoch response {} since " +
                    "this node is not the leader anymore", response);
            }
            return true;
        } else {
            return handleUnexpectedError(partitionError, responseMetadata);
        }
    }

    private EndQuorumEpochResponseData buildEndQuorumEpochResponse(Errors partitionLevelError) {
        return EndQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            log.topicPartition(),
            partitionLevelError,
            quorum.epoch(),
            quorum.leaderIdOrNil());
    }

    /**
     * Handle an EndEpoch request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private EndQuorumEpochResponseData handleEndQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) throws IOException {
        EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return new EndQuorumEpochResponseData().setErrorCode(Errors.INVALID_REQUEST.code());
        }

        EndQuorumEpochRequestData.PartitionData partitionRequest =
            request.topics().get(0).partitions().get(0);

        int requestEpoch = partitionRequest.leaderEpoch();
        int requestReplicaId = partitionRequest.replicaId();

        Optional<Errors> errorOpt = validateVoterOnlyRequest(requestReplicaId, requestEpoch);
        if (errorOpt.isPresent()) {
            return buildEndQuorumEpochResponse(errorOpt.get());
        }

        OptionalInt requestLeaderId = optionalLeaderId(partitionRequest.leaderId());
        maybeTransition(requestLeaderId, requestEpoch, currentTimeMs);

        if (quorum.isFollower()) {
            FollowerState state = quorum.followerStateOrThrow();
            if (state.leaderId() == requestReplicaId) {
                List<Integer> preferredSuccessors = partitionRequest.preferredSuccessors();
                if (!preferredSuccessors.contains(quorum.localId)) {
                    return buildEndQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET);
                }
                long electionBackoffMs = endEpochElectionBackoff(preferredSuccessors);
                state.overrideFetchTimeout(currentTimeMs, electionBackoffMs);
            }
        } else if (quorum.isVoted()) {
            VotedState state = quorum.votedStateOrThrow();
            if (state.votedId() == requestReplicaId) {
                long electionBackoffMs = binaryExponentialElectionBackoffMs(1);
                state.overrideElectionTimeout(currentTimeMs, electionBackoffMs);
            }
        }
        return buildEndQuorumEpochResponse(Errors.NONE);
    }

    private long endEpochElectionBackoff(List<Integer> preferredSuccessors) {
        // Based on the priority inside the preferred successors, choose the corresponding delayed
        // election backoff time based on strict exponential mechanism so that the most up-to-date
        // voter has a higher chance to be elected. If the node's priority is highest, become
        // candidate immediately instead of waiting for next poll.
        int position = preferredSuccessors.indexOf(quorum.localId);
        if (position == 0) {
            return 0;
        } else {
            return strictExponentialElectionBackoffMs(position, preferredSuccessors.size());
        }
    }

    private boolean handleEndQuorumEpochResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        EndQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        Errors partitionError = Errors.forCode(partitionResponse.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(partitionResponse.leaderId());
        int responseEpoch = partitionResponse.leaderEpoch();

        Optional<Boolean> handled = maybeHandleCommonResponse(
            partitionError, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        } else if (partitionError == Errors.NONE) {
            return true;
        } else {
            return handleUnexpectedError(partitionError, responseMetadata);
        }
    }

    private FetchResponseData buildFetchResponse(
        Errors error,
        Records records,
        Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(log.topicPartition(), Errors.NONE, partitionData -> {
            partitionData
                .setRecordSet(records)
                .setErrorCode(error.code())
                .setLogStartOffset(log.startOffset())
                .setHighWatermark(highWatermark
                    .map(offsetMetadata -> offsetMetadata.offset)
                    .orElse(-1L));

            partitionData.currentLeader()
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());

            divergingEpoch.ifPresent(partitionData::setDivergingEpoch);
        });
    }

    private FetchResponseData buildEmptyFetchResponse(
        Errors error,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return buildFetchResponse(error, MemoryRecords.EMPTY, Optional.empty(), highWatermark);
    }

    /**
     * Handle a Fetch request. The fetch offset and last fetched epoch are always
     * validated against the current log. In the case that they do not match, the response will
     * indicate the diverging offset/epoch. A follower is expected to truncate its log in this
     * case and resend the fetch.
     *
     * This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INVALID_REQUEST} if the request epoch is larger than the leader's current epoch
     *     or if either the fetch offset or the last fetched epoch is invalid
     */
    private CompletableFuture<FetchResponseData> handleFetchRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) {
        FetchRequestData request = (FetchRequestData) requestMetadata.data;

        if (!hasValidTopicPartition(request, log.topicPartition())) {
            // Until we support multi-raft, we treat topic partition mismatches as invalid requests
            return completedFuture(new FetchResponseData().setErrorCode(Errors.INVALID_REQUEST.code()));
        }

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        if (request.maxWaitMs() < 0
            || fetchPartition.fetchOffset() < 0
            || fetchPartition.lastFetchedEpoch() < 0
            || fetchPartition.lastFetchedEpoch() > fetchPartition.currentLeaderEpoch()) {
            return completedFuture(buildEmptyFetchResponse(
                Errors.INVALID_REQUEST, Optional.empty()));
        }

        FetchResponseData response = tryCompleteFetchRequest(request.replicaId(), fetchPartition, currentTimeMs);
        FetchResponseData.FetchablePartitionResponse partitionResponse =
            response.responses().get(0).partitionResponses().get(0);

        if (partitionResponse.errorCode() != Errors.NONE.code()
            || partitionResponse.recordSet().sizeInBytes() > 0
            || request.maxWaitMs() == 0) {
            return completedFuture(response);
        }

        CompletableFuture<Long> future = fetchPurgatory.await(
            LogOffset.awaitUncommitted(fetchPartition.fetchOffset()),
            request.maxWaitMs());

        return future.handle((completionTimeMs, exception) -> {
            if (exception != null) {
                Throwable cause = exception instanceof ExecutionException ?
                    exception.getCause() : exception;

                // If the fetch timed out in purgatory, it means no new data is available,
                // and we will complete the fetch successfully. Otherwise, if there was
                // any other error, we need to return it.
                Errors error = Errors.forException(cause);
                if (error != Errors.REQUEST_TIMED_OUT) {
                    logger.debug("Failed to handle fetch from {} at {} due to {}",
                        request.replicaId(), fetchPartition.fetchOffset(), error);
                    return buildEmptyFetchResponse(error, Optional.empty());
                }
            }

            logger.trace("Completing delayed fetch from {} starting at offset {} at {}",
                request.replicaId(), fetchPartition.fetchOffset(), completionTimeMs);

            try {
                return tryCompleteFetchRequest(request.replicaId(), fetchPartition, time.milliseconds());
            } catch (Exception e) {
                logger.error("Caught unexpected error in fetch completion of request {}", request, e);
                return buildEmptyFetchResponse(Errors.UNKNOWN_SERVER_ERROR, Optional.empty());
            }
        });
    }

    @Override
    public CompletableFuture<Records> read(
        OffsetAndEpoch fetchOffsetAndEpoch,
        Isolation isolation,
        long maxWaitTimeMs
    ) {
        CompletableFuture<Records> future = new CompletableFuture<>();
        tryCompleteRead(future, fetchOffsetAndEpoch, isolation, maxWaitTimeMs <= 0);

        if (!future.isDone()) {
            CompletableFuture<Long> completion = fetchPurgatory.await(
                LogOffset.await(fetchOffsetAndEpoch.offset, isolation),
                maxWaitTimeMs);
            completion.whenComplete((completeTimeMs, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    tryCompleteRead(future, fetchOffsetAndEpoch, isolation, true);
                }
            });
        }
        return future;
    }

    private void tryCompleteRead(
        CompletableFuture<Records> future,
        OffsetAndEpoch fetchOffsetAndEpoch,
        Isolation isolation,
        boolean completeIfEmpty
    ) {
        Optional<OffsetAndEpoch> nextOffsetOpt = validateFetchOffsetAndEpoch(
            fetchOffsetAndEpoch.offset, fetchOffsetAndEpoch.epoch);

        if (nextOffsetOpt.isPresent()) {
            future.completeExceptionally(new LogTruncationException("Failed to read data from " + fetchOffsetAndEpoch
                + " since the log has been truncated. The diverging offset is " + nextOffsetOpt.get()));
        } else {
            try {
                LogFetchInfo info = log.read(fetchOffsetAndEpoch.offset, isolation);
                Records records = info.records;

                if (records.sizeInBytes() > 0 || completeIfEmpty) {
                    future.complete(records);
                }
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }
    }

    private FetchResponseData tryCompleteFetchRequest(
        int replicaId,
        FetchRequestData.FetchPartition request,
        long currentTimeMs
    ) {
        Optional<Errors> errorOpt = validateLeaderOnlyRequest(request.currentLeaderEpoch());
        if (errorOpt.isPresent()) {
            return buildEmptyFetchResponse(errorOpt.get(), Optional.empty());
        }

        long fetchOffset = request.fetchOffset();
        int lastFetchedEpoch = request.lastFetchedEpoch();
        LeaderState state = quorum.leaderStateOrThrow();
        Optional<OffsetAndEpoch> divergingEpochOpt = validateFetchOffsetAndEpoch(fetchOffset, lastFetchedEpoch);

        if (divergingEpochOpt.isPresent()) {
            Optional<FetchResponseData.EpochEndOffset> divergingEpoch =
                divergingEpochOpt.map(offsetAndEpoch -> new FetchResponseData.EpochEndOffset()
                    .setEpoch(offsetAndEpoch.epoch)
                    .setEndOffset(offsetAndEpoch.offset));
            return buildFetchResponse(Errors.NONE, MemoryRecords.EMPTY, divergingEpoch, state.highWatermark());
        } else {
            LogFetchInfo info = log.read(fetchOffset, Isolation.UNCOMMITTED);

            if (state.updateReplicaState(replicaId, currentTimeMs, info.startOffsetMetadata)) {
                updateHighWatermark(state, currentTimeMs);
            }

            return buildFetchResponse(Errors.NONE, info.records, Optional.empty(), state.highWatermark());
        }
    }

    /**
     * Check whether a fetch offset and epoch is valid. Return the diverging epoch, which
     * is the largest epoch such that subsequent records are known to diverge.
     */
    private Optional<OffsetAndEpoch> validateFetchOffsetAndEpoch(long fetchOffset, int lastFetchedEpoch) {
        if (fetchOffset == 0 && lastFetchedEpoch == 0) {
            return Optional.empty();
        }

        OffsetAndEpoch endOffsetAndEpoch = log.endOffsetForEpoch(lastFetchedEpoch)
            .orElse(new OffsetAndEpoch(-1L, -1));
        if (endOffsetAndEpoch.epoch != lastFetchedEpoch || endOffsetAndEpoch.offset < fetchOffset) {
            return Optional.of(endOffsetAndEpoch);
        } else {
            return Optional.empty();
        }
    }

    private OptionalInt optionalLeaderId(int leaderIdOrNil) {
        if (leaderIdOrNil < 0)
            return OptionalInt.empty();
        return OptionalInt.of(leaderIdOrNil);
    }

    private boolean handleFetchResponse(
        RaftResponse.Inbound responseMetadata,
        long currentTimeMs
    ) throws IOException {
        FetchResponseData response = (FetchResponseData) responseMetadata.data;
        Errors topLevelError = Errors.forCode(response.errorCode());
        if (topLevelError != Errors.NONE) {
            return handleTopLevelError(topLevelError, responseMetadata);
        }

        if (!RaftUtil.hasValidTopicPartition(response, log.topicPartition())) {
            return false;
        }

        FetchResponseData.FetchablePartitionResponse partitionResponse =
            response.responses().get(0).partitionResponses().get(0);

        FetchResponseData.LeaderIdAndEpoch currentLeaderIdAndEpoch = partitionResponse.currentLeader();
        OptionalInt responseLeaderId = optionalLeaderId(currentLeaderIdAndEpoch.leaderId());
        int responseEpoch = currentLeaderIdAndEpoch.leaderEpoch();
        Errors error = Errors.forCode(partitionResponse.errorCode());

        Optional<Boolean> handled = maybeHandleCommonResponse(
            error, responseLeaderId, responseEpoch, currentTimeMs);
        if (handled.isPresent()) {
            return handled.get();
        }

        FollowerState state = quorum.followerStateOrThrow();
        if (error == Errors.NONE) {
            FetchResponseData.EpochEndOffset divergingEpoch = partitionResponse.divergingEpoch();
            if (divergingEpoch.epoch() >= 0) {
                // The leader is asking us to truncate before continuing
                OffsetAndEpoch divergingOffsetAndEpoch = new OffsetAndEpoch(
                    divergingEpoch.endOffset(), divergingEpoch.epoch());

                state.highWatermark().ifPresent(highWatermark -> {
                    if (divergingOffsetAndEpoch.offset < highWatermark.offset) {
                        throw new KafkaException("The leader requested truncation to offset " +
                            divergingOffsetAndEpoch.offset + ", which is below the current high watermark" +
                            " " + highWatermark);
                    }
                });

                log.truncateToEndOffset(divergingOffsetAndEpoch).ifPresent(truncationOffset -> {
                    logger.info("Truncated to offset {} from Fetch response from leader {}",
                        truncationOffset, quorum.leaderIdOrNil());

                    // Since the end offset has been updated, we should complete any delayed
                    // reads at the end offset.
                    fetchPurgatory.maybeComplete(
                        new LogOffset(Long.MAX_VALUE, Isolation.UNCOMMITTED),
                        currentTimeMs);
                });
            } else {
                Records records = (Records) partitionResponse.recordSet();
                if (records.sizeInBytes() > 0) {
                    appendAsFollower(records);
                }
                OptionalLong highWatermark = partitionResponse.highWatermark() < 0 ?
                    OptionalLong.empty() : OptionalLong.of(partitionResponse.highWatermark());
                updateFollowerHighWatermark(state, highWatermark, currentTimeMs);
            }

            state.resetFetchTimeout(currentTimeMs);
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private void appendAsFollower(
        Records records
    ) {
        LogAppendInfo info = log.appendAsFollower(records);
        log.flush();

        OffsetAndEpoch endOffset = endOffset();
        kafkaRaftMetrics.updateFetchedRecords(info.lastOffset - info.firstOffset + 1);
        kafkaRaftMetrics.updateLogEnd(endOffset);
        logger.trace("Follower end offset updated to {} after append", endOffset);
    }

    private LogAppendInfo appendAsLeader(
        Records records
    ) {
        LogAppendInfo info = log.appendAsLeader(records, quorum.epoch());
        OffsetAndEpoch endOffset = endOffset();
        kafkaRaftMetrics.updateAppendRecords(info.lastOffset - info.firstOffset + 1);
        kafkaRaftMetrics.updateLogEnd(endOffset);
        logger.trace("Leader appended records at base offset {}, new end offset is {}", info.firstOffset, endOffset);
        return info;
    }

    private DescribeQuorumResponseData handleDescribeQuorumRequest(
        RaftRequest.Inbound requestMetadata,
        long currentTimeMs
    ) {
        DescribeQuorumRequestData describeQuorumRequestData = (DescribeQuorumRequestData) requestMetadata.data;
        if (!hasValidTopicPartition(describeQuorumRequestData, log.topicPartition())) {
            return DescribeQuorumRequest.getPartitionLevelErrorResponse(
                describeQuorumRequestData, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }

        if (!quorum.isLeader()) {
            return DescribeQuorumRequest.getTopLevelErrorResponse(Errors.INVALID_REQUEST);
        }

        LeaderState leaderState = quorum.leaderStateOrThrow();
        return DescribeQuorumResponse.singletonResponse(log.topicPartition(),
            leaderState.localId(),
            leaderState.epoch(),
            leaderState.highWatermark().isPresent() ? leaderState.highWatermark().get().offset : -1,
            convertToReplicaStates(leaderState.getVoterEndOffsets()),
            convertToReplicaStates(leaderState.getObserverStates(currentTimeMs))
        );
    }

    List<ReplicaState> convertToReplicaStates(Map<Integer, Long> replicaEndOffsets) {
        return replicaEndOffsets.entrySet().stream()
                   .map(entry -> new ReplicaState()
                                     .setReplicaId(entry.getKey())
                                     .setLogEndOffset(entry.getValue()))
                   .collect(Collectors.toList());
    }

    private boolean hasConsistentLeader(int epoch, OptionalInt leaderId) {
        // Only elected leaders are sent in the request/response header, so if we have an elected
        // leaderId, it should be consistent with what is in the message.
        if (leaderId.isPresent() && leaderId.getAsInt() == quorum.localId) {
            // The response indicates that we should be the leader, so we verify that is the case
            return quorum.isLeader();
        } else {
            return epoch != quorum.epoch()
                || !leaderId.isPresent()
                || !quorum.leaderId().isPresent()
                || leaderId.equals(quorum.leaderId());
        }
    }

    /**
     * Handle response errors that are common across request types.
     *
     * @param error Error from the received response
     * @param leaderId Optional leaderId from the response
     * @param epoch Epoch received from the response
     * @param currentTimeMs Current epoch time in milliseconds
     * @return Optional value indicating whether the error was handled here and the outcome of
     *    that handling. Specifically:
     *
     *    - Optional.empty means that the response was not handled here and the custom
     *        API handler should be applied
     *    - Optional.of(true) indicates that the response was successfully handled here and
     *        the request does not need to be retried
     *    - Optional.of(false) indicates that the response was handled here, but that the request
     *        will need to be retried
     */
    private Optional<Boolean> maybeHandleCommonResponse(
        Errors error,
        OptionalInt leaderId,
        int epoch,
        long currentTimeMs
    ) throws IOException {
        if (epoch < quorum.epoch() || error == Errors.UNKNOWN_LEADER_EPOCH) {
            // We have a larger epoch, so the response is no longer relevant
            return Optional.of(true);
        } else if (epoch > quorum.epoch()
            || error == Errors.FENCED_LEADER_EPOCH
            || error == Errors.NOT_LEADER_OR_FOLLOWER) {

            // The response indicates that the request had a stale epoch, but we need
            // to validate the epoch from the response against our current state.
            maybeTransition(leaderId, epoch, currentTimeMs);
            return Optional.of(true);
        } else if (epoch == quorum.epoch()
            && leaderId.isPresent()
            && !quorum.hasLeader()) {

            // Since we are transitioning to Follower, we will only forward the
            // request to the handler if there is no error. Otherwise, we will let
            // the request be retried immediately (if needed) after the transition.
            // This handling allows an observer to discover the leader and append
            // to the log in the same Fetch request.
            transitionToFollower(epoch, leaderId.getAsInt(), currentTimeMs);
            if (error == Errors.NONE) {
                return Optional.empty();
            } else {
                return Optional.of(true);
            }
        } else if (error == Errors.BROKER_NOT_AVAILABLE) {
            return Optional.of(false);
        } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL) {
            // For now we treat this as a fatal error. Once we have support for quorum
            // reassignment, this error could suggest that either we or the recipient of
            // the request just has stale voter information, which means we can retry
            // after backing off.
            throw new IllegalStateException("Received error indicating inconsistent voter sets");
        } else if (error == Errors.INVALID_REQUEST) {
            throw new IllegalStateException("Received unexpected invalid request error");
        }

        return Optional.empty();
    }

    private void maybeTransition(
        OptionalInt leaderId,
        int epoch,
        long currentTimeMs
    ) throws IOException {
        if (!hasConsistentLeader(epoch, leaderId)) {
            throw new IllegalStateException("Received request or response with leader " + leaderId +
                " and epoch " + epoch + " which is inconsistent with current leader " +
                quorum.leaderId() + " and epoch " + quorum.epoch());
        } else if (epoch > quorum.epoch()) {
            if (leaderId.isPresent()) {
                transitionToFollower(epoch, leaderId.getAsInt(), currentTimeMs);
            } else {
                transitionToUnattached(epoch);
            }
        } else if (leaderId.isPresent() && !quorum.hasLeader()) {
            // The request or response indicates the leader of the current epoch,
            // which is currently unknown
            transitionToFollower(epoch, leaderId.getAsInt(), currentTimeMs);
        }
    }

    private boolean handleTopLevelError(Errors error, RaftResponse.Inbound response) {
        if (error == Errors.BROKER_NOT_AVAILABLE) {
            return false;
        } else if (error == Errors.CLUSTER_AUTHORIZATION_FAILED) {
            throw new ClusterAuthorizationException("Received cluster authorization error in response " + response);
        } else {
            return handleUnexpectedError(error, response);
        }
    }

    private boolean handleUnexpectedError(Errors error, RaftResponse.Inbound response) {
        logger.error("Unexpected error {} in {} response: {}",
            error, response.data.apiKey(), response);
        return false;
    }

    private void handleResponse(RaftResponse.Inbound response, long currentTimeMs) throws IOException {
        // The response epoch matches the local epoch, so we can handle the response
        ApiKeys apiKey = ApiKeys.forId(response.data.apiKey());
        final boolean handledSuccessfully;

        switch (apiKey) {
            case FETCH:
                handledSuccessfully = handleFetchResponse(response, currentTimeMs);
                break;

            case VOTE:
                handledSuccessfully = handleVoteResponse(response, currentTimeMs);
                break;

            case BEGIN_QUORUM_EPOCH:
                handledSuccessfully = handleBeginQuorumEpochResponse(response, currentTimeMs);
                break;

            case END_QUORUM_EPOCH:
                handledSuccessfully = handleEndQuorumEpochResponse(response, currentTimeMs);
                break;

            default:
                throw new IllegalArgumentException("Received unexpected response type: " + apiKey);
        }

        ConnectionState connection = requestManager.getOrCreate(response.sourceId());
        if (handledSuccessfully) {
            connection.onResponseReceived(response.correlationId, currentTimeMs);
        } else {
            connection.onResponseError(response.correlationId, currentTimeMs);
        }
    }

    /**
     * Validate a request which is only valid between voters. If an error is
     * present in the returned value, it should be returned in the response.
     */
    private Optional<Errors> validateVoterOnlyRequest(int remoteNodeId, int requestEpoch) {
        if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH);
        } else if (remoteNodeId < 0) {
            return Optional.of(Errors.INVALID_REQUEST);
        } else if (quorum.isObserver() || !quorum.isVoter(remoteNodeId)) {
            return Optional.of(Errors.INCONSISTENT_VOTER_SET);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Validate a request which is intended for the current quorum leader.
     * If an error is present in the returned value, it should be returned
     * in the response.
     */
    private Optional<Errors> validateLeaderOnlyRequest(int requestEpoch) {
        if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH);
        } else if (requestEpoch > quorum.epoch()) {
            return Optional.of(Errors.UNKNOWN_LEADER_EPOCH);
        } else if (!quorum.isLeader()) {
            // In general, non-leaders do not expect to receive requests
            // matching their own epoch, but it is possible when observers
            // are using the Fetch API to find the result of an election.
            return Optional.of(Errors.NOT_LEADER_OR_FOLLOWER);
        } else if (shutdown.get() != null) {
            return Optional.of(Errors.BROKER_NOT_AVAILABLE);
        } else {
            return Optional.empty();
        }
    }

    private void handleRequest(RaftRequest.Inbound request, long currentTimeMs) throws IOException {
        ApiKeys apiKey = ApiKeys.forId(request.data.apiKey());
        final CompletableFuture<? extends ApiMessage> responseFuture;

        switch (apiKey) {
            case FETCH:
                responseFuture = handleFetchRequest(request, currentTimeMs);
                break;

            case VOTE:
                responseFuture = completedFuture(handleVoteRequest(request));
                break;

            case BEGIN_QUORUM_EPOCH:
                responseFuture = completedFuture(handleBeginQuorumEpochRequest(request, currentTimeMs));
                break;

            case END_QUORUM_EPOCH:
                responseFuture = completedFuture(handleEndQuorumEpochRequest(request, currentTimeMs));
                break;

            case DESCRIBE_QUORUM:
                responseFuture = completedFuture(handleDescribeQuorumRequest(request, currentTimeMs));
                break;

            default:
                throw new IllegalArgumentException("Unexpected request type " + apiKey);
        }

        responseFuture.whenComplete((response, exception) -> {
            final ApiMessage message;
            if (response != null) {
                message = response;
            } else {
                message = RaftUtil.errorResponse(apiKey, Errors.forException(exception));
            }
            sendOutboundMessage(new RaftResponse.Outbound(request.correlationId(), message));
        });
    }

    private void handleInboundMessage(RaftMessage message, long currentTimeMs) throws IOException {
        logger.trace("Received inbound message {}", message);

        if (message instanceof RaftRequest.Inbound) {
            RaftRequest.Inbound request = (RaftRequest.Inbound) message;
            handleRequest(request, currentTimeMs);
        } else if (message instanceof RaftResponse.Inbound) {
            RaftResponse.Inbound response = (RaftResponse.Inbound) message;
            handleResponse(response, currentTimeMs);
        } else {
            throw new IllegalArgumentException("Unexpected message " + message);
        }
    }

    private void sendOutboundMessage(RaftMessage message) {
        channel.send(message);
        logger.trace("Sent outbound message: {}", message);
    }

    /**
     * Attempt to send a request. Return the time to wait before the request can be retried.
     */
    private long maybeSendRequest(
        long currentTimeMs,
        int destinationId,
        Supplier<ApiMessage> requestSupplier
    )  {
        ConnectionState connection = requestManager.getOrCreate(destinationId);

        if (connection.isBackingOff(currentTimeMs)) {
            return connection.remainingBackoffMs(currentTimeMs);
        }

        if (connection.isReady(currentTimeMs)) {
            int correlationId = channel.newCorrelationId();
            ApiMessage request = requestSupplier.get();
            sendOutboundMessage(new RaftRequest.Outbound(correlationId, request, destinationId, currentTimeMs));
            connection.onRequestSent(correlationId, currentTimeMs);
            return Long.MAX_VALUE;
        }

        return connection.remainingRequestTimeMs(currentTimeMs);
    }

    private EndQuorumEpochRequestData buildEndQuorumEpochRequest() {
        List<Integer> preferredSuccessors = quorum.isLeader() ?
            quorum.leaderStateOrThrow().nonLeaderVotersByDescendingFetchOffset() :
            Collections.emptyList();

        return EndQuorumEpochRequest.singletonRequest(
            log.topicPartition(),
            quorum.localId,
            quorum.epoch(),
            quorum.leaderIdOrNil(),
            preferredSuccessors
        );
    }

    private long maybeSendRequests(
        long currentTimeMs,
        Set<Integer> destinationIds,
        Supplier<ApiMessage> requestSupplier
    ) {
        long minBackoffMs = Long.MAX_VALUE;
        for (Integer destinationId : destinationIds) {
            long backoffMs = maybeSendRequest(currentTimeMs, destinationId, requestSupplier);
            if (backoffMs < minBackoffMs) {
                minBackoffMs = backoffMs;
            }
        }
        return minBackoffMs;
    }

    private BeginQuorumEpochRequestData buildBeginQuorumEpochRequest() {
        return BeginQuorumEpochRequest.singletonRequest(
            log.topicPartition(),
            quorum.epoch(),
            quorum.localId
        );
    }

    private VoteRequestData buildVoteRequest() {
        OffsetAndEpoch endOffset = endOffset();
        return VoteRequest.singletonRequest(
            log.topicPartition(),
            quorum.epoch(),
            quorum.localId,
            endOffset.epoch,
            endOffset.offset
        );
    }

    private FetchRequestData buildFetchRequest() {
        FetchRequestData request = RaftUtil.singletonFetchRequest(log.topicPartition(), fetchPartition -> {
            fetchPartition
                .setCurrentLeaderEpoch(quorum.epoch())
                .setLastFetchedEpoch(log.lastFetchedEpoch())
                .setFetchOffset(log.endOffset().offset);
        });
        return request
            .setMaxWaitMs(fetchMaxWaitMs)
            .setReplicaId(quorum.localId);
    }

    private long maybeSendAnyVoterFetch(long currentTimeMs) {
        OptionalInt readyVoterIdOpt = requestManager.findReadyVoter(currentTimeMs);
        if (readyVoterIdOpt.isPresent()) {
            return maybeSendRequest(
                currentTimeMs,
                readyVoterIdOpt.getAsInt(),
                this::buildFetchRequest
            );
        } else {
            return requestManager.backoffBeforeAvailableVoter(currentTimeMs);
        }
    }

    public boolean isRunning() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        return gracefulShutdown == null || !gracefulShutdown.isFinished();
    }

    public boolean isShuttingDown() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        return gracefulShutdown != null && !gracefulShutdown.isFinished();
    }

    private void pollShutdown(GracefulShutdown shutdown) throws IOException {
        // Graceful shutdown allows a leader or candidate to resign its leadership without
        // awaiting expiration of the election timeout. As soon as another leader is elected,
        // the shutdown is considered complete.

        shutdown.update();
        if (shutdown.isFinished()) {
            return;
        }

        long currentTimeMs = shutdown.finishTimer.currentTimeMs();

        if (quorum.remoteVoters().isEmpty() || quorum.hasRemoteLeader()) {
            shutdown.complete();
            return;
        }

        long pollTimeoutMs = shutdown.finishTimer.remainingMs();
        if (quorum.isLeader() || quorum.isCandidate()) {
            long backoffMs = maybeSendRequests(
                currentTimeMs,
                quorum.remoteVoters(),
                this::buildEndQuorumEpochRequest
            );
            pollTimeoutMs = Math.min(backoffMs, pollTimeoutMs);
        }

        List<RaftMessage> inboundMessages = channel.receive(pollTimeoutMs);
        for (RaftMessage message : inboundMessages) {
            handleInboundMessage(message, currentTimeMs);
            currentTimeMs = time.milliseconds();
        }
    }

    private long pollLeader(long currentTimeMs) {
        LeaderState state = quorum.leaderStateOrThrow();
        pollPendingAppends(state, currentTimeMs);

        return maybeSendRequests(
            currentTimeMs,
            state.nonEndorsingFollowers(),
            this::buildBeginQuorumEpochRequest
        );
    }

    private long pollCandidate(long currentTimeMs) throws IOException {
        CandidateState state = quorum.candidateStateOrThrow();
        if (state.isBackingOff()) {
            if (state.isBackoffComplete(currentTimeMs)) {
                logger.info("Re-elect as candidate after election backoff has completed");
                transitionToCandidate(currentTimeMs);
                return 0L;
            }
            return state.remainingBackoffMs(currentTimeMs);
        } else if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            long backoffDurationMs = binaryExponentialElectionBackoffMs(state.retries());
            logger.debug("Election has timed out, backing off for {}ms before becoming a candidate again",
                backoffDurationMs);
            state.startBackingOff(currentTimeMs, backoffDurationMs);
            return backoffDurationMs;
        } else if (!state.isVoteRejected()) {
            // Continue sending Vote requests as long as we still have a chance to win the election
            long minRequestBackoffMs = maybeSendRequests(
                currentTimeMs,
                state.unrecordedVoters(),
                this::buildVoteRequest
            );
            return Math.min(minRequestBackoffMs, state.remainingElectionTimeMs(currentTimeMs));
        } else {
            return state.remainingElectionTimeMs(currentTimeMs);
        }
    }

    private long pollFollower(long currentTimeMs) throws IOException {
        FollowerState state = quorum.followerStateOrThrow();
        if (quorum.isVoter()) {
            return pollFollowerAsVoter(state, currentTimeMs);
        } else {
            return pollFollowerAsObserver(state, currentTimeMs);
        }
    }

    private long pollFollowerAsVoter(FollowerState state, long currentTimeMs) throws IOException {
        failPendingAppends(new NotLeaderOrFollowerException("Failing append " +
            "since this node is not the current leader"));

        if (state.hasFetchTimeoutExpired(currentTimeMs)) {
            logger.info("Become candidate due to fetch timeout");
            transitionToCandidate(currentTimeMs);
            return 0L;
        } else {
            long backoffMs = maybeSendRequest(
                currentTimeMs,
                state.leaderId(),
                this::buildFetchRequest
            );
            return Math.min(backoffMs, state.remainingFetchTimeMs(currentTimeMs));
        }
    }

    private long pollFollowerAsObserver(FollowerState state, long currentTimeMs) {
        if (state.hasFetchTimeoutExpired(currentTimeMs)) {
            return maybeSendAnyVoterFetch(currentTimeMs);
        } else {
            final long backoffMs;

            // If the current leader is backing off due to some failure or if the
            // request has timed out, then we attempt to send the Fetch to another
            // voter in order to discover if there has been a leader change.
            ConnectionState connection = requestManager.getOrCreate(state.leaderId());
            if (connection.hasRequestTimedOut(currentTimeMs)) {
                backoffMs = maybeSendAnyVoterFetch(currentTimeMs);
                connection.reset();
            } else if (connection.isBackingOff(currentTimeMs)) {
                backoffMs = maybeSendAnyVoterFetch(currentTimeMs);
            } else {
                backoffMs = maybeSendRequest(
                    currentTimeMs,
                    state.leaderId(),
                    this::buildFetchRequest
                );

            }
            return Math.min(backoffMs, state.remainingFetchTimeMs(currentTimeMs));
        }
    }

    private long pollVoted(long currentTimeMs) throws IOException {
        VotedState state = quorum.votedStateOrThrow();
        if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            transitionToCandidate(currentTimeMs);
            return 0L;
        } else {
            return state.remainingElectionTimeMs(currentTimeMs);
        }
    }

    private long pollUnattached(long currentTimeMs) throws IOException {
        UnattachedState state = quorum.unattachedStateOrThrow();
        if (quorum.isVoter()) {
            return pollUnattachedAsVoter(state, currentTimeMs);
        } else {
            return pollUnattachedAsObserver(state, currentTimeMs);
        }
    }

    private long pollUnattachedAsVoter(UnattachedState state, long currentTimeMs) throws IOException {
        if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            transitionToCandidate(currentTimeMs);
            return 0L;
        } else {
            return state.remainingElectionTimeMs(currentTimeMs);
        }
    }

    private long pollUnattachedAsObserver(UnattachedState state, long currentTimeMs) {
        long fetchBackoffMs = maybeSendAnyVoterFetch(currentTimeMs);
        return Math.min(fetchBackoffMs, state.remainingElectionTimeMs(currentTimeMs));
    }

    private long pollCurrentState(long currentTimeMs) throws IOException {
        if (quorum.isLeader()) {
            return pollLeader(currentTimeMs);
        } else if (quorum.isCandidate()) {
            return pollCandidate(currentTimeMs);
        } else if (quorum.isFollower()) {
            return pollFollower(currentTimeMs);
        } else if (quorum.isVoted()) {
            return pollVoted(currentTimeMs);
        } else if (quorum.isUnattached()) {
            return pollUnattached(currentTimeMs);
        } else {
            throw new IllegalStateException("Unexpected quorum state " + quorum);
        }
    }

    public void poll() throws IOException {
        GracefulShutdown gracefulShutdown = shutdown.get();
        if (gracefulShutdown != null) {
            pollShutdown(gracefulShutdown);
        } else {
            long currentTimeMs = time.milliseconds();
            long pollTimeoutMs = pollCurrentState(currentTimeMs);
            kafkaRaftMetrics.updatePollStart(currentTimeMs);
            List<RaftMessage> inboundMessages = channel.receive(pollTimeoutMs);

            currentTimeMs = time.milliseconds();
            kafkaRaftMetrics.updatePollEnd(currentTimeMs);

            for (RaftMessage message : inboundMessages) {
                handleInboundMessage(message, currentTimeMs);
                currentTimeMs = time.milliseconds();
            }
        }
    }

    private void failPendingAppends(KafkaException exception) {
        for (UnwrittenAppend unwrittenAppend : unwrittenAppends) {
            unwrittenAppend.fail(exception);
        }
        unwrittenAppends.clear();
    }

    private void pollPendingAppends(LeaderState state, long currentTimeMs) {
        int numAppends = 0;
        int maxNumAppends = unwrittenAppends.size();

        while (!unwrittenAppends.isEmpty() && numAppends < maxNumAppends) {
            final UnwrittenAppend unwrittenAppend = unwrittenAppends.poll();

            if (unwrittenAppend.future.isDone())
                continue;

            if (unwrittenAppend.isTimedOut(currentTimeMs)) {
                unwrittenAppend.fail(new TimeoutException("Request timeout " + unwrittenAppend.requestTimeoutMs
                    + " expired before the records could be appended to the log"));
            } else {
                int epoch = quorum.epoch();
                LogAppendInfo info = appendAsLeader(unwrittenAppend.records);
                OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(info.lastOffset, epoch);
                long numRecords = info.lastOffset - info.firstOffset + 1;
                logger.debug("Completed write of {} records at {}", numRecords, offsetAndEpoch);

                if (unwrittenAppend.ackMode == AckMode.LEADER) {
                    unwrittenAppend.complete(offsetAndEpoch);
                } else if (unwrittenAppend.ackMode == AckMode.QUORUM) {
                    CompletableFuture<Long> future = appendPurgatory.await(
                        LogOffset.awaitCommitted(offsetAndEpoch.offset),
                        unwrittenAppend.requestTimeoutMs);

                    future.whenComplete((completionTimeMs, exception) -> {
                        if (exception != null) {
                            logger.error("Failed to commit append at {} due to {}", offsetAndEpoch, exception);

                            unwrittenAppend.fail(exception);
                        } else {
                            long elapsedTime = Math.max(0, completionTimeMs - currentTimeMs);
                            double elapsedTimePerRecord = (double) elapsedTime / numRecords;
                            kafkaRaftMetrics.updateCommitLatency(elapsedTimePerRecord, currentTimeMs);
                            unwrittenAppend.complete(offsetAndEpoch);

                            logger.debug("Completed commit of {} records at {}", numRecords, offsetAndEpoch);
                        }
                    });
                }
            }

            numAppends++;
        }

        if (numAppends > 0) {
            flushLeaderLog(state, currentTimeMs);
        }
    }

    /**
     * Append a set of records to the log. Successful completion of the future indicates a success of
     * the append, with the uncommitted base offset and epoch.
     *
     * @param records The records to write to the log
     * @param ackMode The commit mode for the appended records
     * @param timeoutMs The maximum time to wait for the append operation to complete (including
     *                  any time needed for replication)
     * @return The uncommitted base offset and epoch of the appended records
     */
    @Override
    public CompletableFuture<OffsetAndEpoch> append(
        Records records,
        AckMode ackMode,
        long timeoutMs
    ) {
        if (records.sizeInBytes() == 0)
            throw new IllegalArgumentException("Attempt to append empty record set");

        if (shutdown.get() != null)
            throw new IllegalStateException("Cannot append records while we are shutting down");

        if (quorum.isObserver())
            throw new IllegalStateException("Illegal attempt to write to an observer");

        CompletableFuture<OffsetAndEpoch> future = new CompletableFuture<>();
        UnwrittenAppend unwrittenAppend = new UnwrittenAppend(
            records, time.milliseconds(), timeoutMs, ackMode, future);

        if (!unwrittenAppends.offer(unwrittenAppend)) {
            future.completeExceptionally(new KafkaException("Failed to append records since the unsent " +
                "append queue is full"));
        }
        channel.wakeup();
        return future;
    }

    @Override
    public CompletableFuture<Void> shutdown(int timeoutMs) {
        logger.info("Beginning graceful shutdown");
        CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();
        shutdown.set(new GracefulShutdown(timeoutMs, shutdownComplete));
        channel.wakeup();
        return shutdownComplete;
    }

    private void close() {
        kafkaRaftMetrics.close();
    }

    public OptionalLong highWatermark() {
        return quorum.highWatermark().isPresent() ? OptionalLong.of(quorum.highWatermark().get().offset) : OptionalLong.empty();
    }

    private class GracefulShutdown {
        final Timer finishTimer;
        final CompletableFuture<Void> completeFuture;

        public GracefulShutdown(long shutdownTimeoutMs,
                                CompletableFuture<Void> completeFuture) {
            this.finishTimer = time.timer(shutdownTimeoutMs);
            this.completeFuture = completeFuture;
        }

        public void update() {
            finishTimer.update();
            if (finishTimer.isExpired()) {
                close();
                logger.warn("Graceful shutdown timed out after {}ms", finishTimer.timeoutMs());
                completeFuture.completeExceptionally(
                    new TimeoutException("Timeout expired before shutdown completed"));
            }
        }

        public boolean isFinished() {
            return completeFuture.isDone();
        }

        public boolean succeeded() {
            return isFinished() && !failed();
        }

        public boolean failed() {
            return completeFuture.isCompletedExceptionally();
        }

        public void complete() {
            close();
            logger.info("Graceful shutdown completed");
            completeFuture.complete(null);
        }
    }

    private static class UnwrittenAppend {
        private final Records records;
        private final long createTimeMs;
        private final long requestTimeoutMs;
        private final AckMode ackMode;
        private final CompletableFuture<OffsetAndEpoch> future;

        private UnwrittenAppend(Records records,
                                long createTimeMs,
                                long requestTimeoutMs,
                                AckMode ackMode,
                                CompletableFuture<OffsetAndEpoch> future) {
            this.future = future;
            this.records = records;
            this.ackMode = ackMode;
            this.createTimeMs = createTimeMs;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        public void complete(OffsetAndEpoch offsetAndEpoch) {
            future.complete(offsetAndEpoch);
        }

        public void fail(Throwable e) {
            future.completeExceptionally(e);
        }

        public boolean isTimedOut(long currentTimeMs) {
            return currentTimeMs > createTimeMs + requestTimeoutMs;
        }
    }
}
