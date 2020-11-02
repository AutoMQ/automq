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

import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonList;
import static org.apache.kafka.raft.RaftClientTestContext.Builder.DEFAULT_ELECTION_TIMEOUT_MS;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientTest {

    @Test
    public void testInitializeSingleMemberQuorum() throws IOException {
        int localId = 0;
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, Collections.singleton(localId)).build();
        context.assertElectedLeader(1, context.localId);
    }

    @Test
    public void testInitializeAsLeaderFromStateStoreSingleMemberQuorum() throws Exception {
        // Start off as leader. We should still bump the epoch after initialization

        int localId = 0;
        int initialEpoch = 2;
        Set<Integer> voters = Collections.singleton(localId);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(initialEpoch, localId)
            .build();

        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(initialEpoch + 1, context.log.lastFetchedEpoch());
        assertEquals(new LeaderAndEpoch(OptionalInt.of(context.localId), initialEpoch + 1),
            context.currentLeaderAndEpoch());
        context.assertElectedLeader(initialEpoch + 1, context.localId);
    }

    @Test
    public void testInitializeAsLeaderFromStateStore() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, 1);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(random -> {
                Mockito.doReturn(0).when(random).nextInt(DEFAULT_ELECTION_TIMEOUT_MS);
            })
            .withElectedLeader(epoch, localId)
            .build();

        assertEquals(0L, context.log.endOffset().offset);
        context.assertUnknownLeader(epoch);

        context.time.sleep(context.electionTimeoutMs);
        context.pollUntilSend();
        context.assertSentVoteRequest(epoch + 1, 0, 0L);
    }

    @Test
    public void testInitializeAsCandidateFromStateStore() throws Exception {
        int localId = 0;
        // Need 3 node to require a 2-node majority
        Set<Integer> voters = Utils.mkSet(localId, 1, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(2, localId)
            .build();

        assertEquals(0L, context.log.endOffset().offset);

        // Send out vote requests.
        context.client.poll();

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(2, 0, 0);
        assertEquals(2, voteRequests.size());
    }

    @Test
    public void testInitializeAsCandidateAndBecomeLeader() throws Exception {
        int localId = 0;
        final int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        context.assertUnknownLeader(0);
        context.time.sleep(2 * context.electionTimeoutMs);

        context.pollUntilSend();
        context.assertVotedCandidate(1, context.localId);

        int correlationId = context.assertSentVoteRequest(1, 0, 0L);
        context.deliverResponse(correlationId, otherNodeId, context.voteResponse(true, Optional.empty(), 1));

        // Become leader after receiving the vote
        context.client.poll();
        context.assertElectedLeader(1, context.localId);
        long electionTimestamp = context.time.milliseconds();

        // Leader change record appended
        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(1L, context.log.lastFlushedOffset());

        // Send BeginQuorumEpoch to voters
        context.client.poll();
        context.assertSentBeginQuorumEpochRequest(1);

        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());

        Record record = batch.iterator().next();
        assertEquals(electionTimestamp, record.timestamp());
        RaftClientTestContext.verifyLeaderChangeMessage(context.localId,
            Collections.singletonList(otherNodeId), record.key(), record.value());
    }

    @Test
    public void testHandleBeginQuorumRequest() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int votedCandidateEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(votedCandidateEpoch, otherNodeId)
            .build();

        context.deliverRequest(context.beginEpochRequest(votedCandidateEpoch, otherNodeId));

        context.client.poll();

        context.assertElectedLeader(votedCandidateEpoch, otherNodeId);

        context.assertSentBeginQuorumEpochResponse(Errors.NONE, votedCandidateEpoch, OptionalInt.of(otherNodeId));
    }

    @Test
    public void testHandleBeginQuorumResponse() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(leaderEpoch, localId)
            .build();

        context.deliverRequest(context.beginEpochRequest(leaderEpoch + 1, otherNodeId));

        context.client.poll();

        context.assertElectedLeader(leaderEpoch + 1, otherNodeId);
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyCandidate() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 2;
        int jitterMs = 85;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(random -> {
                Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());
            })
            .withVotedCandidate(epoch, localId)
            .build();

        context.deliverRequest(context.endEpochRequest(epoch, OptionalInt.empty(),
            otherNodeId, Collections.singletonList(context.localId)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.empty());

        // We should still be candidate until expiration of election timeout
        context.time.sleep(context.electionTimeoutMs + jitterMs - 1);
        context.client.poll();
        context.assertVotedCandidate(epoch, context.localId);

        // Enter the backoff period
        context.time.sleep(1);
        context.client.poll();
        context.assertVotedCandidate(epoch, context.localId);

        // After backoff, we will become a candidate again
        context.time.sleep(context.electionBackoffMaxMs);
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, context.localId);
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyLeader() throws Exception {
        int localId = 0;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, voter2, voter3);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // One of the voters may have sent EndEpoch as a candidate because it
        // had not yet been notified that the local node was the leader.
        context.deliverRequest(context.endEpochRequest(epoch, OptionalInt.empty(), voter2, Arrays.asList(context.localId, voter3)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(context.localId));

        // We should still be leader as long as fetch timeout has not expired
        context.time.sleep(context.fetchTimeoutMs - 1);
        context.client.poll();
        context.assertElectedLeader(epoch, context.localId);
    }

    @Test
    public void testEndQuorumStartsNewElectionAfterBackoffIfReceivedFromVotedCandidate() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(epoch, otherNodeId)
            .build();

        context.deliverRequest(context.endEpochRequest(epoch, OptionalInt.empty(),
            otherNodeId, Collections.singletonList(context.localId)));
        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.empty());

        context.time.sleep(context.electionBackoffMaxMs);
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, context.localId);
    }

    @Test
    public void testEndQuorumStartsNewElectionImmediatelyIfFollowerUnattached() throws Exception {
        int localId = 0;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .build();
        
        context.deliverRequest(context.endEpochRequest(epoch, OptionalInt.of(voter2), voter2, Arrays.asList(context.localId, voter3)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(voter2));

        // Should become a candidate immediately
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, context.localId);
    }

    @Test
    public void testAccumulatorClearedAfterBecomingFollower() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int lingerMs = 50;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
        ByteBuffer buffer = ByteBuffer.allocate(KafkaRaftClient.MAX_BATCH_SIZE);
        Mockito.when(memoryPool.tryAllocate(KafkaRaftClient.MAX_BATCH_SIZE))
            .thenReturn(buffer);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withMemoryPool(memoryPool)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        assertEquals(1L, context.client.scheduleAppend(epoch, singletonList("a")));
        context.deliverRequest(context.beginEpochRequest(epoch + 1, otherNodeId));
        context.client.poll();

        context.assertElectedLeader(epoch + 1, otherNodeId);
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testAccumulatorClearedAfterBecomingVoted() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int lingerMs = 50;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
        ByteBuffer buffer = ByteBuffer.allocate(KafkaRaftClient.MAX_BATCH_SIZE);
        Mockito.when(memoryPool.tryAllocate(KafkaRaftClient.MAX_BATCH_SIZE))
            .thenReturn(buffer);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withMemoryPool(memoryPool)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        assertEquals(1L, context.client.scheduleAppend(epoch, singletonList("a")));
        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, epoch,
            context.log.endOffset().offset));
        context.client.poll();

        context.assertVotedCandidate(epoch + 1, otherNodeId);
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testAccumulatorClearedAfterBecomingUnattached() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int lingerMs = 50;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
        ByteBuffer buffer = ByteBuffer.allocate(KafkaRaftClient.MAX_BATCH_SIZE);
        Mockito.when(memoryPool.tryAllocate(KafkaRaftClient.MAX_BATCH_SIZE))
            .thenReturn(buffer);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withMemoryPool(memoryPool)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        assertEquals(1L, context.client.scheduleAppend(epoch, singletonList("a")));
        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, epoch, 0L));
        context.client.poll();

        context.assertUnknownLeader(epoch + 1);
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testHandleEndQuorumRequest() throws Exception {
        int localId = 0;
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, oldLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(leaderEpoch, oldLeaderId)
            .build();

        context.deliverRequest(context.endEpochRequest(leaderEpoch, OptionalInt.of(oldLeaderId), oldLeaderId, Collections.singletonList(context.localId)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderId));

        context.client.poll();
        context.assertVotedCandidate(leaderEpoch + 1, context.localId);
    }

    @Test
    public void testHandleEndQuorumRequestWithLowerPriorityToBecomeLeader() throws Exception {
        int localId = 0;
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        int preferredNextLeader = 3;
        Set<Integer> voters = Utils.mkSet(localId, oldLeaderId, preferredNextLeader);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(leaderEpoch, oldLeaderId)
            .build();

        context.deliverRequest(context.endEpochRequest(leaderEpoch,
            OptionalInt.of(oldLeaderId), oldLeaderId, Arrays.asList(preferredNextLeader, context.localId)));

        context.pollUntilSend();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderId));

        // The election won't trigger by one round retry backoff
        context.time.sleep(1);

        context.pollUntilSend();

        context.assertSentFetchRequest(leaderEpoch, 0, 0);

        context.time.sleep(context.retryBackoffMs);

        context.pollUntilSend();

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(leaderEpoch + 1, 0, 0);
        assertEquals(2, voteRequests.size());

        // Should have already done self-voting
        context.assertVotedCandidate(leaderEpoch + 1, context.localId);
    }

    @Test
    public void testVoteRequestTimeout() throws Exception {
        int localId = 0;
        int epoch = 1;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();
        context.assertUnknownLeader(0);

        context.time.sleep(2 * context.electionTimeoutMs);
        context.pollUntilSend();
        context.assertVotedCandidate(epoch, context.localId);

        int correlationId = context.assertSentVoteRequest(epoch, 0, 0L);

        context.time.sleep(context.requestTimeoutMs);
        context.client.poll();
        int retryCorrelationId = context.assertSentVoteRequest(epoch, 0, 0L);

        // Even though we have resent the request, we should still accept the response to
        // the first request if it arrives late.
        context.deliverResponse(correlationId, otherNodeId, context.voteResponse(true, Optional.empty(), 1));
        context.client.poll();
        context.assertElectedLeader(epoch, context.localId);

        // If the second request arrives later, it should have no effect
        context.deliverResponse(retryCorrelationId, otherNodeId, context.voteResponse(true, Optional.empty(), 1));
        context.client.poll();
        context.assertElectedLeader(epoch, context.localId);
    }

    @Test
    public void testHandleValidVoteRequestAsFollower() throws Exception {
        int localId = 0;
        int epoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .build();

        context.deliverRequest(context.voteRequest(epoch, otherNodeId, epoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        context.assertVotedCandidate(epoch, otherNodeId);
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithElectedLeader() throws Exception {
        int localId = 0;
        int epoch = 2;
        int otherNodeId = 1;
        int electedLeaderId = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, electedLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, electedLeaderId)
            .build();

        context.deliverRequest(context.voteRequest(epoch, otherNodeId, epoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), false);

        context.assertElectedLeader(epoch, electedLeaderId);
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithVotedCandidate() throws Exception {
        int localId = 0;
        int epoch = 2;
        int otherNodeId = 1;
        int votedCandidateId = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, votedCandidateId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(epoch, votedCandidateId)
            .build();

        context.deliverRequest(context.voteRequest(epoch, otherNodeId, epoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), false);
        context.assertVotedCandidate(epoch, votedCandidateId);
    }

    @Test
    public void testHandleInvalidVoteRequestWithOlderEpoch() throws Exception {
        int localId = 0;
        int epoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .build();

        context.deliverRequest(context.voteRequest(epoch - 1, otherNodeId, epoch - 2, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);
        context.assertUnknownLeader(epoch);
    }

    @Test
    public void testHandleInvalidVoteRequestAsObserver() throws Exception {
        int localId = 0;
        int epoch = 2;
        int otherNodeId = 1;
        int otherNodeId2 = 2;
        Set<Integer> voters = Utils.mkSet(otherNodeId, otherNodeId2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .build();

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, epoch, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.empty(), false);
        context.assertUnknownLeader(epoch);
    }

    @Test
    public void testLeaderIgnoreVoteRequestOnSameEpoch() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, leaderEpoch);

        context.deliverRequest(context.voteRequest(leaderEpoch, otherNodeId, leaderEpoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.of(context.localId), false);
        context.assertElectedLeader(leaderEpoch, context.localId);
    }

    @Test
    public void testListenerCommitCallbackAfterLeaderWrite() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // First poll has no high watermark advance
        context.client.poll();
        assertEquals(OptionalLong.empty(), context.client.highWatermark());

        // Let follower send a fetch to initialize the high watermark,
        // note the offset 0 would be a control message for becoming the leader
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 0L, epoch, 500));
        context.pollUntilSend();
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        List<String> records = Arrays.asList("a", "b", "c");
        long offset = context.client.scheduleAppend(epoch, records);
        context.client.poll();
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Let the follower send a fetch, it should advance the high watermark
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 1L, epoch, 500));
        context.pollUntilSend();
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Let the follower send another fetch from offset 4
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 4L, epoch, 500));
        context.client.poll();
        assertEquals(OptionalLong.of(4L), context.client.highWatermark());
        assertEquals(records, context.listener.commitWithLastOffset(offset));
    }

    @Test
    public void testCandidateIgnoreVoteRequestOnSameEpoch() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(leaderEpoch, localId)
            .build();

        context.pollUntilSend();

        context.deliverRequest(context.voteRequest(leaderEpoch, otherNodeId, leaderEpoch - 1, 1));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), false);
        context.assertVotedCandidate(leaderEpoch, context.localId);
    }

    @Test
    public void testRetryElection() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 1;
        int exponentialFactor = 85;  // set it large enough so that we will bound on jitter
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(random -> {
                Mockito.doReturn(exponentialFactor).when(random).nextInt(Mockito.anyInt());
            })
            .build();

        context.assertUnknownLeader(0);

        context.time.sleep(2 * context.electionTimeoutMs);
        context.pollUntilSend();
        context.assertVotedCandidate(epoch, context.localId);

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        int correlationId = context.assertSentVoteRequest(epoch, 0, 0L);
        context.deliverResponse(correlationId, otherNodeId, context.voteResponse(false, Optional.empty(), 1));

        context.client.poll();

        // All nodes have rejected our candidacy, but we should still remember that we had voted
        context.assertVotedCandidate(epoch, context.localId);

        // Even though our candidacy was rejected, we will backoff for jitter period
        // before we bump the epoch and start a new election.
        context.time.sleep(context.electionBackoffMaxMs - 1);
        context.client.poll();
        context.assertVotedCandidate(epoch, context.localId);

        // After jitter expires, we become a candidate again
        context.time.sleep(1);
        context.client.poll();
        context.pollUntilSend();
        context.assertVotedCandidate(epoch + 1, context.localId);
        context.assertSentVoteRequest(epoch + 1, 0, 0L);
    }

    @Test
    public void testInitializeAsFollowerEmptyLog() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();

        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilSend();

        context.assertSentFetchRequest(epoch, 0L, 0);
    }

    @Test
    public void testInitializeAsFollowerNonEmptyLog() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .appendToLog(0L, lastEpoch, singletonList("foo"))
            .build();

        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilSend();
        context.assertSentFetchRequest(epoch, 1L, lastEpoch);
    }

    @Test
    public void testVoterBecomeCandidateAfterFetchTimeout() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .appendToLog(0L, lastEpoch, singletonList("foo"))
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilSend();
        context.assertSentFetchRequest(epoch, 1L, lastEpoch);

        context.time.sleep(context.fetchTimeoutMs);

        context.pollUntilSend();

        context.assertSentVoteRequest(epoch + 1, lastEpoch, 1L);
        context.assertVotedCandidate(epoch + 1, context.localId);
    }

    @Test
    public void testInitializeObserverNoPreviousState() throws Exception {
        int localId = 0;
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));

        context.client.poll();
        context.assertElectedLeader(epoch, leaderId);
    }

    @Test
    public void testObserverQuorumDiscoveryFailure() throws Exception {
        int localId = 0;
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            context.fetchResponse(-1, -1, MemoryRecords.EMPTY, -1, Errors.UNKNOWN_SERVER_ERROR));
        context.client.poll();

        context.time.sleep(context.retryBackoffMs);
        context.pollUntilSend();

        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        context.client.poll();

        context.assertElectedLeader(epoch, leaderId);
    }

    @Test
    public void testObserverSendDiscoveryFetchAfterFetchTimeout() throws Exception {
        int localId = 0;
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        context.client.poll();

        context.assertElectedLeader(epoch, leaderId);
        context.time.sleep(context.fetchTimeoutMs);

        context.pollUntilSend();
        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testInvalidFetchRequest() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.deliverRequest(context.fetchRequest(
            epoch, otherNodeId, -5L, 0, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(context.localId));

        context.deliverRequest(context.fetchRequest(
            epoch, otherNodeId, 0L, -1, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(context.localId));

        context.deliverRequest(context.fetchRequest(
            epoch, otherNodeId, 0L, epoch + 1, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(context.localId));

        context.deliverRequest(context.fetchRequest(
            epoch + 1, otherNodeId, 0L, 0, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.UNKNOWN_LEADER_EPOCH, epoch, OptionalInt.of(context.localId));

        context.deliverRequest(context.fetchRequest(
            epoch, otherNodeId, 0L, 0, -1));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(context.localId));
    }

    @Test
    public void testVoterOnlyRequestValidation() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        int nonVoterId = 2;
        context.deliverRequest(context.voteRequest(epoch, nonVoterId, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(context.localId), false);

        context.deliverRequest(context.beginEpochRequest(epoch, nonVoterId));
        context.client.poll();
        context.assertSentBeginQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(context.localId));

        context.deliverRequest(context.endEpochRequest(epoch, OptionalInt.of(context.localId), nonVoterId, Collections.singletonList(otherNodeId)));
        context.client.poll();

        // The sent request has no context.localId as a preferable voter.
        context.assertSentEndQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(context.localId));
    }

    @Test
    public void testInvalidVoteRequest() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, 0, -5L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        context.assertElectedLeader(epoch, otherNodeId);

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, -1, 0L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        context.assertElectedLeader(epoch, otherNodeId);

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, epoch + 1, 0L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        context.assertElectedLeader(epoch, otherNodeId);
    }

    @Test
    public void testPurgatoryFetchTimeout() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        int maxWaitTimeMs = 500;
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 1L, epoch, maxWaitTimeMs));
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());

        // After expiration of the max wait time, the fetch returns an empty record set
        context.time.sleep(maxWaitTimeMs);
        context.client.poll();
        MemoryRecords fetchedRecords = context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(context.localId));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    @Test
    public void testPurgatoryFetchSatisfiedByWrite() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 1L, epoch, 500));
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());

        // Append some records that can fulfill the Fetch request
        String[] appendRecords = new String[] {"a", "b", "c"};
        context.client.scheduleAppend(epoch, Arrays.asList(appendRecords));
        context.client.poll();

        MemoryRecords fetchedRecords = context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        RaftClientTestContext.assertMatchingRecords(appendRecords, fetchedRecords);
    }

    @Test
    public void testPurgatoryFetchCompletedByFollowerTransition() throws Exception {
        int localId = 0;
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        context.deliverRequest(context.fetchRequest(epoch, voter2, 1L, epoch, 500));
        context.client.poll();
        assertTrue(context.channel.drainSendQueue().stream()
            .noneMatch(msg -> msg.data() instanceof FetchResponseData));

        // Now we get a BeginEpoch from the other voter and become a follower
        context.deliverRequest(context.beginEpochRequest(epoch + 1, voter3));
        context.client.poll();
        context.assertElectedLeader(epoch + 1, voter3);

        // We expect the BeginQuorumEpoch response and a failed Fetch response
        context.assertSentBeginQuorumEpochResponse(Errors.NONE, epoch + 1, OptionalInt.of(voter3));

        // The fetch should be satisfied immediately and return an error
        MemoryRecords fetchedRecords = context.assertSentFetchResponse(
            Errors.NOT_LEADER_OR_FOLLOWER, epoch + 1, OptionalInt.of(voter3));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    @Test
    public void testFetchResponseIgnoredAfterBecomingCandidate() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        // The other node starts out as the leader
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        // Wait until we have a Fetch inflight to the leader
        context.pollUntilSend();
        int fetchCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);

        // Now await the fetch timeout and become a candidate
        context.time.sleep(context.fetchTimeoutMs);
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, context.localId);

        // The fetch response from the old leader returns, but it should be ignored
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        context.deliverResponse(fetchCorrelationId, otherNodeId,
            context.fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE));

        context.client.poll();
        assertEquals(0, context.log.endOffset().offset);
        context.assertVotedCandidate(epoch + 1, context.localId);
    }

    @Test
    public void testFetchResponseIgnoredAfterBecomingFollowerOfDifferentLeader() throws Exception {
        int localId = 0;
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;
        // Start out with `voter2` as the leader
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, voter2)
            .build();
        context.assertElectedLeader(epoch, voter2);

        // Wait until we have a Fetch inflight to the leader
        context.pollUntilSend();
        int fetchCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);

        // Now receive a BeginEpoch from `voter3`
        context.deliverRequest(context.beginEpochRequest(epoch + 1, voter3));
        context.client.poll();
        context.assertElectedLeader(epoch + 1, voter3);

        // The fetch response from the old leader returns, but it should be ignored
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        FetchResponseData response = context.fetchResponse(epoch, voter2, records, 0L, Errors.NONE);
        context.deliverResponse(fetchCorrelationId, voter2, response);

        context.client.poll();
        assertEquals(0, context.log.endOffset().offset);
        context.assertElectedLeader(epoch + 1, voter3);
    }

    @Test
    public void testVoteResponseIgnoredAfterBecomingFollower() throws Exception {
        int localId = 0;
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;
        // This node initializes as a candidate
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(epoch, voter1)
            .build();
        context.assertVotedCandidate(epoch, voter1);

        // Wait until the vote requests are inflight
        context.pollUntilSend();
        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // While the vote requests are still inflight, we receive a BeginEpoch for the same epoch
        context.deliverRequest(context.beginEpochRequest(epoch, voter3));
        context.client.poll();
        context.assertElectedLeader(epoch, voter3);

        // The vote requests now return and should be ignored
        VoteResponseData voteResponse1 = context.voteResponse(false, Optional.empty(), epoch);
        context.deliverResponse(voteRequests.get(0).correlationId, voter2, voteResponse1);

        VoteResponseData voteResponse2 = context.voteResponse(false, Optional.of(voter3), epoch);
        context.deliverResponse(voteRequests.get(1).correlationId, voter3, voteResponse2);

        context.client.poll();
        context.assertElectedLeader(epoch, voter3);
    }

    @Test
    public void testObserverLeaderRediscoveryAfterBrokerNotAvailableError() throws Exception {
        int localId = 0;
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        context.discoverLeaderAsObserver(leaderId, epoch);

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destinationId());
        context.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.deliverResponse(fetchRequest1.correlationId, fetchRequest1.destinationId(),
            context.fetchResponse(epoch, -1, MemoryRecords.EMPTY, -1, Errors.BROKER_NOT_AVAILABLE));
        context.pollUntilSend();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destinationId());
        assertTrue(voters.contains(fetchRequest2.destinationId()));
        context.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        Errors error = fetchRequest2.destinationId() == leaderId ?
            Errors.NONE : Errors.NOT_LEADER_OR_FOLLOWER;
        context.deliverResponse(fetchRequest2.correlationId, fetchRequest2.destinationId(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, error));
        context.client.poll();

        context.assertElectedLeader(epoch, leaderId);
    }

    @Test
    public void testObserverLeaderRediscoveryAfterRequestTimeout() throws Exception {
        int localId = 0;
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        context.discoverLeaderAsObserver(leaderId, epoch);

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destinationId());
        context.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.time.sleep(context.requestTimeoutMs);
        context.pollUntilSend();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destinationId());
        assertTrue(voters.contains(fetchRequest2.destinationId()));
        context.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        context.deliverResponse(fetchRequest2.correlationId, fetchRequest2.destinationId(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        context.client.poll();

        context.assertElectedLeader(epoch, leaderId);
    }

    @Test
    public void testLeaderGracefulShutdown() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isShuttingDown());
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other voter
        context.client.poll();
        assertTrue(context.client.isShuttingDown());
        assertTrue(context.client.isRunning());
        context.assertSentEndQuorumEpochRequest(1, OptionalInt.of(context.localId), otherNodeId);

        // We should still be able to handle vote requests during graceful shutdown
        // in order to help the new leader get elected
        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, epoch, 1L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);

        // Graceful shutdown completes when a new leader is elected
        context.deliverRequest(context.beginEpochRequest(2, otherNodeId));

        TestUtils.waitForCondition(() -> {
            context.client.poll();
            return !context.client.isRunning();
        }, 5000, "Client failed to shutdown before expiration of timeout");
        assertFalse(context.client.isShuttingDown());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @Test
    public void testEndQuorumEpochSentBasedOnFetchOffset() throws Exception {
        int localId = 0;
        int closeFollower = 2;
        int laggingFollower = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, closeFollower, laggingFollower);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.buildFollowerSet(epoch, closeFollower, laggingFollower);

        // Now shutdown
        context.client.shutdown(context.electionTimeoutMs * 2);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isRunning());

        // Send EndQuorumEpoch request to the close follower
        context.client.poll();
        assertTrue(context.client.isRunning());

        List<RaftRequest.Outbound> endQuorumRequests =
            context.collectEndQuorumRequests(1, OptionalInt.of(context.localId), Utils.mkSet(closeFollower, laggingFollower));

        assertEquals(2, endQuorumRequests.size());
    }

    @Test
    public void testDescribeQuorum() throws Exception {
        int localId = 0;
        int closeFollower = 2;
        int laggingFollower = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, closeFollower, laggingFollower);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.buildFollowerSet(epoch, closeFollower, laggingFollower);

        // Create observer
        int observerId = 3;
        context.deliverRequest(context.fetchRequest(epoch, observerId, 0L, 0, 0));

        context.client.poll();

        long highWatermark = 1L;
        context.assertSentFetchResponse(highWatermark, epoch);

        context.deliverRequest(DescribeQuorumRequest.singletonRequest(context.metadataPartition));

        context.client.poll();

        context.assertSentDescribeQuorumResponse(context.localId, epoch, highWatermark,
            Arrays.asList(
                new ReplicaState()
                    .setReplicaId(context.localId)
                    // As we are appending the records directly to the log,
                    // the leader end offset hasn't been updated yet.
                    .setLogEndOffset(3L),
                new ReplicaState()
                    .setReplicaId(laggingFollower)
                    .setLogEndOffset(0L),
                new ReplicaState()
                    .setReplicaId(closeFollower)
                    .setLogEndOffset(1L)),
            singletonList(
                new ReplicaState()
                    .setReplicaId(observerId)
                    .setLogEndOffset(0L)));
    }

    @Test
    public void testLeaderGracefulShutdownTimeout() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other vote
        context.client.poll();
        assertTrue(context.client.isRunning());

        context.assertSentEndQuorumEpochRequest(epoch, OptionalInt.of(context.localId), otherNodeId);

        // The shutdown timeout is hit before we receive any requests or responses indicating an epoch bump
        context.time.sleep(shutdownTimeoutMs);

        context.client.poll();
        assertFalse(context.client.isRunning());
        assertTrue(shutdownFuture.isCompletedExceptionally());
        assertFutureThrows(shutdownFuture, TimeoutException.class);
    }

    @Test
    public void testFollowerGracefulShutdown() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.client.poll();

        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        context.client.poll();
        assertFalse(context.client.isRunning());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @Test
    public void testGracefulShutdownSingleMemberQuorum() throws IOException {
        int localId = 0;
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, Collections.singleton(localId)).build();

        context.assertElectedLeader(1, context.localId);
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());
        int shutdownTimeoutMs = 5000;
        context.client.shutdown(shutdownTimeoutMs);
        assertTrue(context.client.isRunning());
        context.client.poll();
        assertFalse(context.client.isRunning());
    }

    @Test
    public void testFollowerReplication() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilSend();

        int fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        FetchResponseData response = context.fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, response);

        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(2L, context.log.lastFlushedOffset());
    }

    @Test
    public void testEmptyRecordSetInFetchResponse() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        // Receive an empty fetch response
        context.pollUntilSend();
        int fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);
        FetchResponseData fetchResponse = context.fetchResponse(epoch, otherNodeId,
            MemoryRecords.EMPTY, 0L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        context.client.poll();
        assertEquals(0L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // Receive some records in the next poll, but do not advance high watermark
        context.pollUntilSend();
        Records records = context.buildBatch(0L, epoch, Arrays.asList("a", "b"));
        fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);
        fetchResponse = context.fetchResponse(epoch, otherNodeId,
            records, 0L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // The next fetch response is empty, but should still advance the high watermark
        context.pollUntilSend();
        fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 2L, epoch);
        fetchResponse = context.fetchResponse(epoch, otherNodeId,
            MemoryRecords.EMPTY, 2L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), context.client.highWatermark());
    }

    @Test
    public void testFetchShouldBeTreatedAsLeaderEndorsement() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(random -> {
                Mockito.doReturn(0).when(random).nextInt(DEFAULT_ELECTION_TIMEOUT_MS);
            })
            .withUnknownLeader(epoch - 1)
            .build();

        context.time.sleep(context.electionTimeoutMs);
        context.expectAndGrantVotes(epoch);

        context.pollUntilSend();

        // We send BeginEpoch, but it gets lost and the destination finds the leader through the Fetch API
        context.assertSentBeginQuorumEpochRequest(epoch);

        context.deliverRequest(context.fetchRequest(
            epoch, otherNodeId, 0L, 0, 500));

        context.client.poll();

        // The BeginEpoch request eventually times out. We should not send another one.
        context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(context.localId));
        context.time.sleep(context.requestTimeoutMs);

        context.client.poll();

        List<RaftMessage> sentMessages = context.channel.drainSendQueue();
        assertEquals(0, sentMessages.size());
    }

    @Test
    public void testLeaderAppendSingleMemberQuorum() throws IOException {
        int localId = 0;
        Set<Integer> voters = Collections.singleton(localId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();
        long now = context.time.milliseconds();

        context.assertElectedLeader(1, context.localId);

        // We still write the leader change message
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        String[] appendRecords = new String[] {"a", "b", "c"};

        // First poll has no high watermark advance
        context.client.poll();
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        context.client.scheduleAppend(context.currentEpoch(), Arrays.asList(appendRecords));

        // Then poll the appended data with leader change record
        context.client.poll();
        assertEquals(OptionalLong.of(4L), context.client.highWatermark());

        // Now try reading it
        int otherNodeId = 1;
        context.deliverRequest(context.fetchRequest(1, otherNodeId, 0L, 0, 500));

        context.client.poll();

        MemoryRecords fetchedRecords = context.assertSentFetchResponse(Errors.NONE, 1, OptionalInt.of(context.localId));
        List<MutableRecordBatch> batches = Utils.toList(fetchedRecords.batchIterator());
        assertEquals(2, batches.size());

        MutableRecordBatch leaderChangeBatch = batches.get(0);
        assertTrue(leaderChangeBatch.isControlBatch());
        List<Record> readRecords = Utils.toList(leaderChangeBatch.iterator());
        assertEquals(1, readRecords.size());

        Record record = readRecords.get(0);
        assertEquals(now, record.timestamp());
        RaftClientTestContext.verifyLeaderChangeMessage(context.localId, Collections.emptyList(),
            record.key(), record.value());

        MutableRecordBatch batch = batches.get(1);
        assertEquals(1, batch.partitionLeaderEpoch());
        readRecords = Utils.toList(batch.iterator());
        assertEquals(3, readRecords.size());

        for (int i = 0; i < appendRecords.length; i++) {
            assertEquals(appendRecords[i], Utils.utf8(readRecords.get(i).value()));
        }
    }

    @Test
    public void testFollowerLogReconciliation() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .appendToLog(0L, lastEpoch, Arrays.asList("foo", "bar"))
            .appendToLog(2L, lastEpoch, Arrays.asList("baz"))
            .build();

        context.assertElectedLeader(epoch, otherNodeId);
        assertEquals(3L, context.log.endOffset().offset);

        context.pollUntilSend();

        int correlationId = context.assertSentFetchRequest(epoch, 3L, lastEpoch);

        FetchResponseData response = context.outOfRangeFetchRecordsResponse(epoch, otherNodeId, 2L,
            lastEpoch, 1L);
        context.deliverResponse(correlationId, otherNodeId, response);

        // Poll again to complete truncation
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);

        // Now we should be fetching
        context.client.poll();
        context.assertSentFetchRequest(epoch, 2L, lastEpoch);
    }

    @Test
    public void testMetrics() throws Exception {
        int localId = 0;
        int epoch = 1;
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, Collections.singleton(localId))
            .build();

        assertNotNull(getMetric(context.metrics, "current-state"));
        assertNotNull(getMetric(context.metrics, "current-leader"));
        assertNotNull(getMetric(context.metrics, "current-vote"));
        assertNotNull(getMetric(context.metrics, "current-epoch"));
        assertNotNull(getMetric(context.metrics, "high-watermark"));
        assertNotNull(getMetric(context.metrics, "log-end-offset"));
        assertNotNull(getMetric(context.metrics, "log-end-epoch"));
        assertNotNull(getMetric(context.metrics, "number-unknown-voter-connections"));
        assertNotNull(getMetric(context.metrics, "poll-idle-ratio-avg"));
        assertNotNull(getMetric(context.metrics, "commit-latency-avg"));
        assertNotNull(getMetric(context.metrics, "commit-latency-max"));
        assertNotNull(getMetric(context.metrics, "election-latency-avg"));
        assertNotNull(getMetric(context.metrics, "election-latency-max"));
        assertNotNull(getMetric(context.metrics, "fetch-records-rate"));
        assertNotNull(getMetric(context.metrics, "append-records-rate"));

        assertEquals("leader", getMetric(context.metrics, "current-state").metricValue());
        assertEquals((double) context.localId, getMetric(context.metrics, "current-leader").metricValue());
        assertEquals((double) context.localId, getMetric(context.metrics, "current-vote").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "current-epoch").metricValue());
        assertEquals((double) 1L, getMetric(context.metrics, "high-watermark").metricValue());
        assertEquals((double) 1L, getMetric(context.metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "log-end-epoch").metricValue());

        context.client.scheduleAppend(epoch, Arrays.asList("a", "b", "c"));
        context.client.poll();

        assertEquals((double) 4L, getMetric(context.metrics, "high-watermark").metricValue());
        assertEquals((double) 4L, getMetric(context.metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "log-end-epoch").metricValue());

        CompletableFuture<Void> shutdownFuture = context.client.shutdown(100);
        context.client.poll();
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());

        // should only have total-metrics-count left
        assertEquals(1, context.metrics.metrics().size());
    }

    @Test
    public void testClusterAuthorizationFailedInFetch() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();

        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilSend();

        int correlationId = context.assertSentFetchRequest(epoch, 0, 0);
        FetchResponseData response = new FetchResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInBeginQuorumEpoch() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(random -> {
                Mockito.doReturn(0).when(random).nextInt(DEFAULT_ELECTION_TIMEOUT_MS);
            })
            .withUnknownLeader(epoch - 1)
            .build();

        context.time.sleep(context.electionTimeoutMs);
        context.expectAndGrantVotes(epoch);

        context.pollUntilSend();
        int correlationId = context.assertSentBeginQuorumEpochRequest(epoch);
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInVote() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(epoch, localId)
            .build();

        context.assertVotedCandidate(epoch, context.localId);

        context.pollUntilSend();
        int correlationId = context.assertSentVoteRequest(epoch, 0, 0L);
        VoteResponseData response = new VoteResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInEndQuorumEpoch() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.client.shutdown(5000);
        context.pollUntilSend();

        int correlationId = context.assertSentEndQuorumEpochRequest(epoch, OptionalInt.of(context.localId), otherNodeId);
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @Test
    public void testHandleClaimFiresImmediatelyOnEmptyLog() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);
        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());
    }

    @Test
    public void testHandleClaimCallbackFiresAfterHighWatermarkReachesEpochStartOffset() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        List<String> batch1 = Arrays.asList("1", "2", "3");
        List<String> batch2 = Arrays.asList("4", "5", "6");
        List<String> batch3 = Arrays.asList("7", "8", "9");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(0L, 1, batch1)
            .appendToLog(3L, 1, batch2)
            .appendToLog(6L, 2, batch3)
            .withUnknownLeader(epoch - 1)
            .build();

        context.becomeLeader();
        context.client.poll();

        // The high watermark is not known to the leader until the followers
        // begin fetching, so we should not have fired the `handleClaim` callback.
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Deliver a fetch from the other voter. The high watermark will not
        // be exposed until it is able to reach the start of the leader epoch,
        // so we are unable to deliver committed data or fire `handleClaim`.
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 3L, 1, 500));
        context.client.poll();
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Now catch up to the start of the leader epoch so that the high
        // watermark advances and we can start sending committed data to the
        // listener.
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 9L, 2, 500));
        context.client.poll();
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
        assertEquals(3, context.listener.numCommittedBatches());
        assertEquals(batch1, context.listener.commitWithBaseOffset(0L));
        assertEquals(batch2, context.listener.commitWithBaseOffset(3L));
        assertEquals(batch3, context.listener.commitWithBaseOffset(6L));
        assertEquals(OptionalLong.of(8L), context.listener.lastCommitOffset());

        // Now that the listener has caught up to the start of the leader epoch,
        // we expect the `handleClaim` callback.
        context.client.poll();
        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());
    }

    @Test
    public void testLateRegisteredListenerCatchesUp() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        List<String> batch1 = Arrays.asList("1", "2", "3");
        List<String> batch2 = Arrays.asList("4", "5", "6");
        List<String> batch3 = Arrays.asList("7", "8", "9");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(0L, 1, batch1)
            .appendToLog(3L, 1, batch2)
            .appendToLog(6L, 2, batch3)
            .withUnknownLeader(epoch - 1)
            .build();

        context.becomeLeader();
        context.client.poll();

        // Let the initial listener catch up
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 9L, 2, 500));
        context.client.poll();
        assertEquals(OptionalLong.of(9L), context.client.highWatermark());
        context.client.poll();
        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());

        // Register a second listener and allow it to catch up to the high watermark
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener();
        context.client.register(secondListener);
        context.client.poll();
        assertEquals(OptionalLong.of(8L), secondListener.lastCommitOffset());
        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());

        // Ensure that the `handleClaim` callback was not fired early
        assertEquals(9L, context.listener.claimedEpochStartOffset(epoch));
    }

    @Test
    public void testHandleCommitCallbackFiresAfterFollowerHighWatermarkAdvances() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .build();
        assertEquals(OptionalLong.empty(), context.client.highWatermark());

        // Poll for our first fetch request
        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // The response does not advance the high watermark
        List<String> records1 = Arrays.asList("a", "b", "c");
        MemoryRecords batch1 = context.buildBatch(0L, 3, records1);
        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            context.fetchResponse(epoch, otherNodeId, batch1, 0L, Errors.NONE));
        context.client.poll();

        // The listener should not have seen any data
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());
        assertEquals(0, context.listener.numCommittedBatches());
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());

        // Now look for the next fetch request
        context.pollUntilSend();
        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, epoch, 3L, 3);

        // The high watermark advances to include the first batch we fetched
        List<String> records2 = Arrays.asList("d", "e", "f");
        MemoryRecords batch2 = context.buildBatch(3L, 3, records2);
        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            context.fetchResponse(epoch, otherNodeId, batch2, 3L, Errors.NONE));
        context.client.poll();

        // The listener should have seen only the data from the first batch
        assertEquals(OptionalLong.of(3L), context.client.highWatermark());
        assertEquals(1, context.listener.numCommittedBatches());
        assertEquals(OptionalLong.of(2L), context.listener.lastCommitOffset());
        assertEquals(records1, context.listener.lastCommit().records());
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
    }

    @Test
    public void testHandleCommitCallbackFiresInVotedState() throws Exception {
        // This test verifies that the state machine can still catch up even while
        // an election is in progress as long as the high watermark is known.

        int localId = 0;
        int otherNodeId = 1;
        int epoch = 7;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(0L, 2, Arrays.asList("a", "b", "c"))
            .appendToLog(3L, 4, Arrays.asList("d", "e", "f"))
            .appendToLog(6L, 4, Arrays.asList("g", "h", "i"))
            .withUnknownLeader(epoch - 1)
            .build();

        // Start off as the leader and receive a fetch to initialize the high watermark
        context.becomeLeader();
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 10L, epoch, 500));
        context.client.poll();
        assertEquals(OptionalLong.of(10L), context.client.highWatermark());

        // Now we receive a vote request which transitions us to the 'voted' state
        int candidateEpoch = epoch + 1;
        context.deliverRequest(context.voteRequest(candidateEpoch, otherNodeId, epoch, 10L));
        context.client.poll();
        context.assertVotedCandidate(candidateEpoch, otherNodeId);
        assertEquals(OptionalLong.of(10L), context.client.highWatermark());

        // Register another listener and verify that it catches up while we remain 'voted'
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener();
        context.client.register(secondListener);
        context.client.poll();
        context.assertVotedCandidate(candidateEpoch, otherNodeId);

        // Note the offset is 8 because the record at offset 9 is a control record
        assertEquals(OptionalLong.of(8L), secondListener.lastCommitOffset());
        assertEquals(OptionalInt.empty(), secondListener.currentClaimedEpoch());
    }

    @Test
    public void testHandleCommitCallbackFiresInCandidateState() throws Exception {
        // This test verifies that the state machine can still catch up even while
        // an election is in progress as long as the high watermark is known.

        int localId = 0;
        int otherNodeId = 1;
        int epoch = 7;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(0L, 2, Arrays.asList("a", "b", "c"))
            .appendToLog(3L, 4, Arrays.asList("d", "e", "f"))
            .appendToLog(6L, 4, Arrays.asList("g", "h", "i"))
            .withUnknownLeader(epoch - 1)
            .build();

        // Start off as the leader and receive a fetch to initialize the high watermark
        context.becomeLeader();
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 9L, epoch, 500));
        context.client.poll();
        assertEquals(OptionalLong.of(9L), context.client.highWatermark());

        // Now we receive a vote request which transitions us to the 'unattached' state
        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeId, epoch, 9L));
        context.client.poll();
        context.assertUnknownLeader(epoch + 1);
        assertEquals(OptionalLong.of(9L), context.client.highWatermark());

        // Timeout the election and become candidate
        int candidateEpoch = epoch + 2;
        context.time.sleep(context.electionTimeoutMs * 2);
        context.client.poll();
        context.assertVotedCandidate(candidateEpoch, localId);

        // Register another listener and verify that it catches up
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener();
        context.client.register(secondListener);
        context.client.poll();
        context.assertVotedCandidate(candidateEpoch, localId);

        // Note the offset is 8 because the record at offset 9 is a control record
        assertEquals(OptionalLong.of(8L), secondListener.lastCommitOffset());
        assertEquals(OptionalInt.empty(), secondListener.currentClaimedEpoch());
    }

    private static KafkaMetric getMetric(final Metrics metrics, final String name) {
        return metrics.metrics().get(metrics.metricName(name, "raft-metrics"));
    }

}
