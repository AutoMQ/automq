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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.BatchBuilder;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.test.TestUtils;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RaftClientTestContext {
    private static final StringSerde STRING_SERDE = new StringSerde();

    final TopicPartition metadataPartition = Builder.METADATA_PARTITION;
    final int electionBackoffMaxMs = Builder.ELECTION_BACKOFF_MAX_MS;
    final int electionTimeoutMs = Builder.DEFAULT_ELECTION_TIMEOUT_MS;
    final int electionFetchMaxWaitMs = Builder.FETCH_MAX_WAIT_MS;
    final int fetchTimeoutMs = Builder.FETCH_TIMEOUT_MS;
    final int requestTimeoutMs = Builder.REQUEST_TIMEOUT_MS;
    final int retryBackoffMs = Builder.RETRY_BACKOFF_MS;

    private final QuorumStateStore quorumStateStore;
    final int localId;
    final KafkaRaftClient<String> client;
    final Metrics metrics;
    final MockLog log;
    final MockNetworkChannel channel;
    final MockTime time;
    final MockListener listener;

    final Set<Integer> voters;

    public static final class Builder {
        static final int DEFAULT_ELECTION_TIMEOUT_MS = 10000;

        private static final TopicPartition METADATA_PARTITION = new TopicPartition("metadata", 0);
        private static final int ELECTION_BACKOFF_MAX_MS = 100;
        private static final int FETCH_MAX_WAIT_MS = 0;
        // fetch timeout is usually larger than election timeout
        private static final int FETCH_TIMEOUT_MS = 50000;
        private static final int REQUEST_TIMEOUT_MS = 5000;
        private static final int RETRY_BACKOFF_MS = 50;
        private static final int DEFAULT_APPEND_LINGER_MS = 0;

        private final MockTime time = new MockTime();
        private final QuorumStateStore quorumStateStore = new MockQuorumStateStore();
        private final Random random = Mockito.spy(new Random(1));
        private final MockLog log = new MockLog(METADATA_PARTITION);
        private final Set<Integer> voters;
        private final int localId;

        private int electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;
        private int appendLingerMs = DEFAULT_APPEND_LINGER_MS;
        private MemoryPool memoryPool = MemoryPool.NONE;

        Builder(int localId, Set<Integer> voters) {
            this.voters = voters;
            this.localId = localId;
        }

        Builder withElectedLeader(int epoch, int leaderId) throws IOException {
            quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, leaderId, voters));
            return this;
        }

        Builder withUnknownLeader(int epoch) throws IOException {
            quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));
            return this;
        }

        Builder withVotedCandidate(int epoch, int votedId) throws IOException {
            quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, votedId, voters));
            return this;
        }

        Builder updateRandom(Consumer<Random> consumer) {
            consumer.accept(random);
            return this;
        }

        Builder withMemoryPool(MemoryPool pool) {
            this.memoryPool = pool;
            return this;
        }

        Builder withAppendLingerMs(int appendLingerMs) {
            this.appendLingerMs = appendLingerMs;
            return this;
        }

        Builder appendToLog(long baseOffset, int epoch, List<String> records) {
            MemoryRecords batch = buildBatch(time.milliseconds(), baseOffset, epoch, records);
            log.appendAsLeader(batch, epoch);
            return this;
        }

        RaftClientTestContext build() throws IOException {
            Metrics metrics = new Metrics(time);
            MockNetworkChannel channel = new MockNetworkChannel();
            LogContext logContext = new LogContext();
            QuorumState quorum = new QuorumState(localId, voters, electionTimeoutMs, FETCH_TIMEOUT_MS,
                    quorumStateStore, time, logContext, random);
            MockListener listener = new MockListener();

            Map<Integer, InetSocketAddress> voterAddresses = voters
                .stream()
                .collect(Collectors.toMap(
                    Function.identity(),
                    RaftClientTestContext::mockAddress
                ));

            KafkaRaftClient<String> client = new KafkaRaftClient<>(
                STRING_SERDE,
                channel,
                log,
                quorum,
                memoryPool,
                time,
                metrics,
                new MockExpirationService(time),
                voterAddresses,
                ELECTION_BACKOFF_MAX_MS,
                RETRY_BACKOFF_MS,
                REQUEST_TIMEOUT_MS,
                FETCH_MAX_WAIT_MS,
                appendLingerMs,
                logContext,
                random
            );

            client.register(listener);
            client.initialize();

            return new RaftClientTestContext(
                localId,
                client,
                log,
                channel,
                time,
                quorumStateStore,
                voters,
                metrics,
                listener
            );
        }
    }

    private RaftClientTestContext(
        int localId,
        KafkaRaftClient<String> client,
        MockLog log,
        MockNetworkChannel channel,
        MockTime time,
        QuorumStateStore quorumStateStore,
        Set<Integer> voters,
        Metrics metrics,
        MockListener listener
    ) {
        this.localId = localId;
        this.client = client;
        this.log = log;
        this.channel = channel;
        this.time = time;
        this.quorumStateStore = quorumStateStore;
        this.voters = voters;
        this.metrics = metrics;
        this.listener = listener;
    }

    MemoryRecords buildBatch(
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        return buildBatch(time.milliseconds(), baseOffset, epoch, records);
    }

    static MemoryRecords buildBatch(
        long timestamp,
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        BatchBuilder<String> builder = new BatchBuilder<>(
            buffer,
            STRING_SERDE,
            CompressionType.NONE,
            baseOffset,
            timestamp,
            false,
            epoch,
            512
        );

        for (String record : records) {
            builder.appendRecord(record, null);
        }

        return builder.build();
    }

    static RaftClientTestContext initializeAsLeader(int localId, Set<Integer> voters, int epoch) throws Exception {
        if (epoch <= 0) {
            throw new IllegalArgumentException("Cannot become leader in epoch " + epoch);
        }

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch - 1)
            .build();

        context.assertUnknownLeader(epoch - 1);
        context.becomeLeader();
        return context;
    }

    void becomeLeader() throws Exception {
        int currentEpoch = currentEpoch();
        time.sleep(electionTimeoutMs * 2);
        expectAndGrantVotes(currentEpoch + 1);
        expectBeginEpoch(currentEpoch + 1);
    }

    OptionalInt currentLeader() {
        return currentLeaderAndEpoch().leaderId;
    }

    int currentEpoch() {
        return currentLeaderAndEpoch().epoch;
    }

    LeaderAndEpoch currentLeaderAndEpoch() {
        try {
            ElectionState election = quorumStateStore.readElectionState();
            return new LeaderAndEpoch(election.leaderIdOpt, election.epoch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void expectAndGrantVotes(
        int epoch
    ) throws Exception {
        pollUntilSend();

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch,
            log.lastFetchedEpoch(), log.endOffset().offset);

        for (RaftRequest.Outbound request : voteRequests) {
            VoteResponseData voteResponse = voteResponse(true, Optional.empty(), epoch);
            deliverResponse(request.correlationId, request.destinationId(), voteResponse);
        }

        client.poll();
        assertElectedLeader(epoch, localId);
    }

    void expectBeginEpoch(
        int epoch
    ) throws Exception {
        pollUntilSend();
        for (RaftRequest.Outbound request : collectBeginEpochRequests(epoch)) {
            BeginQuorumEpochResponseData beginEpochResponse = beginEpochResponse(epoch, localId);
            deliverResponse(request.correlationId, request.destinationId(), beginEpochResponse);
        }
        client.poll();
    }

    void pollUntilSend() throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            client.poll();
            return channel.hasSentMessages();
        }, 5000, "Condition failed to be satisfied before timeout");
    }

    void assertVotedCandidate(int epoch, int leaderId) throws IOException {
        assertEquals(ElectionState.withVotedCandidate(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    void assertElectedLeader(int epoch, int leaderId) throws IOException {
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    void assertUnknownLeader(int epoch) throws IOException {
        assertEquals(ElectionState.withUnknownLeader(epoch, voters), quorumStateStore.readElectionState());
    }

    int assertSentDescribeQuorumResponse(int leaderId,
                                                 int leaderEpoch,
                                                 long highWatermark,
                                                 List<ReplicaState> voterStates,
                                                 List<ReplicaState> observerStates) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(
            raftMessage.data() instanceof DescribeQuorumResponseData,
            "Unexpected request type " + raftMessage.data());
        DescribeQuorumResponseData response = (DescribeQuorumResponseData) raftMessage.data();

        DescribeQuorumResponseData expectedResponse = DescribeQuorumResponse.singletonResponse(
            metadataPartition,
            leaderId,
            leaderEpoch,
            highWatermark,
            voterStates,
            observerStates);

        assertEquals(expectedResponse, response);
        return raftMessage.correlationId();
    }

    int assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(1, voteRequests.size());
        return voteRequests.iterator().next().correlationId();
    }

    void assertSentVoteResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId,
        boolean voteGranted
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof VoteResponseData);
        VoteResponseData response = (VoteResponseData) raftMessage.data();
        assertTrue(hasValidTopicPartition(response, metadataPartition));

        VoteResponseData.PartitionData partitionResponse = response.topics().get(0).partitions().get(0);

        assertEquals(voteGranted, partitionResponse.voteGranted());
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
    }

    List<RaftRequest.Outbound> collectVoteRequests(
        int epoch,
        int lastEpoch,
        long lastEpochOffset
    ) {
        List<RaftRequest.Outbound> voteRequests = new ArrayList<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof VoteRequestData) {
                VoteRequestData request = (VoteRequestData) raftMessage.data();
                VoteRequestData.PartitionData partitionRequest = unwrap(request);

                assertEquals(epoch, partitionRequest.candidateEpoch());
                assertEquals(localId, partitionRequest.candidateId());
                assertEquals(lastEpoch, partitionRequest.lastOffsetEpoch());
                assertEquals(lastEpochOffset, partitionRequest.lastOffset());
                voteRequests.add((RaftRequest.Outbound) raftMessage);
            }
        }
        return voteRequests;
    }

    void deliverRequest(ApiMessage request) {
        RaftRequest.Inbound message = new RaftRequest.Inbound(channel.newCorrelationId(), request, time.milliseconds());
        channel.mockReceive(message);
    }

    void deliverResponse(int correlationId, int sourceId, ApiMessage response) {
        channel.mockReceive(new RaftResponse.Inbound(correlationId, response, sourceId));
    }

    int assertSentBeginQuorumEpochRequest(int epoch) {
        List<RaftRequest.Outbound> requests = collectBeginEpochRequests(epoch);
        assertEquals(1, requests.size());
        return requests.get(0).correlationId;
    }

    void assertSentBeginQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginQuorumEpochResponseData);
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        BeginQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(partitionError, Errors.forCode(partitionResponse.errorCode()));
    }
    
    int assertSentEndQuorumEpochRequest(int epoch, OptionalInt leaderId, int destinationId) {
        List<RaftRequest.Outbound> endQuorumRequests = collectEndQuorumRequests(
            epoch, leaderId, Collections.singleton(destinationId));
        assertEquals(1, endQuorumRequests.size());
        return endQuorumRequests.get(0).correlationId();
    }

    void assertSentEndQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof EndQuorumEpochResponseData);
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        EndQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(partitionError, Errors.forCode(partitionResponse.errorCode()));
    }

    RaftRequest.Outbound assertSentFetchRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(ApiKeys.FETCH);
        assertEquals(1, sentRequests.size());
        return sentRequests.get(0);
    }

    int assertSentFetchRequest(
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertFetchRequestData(raftMessage, epoch, fetchOffset, lastFetchedEpoch);
        return raftMessage.correlationId();
    }

    MemoryRecords assertSentFetchResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        FetchResponseData.FetchablePartitionResponse partitionResponse = assertSentPartitionResponse();
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.currentLeader().leaderId());
        return (MemoryRecords) partitionResponse.recordSet();
    }

    MemoryRecords assertSentFetchResponse(
        long highWatermark,
        int leaderEpoch
    ) {
        FetchResponseData.FetchablePartitionResponse partitionResponse = assertSentPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderEpoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(highWatermark, partitionResponse.highWatermark());
        return (MemoryRecords) partitionResponse.recordSet();
    }

    void buildFollowerSet(
        int epoch,
        int closeFollower,
        int laggingFollower
    ) throws Exception {
        // The lagging follower fetches first
        deliverRequest(fetchRequest(1, laggingFollower, 0L, 0, 0));

        client.poll();

        assertSentFetchResponse(0L, epoch);

        // Append some records, so that the close follower will be able to advance further.
        client.scheduleAppend(epoch, Arrays.asList("foo", "bar"));
        client.poll();

        deliverRequest(fetchRequest(epoch, closeFollower, 1L, epoch, 0));

        client.poll();

        assertSentFetchResponse(1L, epoch);
    }

    List<RaftRequest.Outbound> collectEndQuorumRequests(int epoch, OptionalInt leaderId, Set<Integer> destinationIdSet) {
        List<RaftRequest.Outbound> endQuorumRequests = new ArrayList<>();
        Set<Integer> collectedDestinationIdSet = new HashSet<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof EndQuorumEpochRequestData) {
                EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) raftMessage.data();

                EndQuorumEpochRequestData.PartitionData partitionRequest =
                    request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.leaderEpoch());
                assertEquals(leaderId.orElse(-1), partitionRequest.leaderId());
                assertEquals(localId, partitionRequest.replicaId());

                RaftRequest.Outbound outboundRequest = (RaftRequest.Outbound) raftMessage;
                collectedDestinationIdSet.add(outboundRequest.destinationId());
                endQuorumRequests.add(outboundRequest);
            }
        }
        assertEquals(destinationIdSet, collectedDestinationIdSet);
        return endQuorumRequests;
    }

    void discoverLeaderAsObserver(
        int leaderId,
        int epoch
    ) throws Exception {
        pollUntilSend();
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE));
        client.poll();
        assertElectedLeader(epoch, leaderId);
    }

    private List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        for (RaftRequest.Outbound raftRequest : channel.drainSentRequests(ApiKeys.BEGIN_QUORUM_EPOCH)) {
            assertTrue(raftRequest.data() instanceof BeginQuorumEpochRequestData);
            BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftRequest.data();

            BeginQuorumEpochRequestData.PartitionData partitionRequest =
                request.topics().get(0).partitions().get(0);

            assertEquals(epoch, partitionRequest.leaderEpoch());
            assertEquals(localId, partitionRequest.leaderId());
            requests.add(raftRequest);
        }
        return requests;
    }

    private FetchResponseData.FetchablePartitionResponse assertSentPartitionResponse() {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1, sentMessages.size(), "Found unexpected sent messages " + sentMessages);
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH.id, raftMessage.data.apiKey());
        FetchResponseData response = (FetchResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        assertEquals(1, response.responses().size());
        assertEquals(metadataPartition.topic(), response.responses().get(0).topic());
        assertEquals(1, response.responses().get(0).partitionResponses().size());
        return response.responses().get(0).partitionResponses().get(0);
    }

    private static InetSocketAddress mockAddress(int id) {
        return new InetSocketAddress("localhost", 9990 + id);
    }

    EndQuorumEpochRequestData endEpochRequest(
        int epoch,
        OptionalInt leaderId,
        int replicaId,
        List<Integer> preferredSuccessors) {
        return EndQuorumEpochRequest.singletonRequest(
            metadataPartition,
            replicaId,
            epoch,
            leaderId.orElse(-1),
            preferredSuccessors
        );
    }

    BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId) {
        return BeginQuorumEpochRequest.singletonRequest(
            metadataPartition,
            epoch,
            leaderId
        );
    }

    private BeginQuorumEpochResponseData beginEpochResponse(int epoch, int leaderId) {
        return BeginQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId
        );
    }

    VoteRequestData voteRequest(int epoch, int candidateId, int lastEpoch, long lastEpochOffset) {
        return VoteRequest.singletonRequest(
            metadataPartition,
            epoch,
            candidateId,
            lastEpoch,
            lastEpochOffset
        );
    }

    VoteResponseData voteResponse(boolean voteGranted, Optional<Integer> leaderId, int epoch) {
        return VoteResponse.singletonResponse(
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            voteGranted
        );
    }

    private VoteRequestData.PartitionData unwrap(VoteRequestData voteRequest) {
        assertTrue(RaftUtil.hasValidTopicPartition(voteRequest, metadataPartition));
        return voteRequest.topics().get(0).partitions().get(0);
    }

    static void assertMatchingRecords(
        String[] expected,
        Records actual
    ) {
        List<Record> recordList = Utils.toList(actual.records());
        assertEquals(expected.length, recordList.size());
        for (int i = 0; i < expected.length; i++) {
            Record record = recordList.get(i);
            assertEquals(expected[i], Utils.utf8(record.value()),
                "Record at offset " + record.offset() + " does not match expected");
        }
    }

    static void verifyLeaderChangeMessage(
        int leaderId,
        List<Integer> voters,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(recordKey));

        LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(recordValue);
        assertEquals(leaderId, leaderChangeMessage.leaderId());
        assertEquals(voters.stream().map(voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toList()),
            leaderChangeMessage.voters());
    }

    void assertFetchRequestData(
        RaftMessage message,
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        assertTrue(
            message.data() instanceof FetchRequestData, "Unexpected request type " + message.data());
        FetchRequestData request = (FetchRequestData) message.data();

        assertEquals(1, request.topics().size());
        assertEquals(metadataPartition.topic(), request.topics().get(0).topic());
        assertEquals(1, request.topics().get(0).partitions().size());

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        assertEquals(epoch, fetchPartition.currentLeaderEpoch());
        assertEquals(fetchOffset, fetchPartition.fetchOffset());
        assertEquals(lastFetchedEpoch, fetchPartition.lastFetchedEpoch());
        assertEquals(localId, request.replicaId());
    }

    FetchRequestData fetchRequest(
        int epoch,
        int replicaId,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        FetchRequestData request = RaftUtil.singletonFetchRequest(metadataPartition, fetchPartition -> {
            fetchPartition
                .setCurrentLeaderEpoch(epoch)
                .setLastFetchedEpoch(lastFetchedEpoch)
                .setFetchOffset(fetchOffset);
        });
        return request
            .setMaxWaitMs(maxWaitTimeMs)
            .setReplicaId(replicaId);
    }

    FetchResponseData fetchResponse(
        int epoch,
        int leaderId,
        Records records,
        long highWatermark,
        Errors error
    ) {
        return RaftUtil.singletonFetchResponse(metadataPartition, Errors.NONE, partitionData -> {
            partitionData
                .setRecordSet(records)
                .setErrorCode(error.code())
                .setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);
        });
    }

    FetchResponseData outOfRangeFetchRecordsResponse(
        int epoch,
        int leaderId,
        long divergingEpochEndOffset,
        int divergingEpoch,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(metadataPartition, Errors.NONE, partitionData -> {
            partitionData
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);

            partitionData.divergingEpoch()
                .setEpoch(divergingEpoch)
                .setEndOffset(divergingEpochEndOffset);
        });
    }

    static class MockListener implements RaftClient.Listener<String> {
        private final List<BatchReader.Batch<String>> commits = new ArrayList<>();
        private final Map<Integer, Long> claimedEpochStartOffsets = new HashMap<>();
        private OptionalInt currentClaimedEpoch = OptionalInt.empty();

        int numCommittedBatches() {
            return commits.size();
        }

        Long claimedEpochStartOffset(int epoch) {
            return claimedEpochStartOffsets.get(epoch);
        }

        BatchReader.Batch<String> lastCommit() {
            if (commits.isEmpty()) {
                return null;
            } else {
                return commits.get(commits.size() - 1);
            }
        }

        OptionalLong lastCommitOffset() {
            if (commits.isEmpty()) {
                return OptionalLong.empty();
            } else {
                return OptionalLong.of(commits.get(commits.size() - 1).lastOffset());
            }
        }

        OptionalInt currentClaimedEpoch() {
            return currentClaimedEpoch;
        }

        List<String> commitWithBaseOffset(long baseOffset) {
            return commits.stream()
                .filter(batch -> batch.baseOffset() == baseOffset)
                .findFirst()
                .map(batch -> batch.records())
                .orElse(null);
        }

        List<String> commitWithLastOffset(long lastOffset) {
            return commits.stream()
                .filter(batch -> batch.lastOffset() == lastOffset)
                .findFirst()
                .map(batch -> batch.records())
                .orElse(null);
        }

        @Override
        public void handleClaim(int epoch) {
            // We record the next expected offset as the claimed epoch's start
            // offset. This is useful to verify that the `handleClaim` callback
            // was not received early.
            long claimedEpochStartOffset = lastCommitOffset().isPresent() ?
                lastCommitOffset().getAsLong() + 1 : 0L;
            this.currentClaimedEpoch = OptionalInt.of(epoch);
            this.claimedEpochStartOffsets.put(epoch, claimedEpochStartOffset);
        }

        @Override
        public void handleResign() {
            this.currentClaimedEpoch = OptionalInt.empty();
        }

        @Override
        public void handleCommit(BatchReader<String> reader) {
            try {
                while (reader.hasNext()) {
                    long nextOffset = lastCommitOffset().isPresent() ?
                        lastCommitOffset().getAsLong() + 1 : 0L;
                    BatchReader.Batch<String> batch = reader.next();
                    // We expect monotonic offsets, but not necessarily sequential
                    // offsets since control records will be filtered.
                    assertTrue(batch.baseOffset() >= nextOffset,
                        "Received non-monotonic commit " + batch +
                            ". We expected an offset at least as large as " + nextOffset);
                    commits.add(batch);
                }
            } finally {
                reader.close();
            }
        }
    }

}
