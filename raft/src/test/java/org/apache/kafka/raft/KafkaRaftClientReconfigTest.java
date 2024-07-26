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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.raft.internals.VoterSetTest;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriterReaderTest;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.apache.kafka.snapshot.Snapshots.BOOTSTRAP_SNAPSHOT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientReconfigTest {

    @Test
    public void testLeaderWritesBootstrapRecords() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(0)
            .build();

        List<List<ControlRecord>> expectedBootstrapRecords = Arrays.asList(
            Arrays.asList(
                new ControlRecord(
                    ControlRecordType.SNAPSHOT_HEADER,
                    new SnapshotHeaderRecord()
                        .setVersion((short) 0)
                        .setLastContainedLogTimestamp(0)
                ),
                new ControlRecord(
                    ControlRecordType.KRAFT_VERSION,
                    new KRaftVersionRecord()
                        .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                        .setKRaftVersion((short) 1)
                ),
                new ControlRecord(
                    ControlRecordType.KRAFT_VOTERS,
                    voters.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
                )
            ),
            Arrays.asList(
                new ControlRecord(
                    ControlRecordType.SNAPSHOT_FOOTER,
                    new SnapshotFooterRecord()
                        .setVersion((short) 0)
                )
            )
        );

        // check the bootstrap snapshot exists and contains the expected records
        assertEquals(BOOTSTRAP_SNAPSHOT_ID, context.log.latestSnapshotId().get());
        try (SnapshotReader<?> reader = RecordsSnapshotReader.of(
                context.log.latestSnapshot().get(),
                context.serde,
                BufferSupplier.NO_CACHING,
                KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
                false
            )
        ) {
            SnapshotWriterReaderTest.assertControlSnapshot(expectedBootstrapRecords, reader);
        }

        context.becomeLeader();

        // check if leader writes 3 bootstrap records to the log
        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(
            local.id(),
            Arrays.asList(local.id(), follower.id()),
            Arrays.asList(local.id(), follower.id()),
            record.key(),
            record.value()
        );
        record = recordIterator.next();
        verifyKRaftVersionRecord((short) 1, record.key(), record.value());
        record = recordIterator.next();
        verifyVotersRecord(voters, record.key(), record.value());
    }

    @Test
    public void testBootstrapCheckpointIsNotReturnedOnFetch() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(0)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // check that leader does not respond with bootstrap snapshot id when follower fetches offset 0
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                follower,
                0,
                0,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
    }

    @Test
    public void testLeaderDoesNotBootstrapRecordsWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withStaticVoters(voters)
            .withBootstrapSnapshot(Optional.empty())
            .withUnknownLeader(0)
            .build();

        List<List<ControlRecord>> expectedBootstrapRecords = Arrays.asList(
            Arrays.asList(
                new ControlRecord(
                    ControlRecordType.SNAPSHOT_HEADER,
                    new SnapshotHeaderRecord()
                        .setVersion((short) 0)
                        .setLastContainedLogTimestamp(0)
                )
            ),
            Arrays.asList(
                new ControlRecord(
                    ControlRecordType.SNAPSHOT_FOOTER,
                    new SnapshotFooterRecord()
                        .setVersion((short) 0)
                )
            )
        );

        // check the bootstrap snapshot exists but is empty
        assertEquals(BOOTSTRAP_SNAPSHOT_ID, context.log.latestSnapshotId().get());
        try (SnapshotReader<?> reader = RecordsSnapshotReader.of(
                context.log.latestSnapshot().get(),
                context.serde,
                BufferSupplier.NO_CACHING,
                KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
                false
            )
        ) {
            SnapshotWriterReaderTest.assertControlSnapshot(expectedBootstrapRecords, reader);
        }

        // check leader does not write bootstrap records to log
        context.becomeLeader();

        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(
            local.id(),
            Arrays.asList(local.id(), follower.id()),
            Arrays.asList(local.id(), follower.id()),
            record.key(),
            record.value()
        );
        assertFalse(recordIterator.hasNext());
    }

    @Test
    public void testFollowerDoesNotRequestLeaderBootstrapSnapshot() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        int epoch = 1;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, leader));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leader.id())
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // check if leader response were to contain bootstrap snapshot id, follower would not send fetch snapshot request
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leader.id(), BOOTSTRAP_SNAPSHOT_ID, 0)
        );
        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testFollowerReadsKRaftBootstrapRecords() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        ReplicaKey follower = replicaKey(local.id() + 2, true);
        VoterSet voterSet = VoterSetTest.voterSet(Stream.of(local, leader));
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voterSet))
            .withElectedLeader(epoch, leader.id())
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // check that before receiving bootstrap records from leader, follower is not in the voter set
        assertFalse(context.client.quorum().isVoter(follower));

        // leader sends batch with bootstrap records
        VoterSet leadersVoterSet = VoterSetTest.voterSet(
            Stream.concat(voterSet.voterKeys().stream(), Stream.of(follower))
        );
        ByteBuffer buffer = ByteBuffer.allocate(128);
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0, // baseOffset
                0, // logAppendTime
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false, // isTransactional
                true, // isControlBatch
                epoch,
                buffer.capacity()
            )
        ) {
            builder.appendLeaderChangeMessage(
                0,
                new LeaderChangeMessage()
            );
            builder.appendKRaftVersionMessage(
                0, // timestamp
                new KRaftVersionRecord()
                    .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                    .setKRaftVersion((short) 1)
            );
            builder.appendVotersMessage(
                0, // timestamp
                leadersVoterSet.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
            );
            MemoryRecords leaderRecords = builder.build();
            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, leader.id(), leaderRecords, 0, Errors.NONE)
            );
        }

        // follower applies the bootstrap records, registering follower2 as a new voter
        context.client.poll();
        assertTrue(context.client.quorum().isVoter(follower));
    }

    @Test
    public void testAddVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Show that the new voter is not currently a voter
        assertFalse(context.client.quorum().isVoter(newVoter));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE)
        );

        // Handle the the API_VERSIONS response
        context.client.poll();
        // Append new VotersRecord to log
        context.client.poll();
        // The new voter is now a voter after writing the VotersRecord to the log
        assertTrue(context.client.quorum().isVoter(newVoter));

        // Send a FETCH to increase the HWM and commit the new voter set
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Expect reply for AddVoter request
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NONE);
    }

    @Test
    void testAddVoterInvalidClusterId() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // empty cluster id is rejected
        context.deliverRequest(context.addVoterRequest("", Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.addVoterRequest("invalid-uuid", Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        assertFalse(context.client.quorum().isVoter(newVoter));
    }

    @Test
    void testAddVoterToNotLeader() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    @Test
    void testAddVoterWithMissingDefaultListener() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(ListenerName.normalised("not_the_default_listener"), newAddress)
        );

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INVALID_REQUEST);
    }

    @Test
    void testAddVoterWithPendingAddVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Attempting to add another voter should be an error
        ReplicaKey anotherNewVoter = replicaKey(local.id() + 3, true);
        InetSocketAddress anotherNewAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + anotherNewVoter.id()
        );
        Endpoints anotherNewListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), anotherNewAddress)
        );
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, anotherNewVoter, anotherNewListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterWithoutFencedPreviousLeaders() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters)
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.UNSUPPORTED_VERSION);
    }

    @Test
    void testAddVoterWithExistingVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(follower.id(), true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter with the same id as an existing voter
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.DUPLICATE_VOTER);
    }

    @Test
    void testAddVoterTimeout() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Show that the new voter is not currently a voter
        assertFalse(context.client.quorum().isVoter(newVoter));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE)
        );

        // Handle the the API_VERSIONS response
        context.client.poll();

        // Wait for request timeout without sending a FETCH request to timeout the add voter RPC
        context.time.sleep(context.requestTimeoutMs());

        // Expect the AddVoter RPC to timeout
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);

        // Event though the voters record never committed and the RPC timeout show that the new
        // voter is part of the voter set
        assertTrue(context.client.quorum().isVoter(newVoter));
    }

    @Test
    void testAddVoterWithApiVersionsFromIncorrectNode() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.INVALID_REQUEST)
        );
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterInvalidFeatureVersion() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response that doesn't support kraft.version 1
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE, new SupportedVersionRange((short) 0))
        );
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INVALID_REQUEST);
    }

    @Test
    void testAddVoterWithLaggingNewVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE)
        );
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterFailsWhenLosingLeadership() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Leader completes the AddVoter RPC when resigning
        context.client.resign(epoch);
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    @Test
    void testAddVoterWithMissingDirectoryId() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, false);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INVALID_REQUEST);
    }

    @Test
    public void testRemoveVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        assertTrue(context.client.quorum().isVoter(follower2));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // follower2 should not be a voter in the latest voter set
        assertFalse(context.client.quorum().isVoter(follower2));

        // Send a FETCH to increase the HWM and commit the new voter set
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Expect reply for RemoveVoter request
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NONE);
    }

    @Test
    public void testRemoveVoterIsLeader() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        assertTrue(context.client.quorum().isVoter(follower2));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove local leader
        context.deliverRequest(context.removeVoterRequest(local));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // local should not be a voter in the latest voter set
        assertFalse(context.client.quorum().isVoter(local));

        // Send a FETCH request for follower1
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Send a FETCH request for follower2 and increaes the HWM
        context.deliverRequest(
            context.fetchRequest(epoch, follower2, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Expect reply for RemoveVoter request
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NONE);

        // Expect END_QUORUM_EPOCH requests
        context.pollUntilRequest();
        context.collectEndQuorumRequests(epoch, new HashSet<>(Arrays.asList(follower1.id(), follower2.id())), Optional.empty());

        // Election timeout is randome numer in [electionTimeoutMs, 2 * electionTimeoutMs)
        context.time.sleep(2 * context.electionTimeoutMs());
        context.client.poll();

        assertTrue(context.client.quorum().isObserver());
        assertTrue(context.client.quorum().isUnattached());
    }

    @Test
    public void testRemoveVoterInvalidClusterId() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();

        // empty cluster id is rejected
        context.deliverRequest(context.removeVoterRequest("", follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.removeVoterRequest("invalid-uuid", follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        assertTrue(context.client.quorum().isVoter(follower1));
    }

    @Test
    void testRemoveVoterToNotLeader() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.removeVoterRequest(follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    @Test
    void testRemoveVoterWithPendingRemoveVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Attempt to remove follower1
        context.deliverRequest(context.removeVoterRequest(follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testRemoveVoterWithoutFencedPreviousLeaders() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testRemoveVoterWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters)
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.UNSUPPORTED_VERSION);
    }

    @Test
    void testRemoveVoterWithNoneVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove replica with same id as follower2
        context.deliverRequest(context.removeVoterRequest(replicaKey(follower2.id(), true)));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.VOTER_NOT_FOUND);
    }

    @Test
    void testRemoveVoterWithNoneVoterId() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(
            context.removeVoterRequest(
                ReplicaKey.of(follower2.id() + 1, follower2.directoryId().get())
            )
        );
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.VOTER_NOT_FOUND);
    }

    @Test
    void testRemoveVoterToEmptyVoterSet() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .build();
        assertEquals(OptionalInt.of(local.id()), context.currentLeader());

        // Attempt to remove local leader to empty voter set
        context.deliverRequest(context.removeVoterRequest(local));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.VOTER_NOT_FOUND);
    }

    @Test
    void testRemoveVoterTimedOut() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        assertTrue(context.client.quorum().isVoter(follower2));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Wait for request timeout without sending a FETCH request to timeout the remove voter RPC
        context.time.sleep(context.requestTimeoutMs());

        // Expect a timeout error
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);

        // Event though the voters record never committed and the RPC timeout show that the old
        // voter is not part of the voter set
        assertFalse(context.client.quorum().isVoter(follower2));
    }

    @Test
    void testRemoveVoterFailsWhenLosingLeadership() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        assertTrue(context.client.quorum().isVoter(follower2));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Leader completes the RemoveVoter RPC when resigning
        context.client.resign(epoch);
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);

        // Event though the voters record never committed, the old voter is not part of the voter
        // set
        assertFalse(context.client.quorum().isVoter(follower2));
    }

    @Test
    void testAddVoterWithPendingRemoveVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Attempt to add a new voter while the RemoveVoter RPC is pending
        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testRemoveVoterWithPendingAddVoter() throws Exception {
        ReplicaKey local = replicaKey(randomeReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Collections.singletonMap(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Attempt to remove followe while AddVoter is pending
        context.deliverRequest(context.removeVoterRequest(follower));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    private static void verifyVotersRecord(
        VoterSet expectedVoterSet,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.KRAFT_VOTERS, ControlRecordType.parse(recordKey));
        VotersRecord votersRecord = ControlRecordUtils.deserializeVotersRecord(recordValue);
        assertEquals(
            expectedVoterSet,
            VoterSet.fromVotersRecord(votersRecord)
        );
    }

    private static void verifyKRaftVersionRecord(
        short expectedKRaftVersion,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.KRAFT_VERSION, ControlRecordType.parse(recordKey));
        KRaftVersionRecord kRaftVersionRecord = ControlRecordUtils.deserializeKRaftVersionRecord(recordValue);
        assertEquals(expectedKRaftVersion, kRaftVersionRecord.kRaftVersion());
    }

    private int randomeReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }

    private static ApiVersionsResponseData apiVersionsResponse(Errors error) {
        return apiVersionsResponse(error, new SupportedVersionRange((short) 0, (short) 1));
    }

    private static ApiVersionsResponseData apiVersionsResponse(Errors error, SupportedVersionRange supportedVersions) {
        ApiVersionsResponseData.SupportedFeatureKeyCollection supportedFeatures =
            new ApiVersionsResponseData.SupportedFeatureKeyCollection(1);

        if (supportedVersions.max() > 0) {
            supportedFeatures.add(
                new ApiVersionsResponseData.SupportedFeatureKey()
                    .setName(KRaftVersion.FEATURE_NAME)
                    .setMinVersion(supportedVersions.min())
                    .setMaxVersion(supportedVersions.max())
            );
        }

        return new ApiVersionsResponseData()
            .setErrorCode(error.code())
            .setSupportedFeatures(supportedFeatures);
    }
}
