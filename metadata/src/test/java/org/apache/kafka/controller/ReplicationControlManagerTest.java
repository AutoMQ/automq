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

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData.PartitionData;
import org.apache.kafka.common.message.AlterPartitionRequestData.TopicData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitionsCollection;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.apache.kafka.common.protocol.Errors.ELECTION_NOT_NEEDED;
import static org.apache.kafka.common.protocol.Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.FENCED_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.Errors.INVALID_PARTITIONS;
import static org.apache.kafka.common.protocol.Errors.INVALID_REPLICA_ASSIGNMENT;
import static org.apache.kafka.common.protocol.Errors.INVALID_TOPIC_EXCEPTION;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.NO_REASSIGNMENT_IN_PROGRESS;
import static org.apache.kafka.common.protocol.Errors.POLICY_VIOLATION;
import static org.apache.kafka.common.protocol.Errors.PREFERRED_LEADER_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class ReplicationControlManagerTest {
    private final static Logger log = LoggerFactory.getLogger(ReplicationControlManagerTest.class);
    private final static int BROKER_SESSION_TIMEOUT_MS = 1000;

    private static class ReplicationControlTestContext {
        final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        final LogContext logContext = new LogContext();
        final MockTime time = new MockTime();
        final MockRandom random = new MockRandom();
        final ControllerMetrics metrics = new MockControllerMetrics();
        final String clusterId = Uuid.randomUuid().toString();
        final ClusterControlManager clusterControl = new ClusterControlManager(logContext,
            clusterId,
            time,
            snapshotRegistry,
            TimeUnit.MILLISECONDS.convert(BROKER_SESSION_TIMEOUT_MS, TimeUnit.NANOSECONDS),
            new StripedReplicaPlacer(random),
            metrics);
        final ConfigurationControlManager configurationControl = new ConfigurationControlManager(
            new LogContext(),
            snapshotRegistry,
            KafkaConfigSchema.EMPTY,
            __ -> { },
            Optional.empty(),
            (__, ___) -> { });
        final ReplicationControlManager replicationControl;

        void replay(List<ApiMessageAndVersion> records) throws Exception {
            RecordTestUtils.replayAll(clusterControl, records);
            RecordTestUtils.replayAll(configurationControl, records);
            RecordTestUtils.replayAll(replicationControl, records);
        }

        ReplicationControlTestContext() {
            this(Optional.empty());
        }

        ReplicationControlTestContext(Optional<CreateTopicPolicy> createTopicPolicy) {
            this.replicationControl = new ReplicationControlManager(snapshotRegistry,
                new LogContext(),
                (short) 3,
                1,
                Integer.MAX_VALUE,
                true,
                configurationControl,
                clusterControl,
                metrics,
                createTopicPolicy);
            clusterControl.activate();
        }

        CreatableTopicResult createTestTopic(String name,
                                             int numPartitions,
                                             short replicationFactor,
                                             short expectedErrorCode) throws Exception {
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreatableTopic topic = new CreatableTopic().setName(name);
            topic.setNumPartitions(numPartitions).setReplicationFactor(replicationFactor);
            request.topics().add(topic);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(request);
            CreatableTopicResult topicResult = result.response().topics().find(name);
            assertNotNull(topicResult);
            assertEquals(expectedErrorCode, topicResult.errorCode());
            if (expectedErrorCode == NONE.code()) {
                replay(result.records());
            }
            return topicResult;
        }

        CreatableTopicResult createTestTopic(String name, int[][] replicas) throws Exception {
            return createTestTopic(name, replicas, Collections.emptyMap(), (short) 0);
        }

        CreatableTopicResult createTestTopic(String name, int[][] replicas,
                                             short expectedErrorCode) throws Exception {
            return createTestTopic(name, replicas, Collections.emptyMap(), expectedErrorCode);
        }

        CreatableTopicResult createTestTopic(String name, int[][] replicas,
                                             Map<String, String> configs,
                                             short expectedErrorCode) throws Exception {
            assertFalse(replicas.length == 0);
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreatableTopic topic = new CreatableTopic().setName(name);
            topic.setNumPartitions(-1).setReplicationFactor((short) -1);
            for (int i = 0; i < replicas.length; i++) {
                topic.assignments().add(new CreatableReplicaAssignment().
                    setPartitionIndex(i).setBrokerIds(Replicas.toList(replicas[i])));
            }
            configs.entrySet().forEach(e -> topic.configs().add(
                new CreateTopicsRequestData.CreateableTopicConfig().setName(e.getKey()).
                    setValue(e.getValue())));
            request.topics().add(topic);
            ControllerResult<CreateTopicsResponseData> result =
                replicationControl.createTopics(request);
            CreatableTopicResult topicResult = result.response().topics().find(name);
            assertNotNull(topicResult);
            assertEquals(expectedErrorCode, topicResult.errorCode());
            if (expectedErrorCode == NONE.code()) {
                assertEquals(replicas.length, topicResult.numPartitions());
                assertEquals(replicas[0].length, topicResult.replicationFactor());
                replay(result.records());
            }
            return topicResult;
        }

        void createPartitions(int count, String name,
                int[][] replicas, short expectedErrorCode) throws Exception {
            assertFalse(replicas.length == 0);
            CreatePartitionsTopic topic = new CreatePartitionsTopic().
                setName(name).
                setCount(count);
            for (int i = 0; i < replicas.length; i++) {
                topic.assignments().add(new CreatePartitionsAssignment().
                    setBrokerIds(Replicas.toList(replicas[i])));
            }
            ControllerResult<List<CreatePartitionsTopicResult>> result =
                replicationControl.createPartitions(Collections.singletonList(topic));
            assertEquals(1, result.response().size());
            CreatePartitionsTopicResult topicResult = result.response().get(0);
            assertEquals(name, topicResult.name());
            assertEquals(expectedErrorCode, topicResult.errorCode());
            replay(result.records());
        }

        void registerBrokers(Integer... brokerIds) throws Exception {
            for (int brokerId : brokerIds) {
                RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
                    setBrokerEpoch(brokerId + 100).setBrokerId(brokerId);
                brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
                    setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                    setPort((short) 9092 + brokerId).
                    setName("PLAINTEXT").
                    setHost("localhost"));
                replay(Collections.singletonList(new ApiMessageAndVersion(brokerRecord, (short) 0)));
            }
        }

        void alterPartition(
            TopicIdPartition topicIdPartition,
            int leaderId,
            List<Integer> isr,
            LeaderRecoveryState leaderRecoveryState
        ) throws Exception {
            BrokerRegistration registration = clusterControl.brokerRegistrations().get(leaderId);
            assertFalse(registration.fenced());

            PartitionRegistration partition = replicationControl.getPartition(
                topicIdPartition.topicId(),
                topicIdPartition.partitionId()
            );
            assertNotNull(partition);
            assertEquals(leaderId, partition.leader);

            PartitionData partitionData = new PartitionData()
                .setPartitionIndex(topicIdPartition.partitionId())
                .setPartitionEpoch(partition.partitionEpoch)
                .setLeaderEpoch(partition.leaderEpoch)
                .setLeaderRecoveryState(leaderRecoveryState.value())
                .setNewIsr(isr);

            String topicName = replicationControl.getTopic(topicIdPartition.topicId()).name();
            TopicData topicData = new TopicData()
                .setName(topicName)
                .setPartitions(singletonList(partitionData));

            ControllerResult<AlterPartitionResponseData> alterPartition = replicationControl.alterPartition(
                new AlterPartitionRequestData()
                    .setBrokerId(leaderId)
                    .setBrokerEpoch(registration.epoch())
                    .setTopics(singletonList(topicData)));
            replay(alterPartition.records());
        }

        void unfenceBrokers(Integer... brokerIds)  throws Exception {
            unfenceBrokers(Utils.mkSet(brokerIds));
        }

        void unfenceBrokers(Set<Integer> brokerIds) throws Exception {
            for (int brokerId : brokerIds) {
                ControllerResult<BrokerHeartbeatReply> result = replicationControl.
                    processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                        setBrokerId(brokerId).setBrokerEpoch(brokerId + 100).
                        setCurrentMetadataOffset(1).
                        setWantFence(false).setWantShutDown(false), 0);
                assertEquals(new BrokerHeartbeatReply(true, false, false, false),
                    result.response());
                replay(result.records());
            }
        }

        void alterTopicConfig(
            String topic,
            String configKey,
            String configValue
        ) throws Exception {
            ConfigRecord configRecord = new ConfigRecord()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(topic)
                .setName(configKey)
                .setValue(configValue);
            replay(singletonList(new ApiMessageAndVersion(configRecord, (short) 0)));
        }

        void fenceBrokers(Set<Integer> brokerIds) throws Exception {
            time.sleep(BROKER_SESSION_TIMEOUT_MS);

            Set<Integer> unfencedBrokerIds = clusterControl.brokerRegistrations().keySet().stream()
                .filter(brokerId -> !brokerIds.contains(brokerId))
                .collect(Collectors.toSet());
            unfenceBrokers(unfencedBrokerIds.toArray(new Integer[0]));

            Optional<Integer> staleBroker = clusterControl.heartbeatManager().findOneStaleBroker();
            while (staleBroker.isPresent()) {
                ControllerResult<Void> fenceResult = replicationControl.maybeFenceOneStaleBroker();
                replay(fenceResult.records());
                staleBroker = clusterControl.heartbeatManager().findOneStaleBroker();
            }

            assertEquals(brokerIds, clusterControl.fencedBrokerIds());
        }

        long currentBrokerEpoch(int brokerId) {
            Map<Integer, BrokerRegistration> registrations = clusterControl.brokerRegistrations();
            BrokerRegistration registration = registrations.get(brokerId);
            assertNotNull(registration, "No current registration for broker " + brokerId);
            return registration.epoch();
        }

        OptionalInt currentLeader(TopicIdPartition topicIdPartition) {
            PartitionRegistration partition = replicationControl.
                getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
            return (partition.leader < 0) ? OptionalInt.empty() : OptionalInt.of(partition.leader);
        }
    }

    private static class MockCreateTopicPolicy implements CreateTopicPolicy {
        private final List<RequestMetadata> expecteds;
        private final AtomicLong index = new AtomicLong(0);

        MockCreateTopicPolicy(List<RequestMetadata> expecteds) {
            this.expecteds = expecteds;
        }

        @Override
        public void validate(RequestMetadata actual) throws PolicyViolationException {
            long curIndex = index.getAndIncrement();
            if (curIndex >= expecteds.size()) {
                throw new PolicyViolationException("Unexpected topic creation: index " +
                    "out of range at " + curIndex);
            }
            RequestMetadata expected = expecteds.get((int) curIndex);
            if (!expected.equals(actual)) {
                throw new PolicyViolationException("Expected: " + expected +
                    ". Got: " + actual);
            }
        }

        @Override
        public void close() throws Exception {
            // nothing to do
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // nothing to do
        }
    }

    @Test
    public void testCreateTopics() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(-1).setReplicationFactor((short) -1));
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code()).
                setErrorMessage("Unable to replicate the partition 3 time(s): All " +
                    "brokers are currently fenced."));
        assertEquals(expectedResponse, result.response());

        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ControllerResult<CreateTopicsResponseData> result2 =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
        expectedResponse2.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3).
            setErrorMessage(null).setErrorCode((short) 0).
            setTopicId(result2.response().topics().find("foo").topicId()));
        assertEquals(expectedResponse2, result2.response());
        ctx.replay(result2.records());
        assertEquals(new PartitionRegistration(new int[] {1, 2, 0},
            new int[] {1, 2, 0}, Replicas.NONE, Replicas.NONE, 1, LeaderRecoveryState.RECOVERED, 0, 0),
            replicationControl.getPartition(
                ((TopicRecord) result2.records().get(0).message()).topicId(), 0));
        ControllerResult<CreateTopicsResponseData> result3 =
                replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
        expectedResponse3.topics().add(new CreatableTopicResult().setName("foo").
                setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()).
                setErrorMessage("Topic 'foo' already exists."));
        assertEquals(expectedResponse3, result3.response());
        Uuid fooId = result2.response().topics().find("foo").topicId();
        RecordTestUtils.assertBatchIteratorContains(asList(
            asList(new ApiMessageAndVersion(new PartitionRecord().
                    setPartitionId(0).setTopicId(fooId).
                    setReplicas(asList(1, 2, 0)).setIsr(asList(1, 2, 0)).
                    setRemovingReplicas(Collections.emptyList()).setAddingReplicas(Collections.emptyList()).setLeader(1).
                    setLeaderEpoch(0).setPartitionEpoch(0), (short) 0),
                new ApiMessageAndVersion(new TopicRecord().
                    setTopicId(fooId).setName("foo"), (short) 0))),
            ctx.replicationControl.iterator(Long.MAX_VALUE));
    }

    @Test
    public void testBrokerCountMetrics() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;

        ctx.registerBrokers(0);

        assertEquals(1, ctx.metrics.fencedBrokerCount());
        assertEquals(0, ctx.metrics.activeBrokerCount());

        ctx.unfenceBrokers(0);

        assertEquals(0, ctx.metrics.fencedBrokerCount());
        assertEquals(1, ctx.metrics.activeBrokerCount());

        ctx.registerBrokers(1);
        ctx.unfenceBrokers(1);

        assertEquals(2, ctx.metrics.activeBrokerCount());

        ctx.registerBrokers(2);
        ctx.unfenceBrokers(2);

        assertEquals(0, ctx.metrics.fencedBrokerCount());
        assertEquals(3, ctx.metrics.activeBrokerCount());

        ControllerResult<Void> result = replicationControl.unregisterBroker(0);
        ctx.replay(result.records());
        result = replicationControl.unregisterBroker(2);
        ctx.replay(result.records());

        assertEquals(0, ctx.metrics.fencedBrokerCount());
        assertEquals(1, ctx.metrics.activeBrokerCount());
    }

    @Test
    public void testCreateTopicsWithValidateOnlyFlag() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreateTopicsRequestData request = new CreateTopicsRequestData().setValidateOnly(true);
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3));
        ControllerResult<CreateTopicsResponseData> result =
            ctx.replicationControl.createTopics(request);
        assertEquals(0, result.records().size());
        CreatableTopicResult topicResult = result.response().topics().find("foo");
        assertEquals((short) 0, topicResult.errorCode());
    }

    @Test
    public void testInvalidCreateTopicsWithValidateOnlyFlag() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreateTopicsRequestData request = new CreateTopicsRequestData().setValidateOnly(true);
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 4));
        ControllerResult<CreateTopicsResponseData> result =
            ctx.replicationControl.createTopics(request);
        assertEquals(0, result.records().size());
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code()).
            setErrorMessage("Unable to replicate the partition 4 time(s): The target " +
                "replication factor of 4 cannot be reached because only 3 broker(s) " +
                "are registered."));
        assertEquals(expectedResponse, result.response());
    }

    @Test
    public void testCreateTopicsWithPolicy() throws Exception {
        MockCreateTopicPolicy createTopicPolicy = new MockCreateTopicPolicy(asList(
            new CreateTopicPolicy.RequestMetadata("foo", 2, (short) 2,
                null, Collections.emptyMap()),
            new CreateTopicPolicy.RequestMetadata("bar", 3, (short) 2,
                null, Collections.emptyMap()),
            new CreateTopicPolicy.RequestMetadata("baz", null, null,
                Collections.singletonMap(0, asList(2, 1, 0)),
                Collections.singletonMap(SEGMENT_BYTES_CONFIG, "12300000")),
            new CreateTopicPolicy.RequestMetadata("quux", null, null,
                Collections.singletonMap(0, asList(2, 1, 0)), Collections.emptyMap())));
        ReplicationControlTestContext ctx =
            new ReplicationControlTestContext(Optional.of(createTopicPolicy));
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", 2, (short) 2, NONE.code());
        ctx.createTestTopic("bar", 3, (short) 3, POLICY_VIOLATION.code());
        ctx.createTestTopic("baz", new int[][] {new int[] {2, 1, 0}},
            Collections.singletonMap(SEGMENT_BYTES_CONFIG, "12300000"), NONE.code());
        ctx.createTestTopic("quux", new int[][] {new int[] {1, 2, 0}}, POLICY_VIOLATION.code());
    }

    @Test
    public void testGlobalTopicAndPartitionMetrics() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) -1));

        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        List<Uuid> topicsToDelete = new ArrayList<>();

        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(request);
        topicsToDelete.add(result.response().topics().find("foo").topicId());

        RecordTestUtils.replayAll(replicationControl, result.records());
        assertEquals(1, ctx.metrics.globalTopicsCount());

        request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("bar").
            setNumPartitions(1).setReplicationFactor((short) -1));
        request.topics().add(new CreatableTopic().setName("baz").
            setNumPartitions(2).setReplicationFactor((short) -1));
        result = replicationControl.createTopics(request);
        RecordTestUtils.replayAll(replicationControl, result.records());
        assertEquals(3, ctx.metrics.globalTopicsCount());
        assertEquals(4, ctx.metrics.globalPartitionCount());

        topicsToDelete.add(result.response().topics().find("baz").topicId());
        ControllerResult<Map<Uuid, ApiError>> deleteResult = replicationControl.deleteTopics(topicsToDelete);
        RecordTestUtils.replayAll(replicationControl, deleteResult.records());
        assertEquals(1, ctx.metrics.globalTopicsCount());
        assertEquals(1, ctx.metrics.globalPartitionCount());

        Uuid topicToDelete = result.response().topics().find("bar").topicId();
        deleteResult = replicationControl.deleteTopics(Collections.singletonList(topicToDelete));
        RecordTestUtils.replayAll(replicationControl, deleteResult.records());
        assertEquals(0, ctx.metrics.globalTopicsCount());
        assertEquals(0, ctx.metrics.globalPartitionCount());
    }

    @Test
    public void testOfflinePartitionAndReplicaImbalanceMetrics() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);

        CreatableTopicResult foo = ctx.createTestTopic("foo", new int[][] {
            new int[] {0, 2}, new int[] {0, 1}});

        CreatableTopicResult zar = ctx.createTestTopic("zar", new int[][] {
            new int[] {0, 1, 2}, new int[] {1, 2, 3}, new int[] {1, 2, 0}});

        ControllerResult<Void> result = replicationControl.unregisterBroker(0);
        ctx.replay(result.records());

        // All partitions should still be online after unregistering broker 0
        assertEquals(0, ctx.metrics.offlinePartitionCount());
        // Three partitions should not have their preferred (first) replica 0
        assertEquals(3, ctx.metrics.preferredReplicaImbalanceCount());

        result = replicationControl.unregisterBroker(1);
        ctx.replay(result.records());

        // After unregistering broker 1, 1 partition for topic foo should go offline
        assertEquals(1, ctx.metrics.offlinePartitionCount());
        // All five partitions should not have their preferred (first) replica at this point
        assertEquals(5, ctx.metrics.preferredReplicaImbalanceCount());

        result = replicationControl.unregisterBroker(2);
        ctx.replay(result.records());

        // After unregistering broker 2, the last partition for topic foo should go offline
        // and 2 partitions for topic zar should go offline
        assertEquals(4, ctx.metrics.offlinePartitionCount());

        result = replicationControl.unregisterBroker(3);
        ctx.replay(result.records());

        // After unregistering broker 3 the last partition for topic zar should go offline
        assertEquals(5, ctx.metrics.offlinePartitionCount());

        // Deleting topic foo should bring the offline partition count down to 3
        ArrayList<ApiMessageAndVersion> records = new ArrayList<>();
        replicationControl.deleteTopic(foo.topicId(), records);
        ctx.replay(records);

        assertEquals(3, ctx.metrics.offlinePartitionCount());

        // Deleting topic zar should bring the offline partition count down to 0
        records = new ArrayList<>();
        replicationControl.deleteTopic(zar.topicId(), records);
        ctx.replay(records);

        assertEquals(0, ctx.metrics.offlinePartitionCount());
    }

    @Test
    public void testValidateNewTopicNames() {
        Map<String, ApiError> topicErrors = new HashMap<>();
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName(""));
        topics.add(new CreatableTopic().setName("woo"));
        topics.add(new CreatableTopic().setName("."));
        ReplicationControlManager.validateNewTopicNames(topicErrors, topics);
        Map<String, ApiError> expectedTopicErrors = new HashMap<>();
        expectedTopicErrors.put("", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name is illegal, it can't be empty"));
        expectedTopicErrors.put(".", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name cannot be \".\" or \"..\""));
        assertEquals(expectedTopicErrors, topicErrors);
    }

    @Test
    public void testRemoveLeaderships() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        CreatableTopicResult result = ctx.createTestTopic("foo",
            new int[][] {
                new int[] {0, 1, 2},
                new int[] {1, 2, 3},
                new int[] {2, 3, 0},
                new int[] {0, 2, 1}
            });
        Set<TopicIdPartition> expectedPartitions = new HashSet<>();
        expectedPartitions.add(new TopicIdPartition(result.topicId(), 0));
        expectedPartitions.add(new TopicIdPartition(result.topicId(), 3));
        assertEquals(expectedPartitions, RecordTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
        List<ApiMessageAndVersion> records = new ArrayList<>();
        replicationControl.handleBrokerFenced(0, records);
        ctx.replay(records);
        assertEquals(Collections.emptySet(), RecordTestUtils.
            iteratorToSet(replicationControl.brokersToIsrs().iterator(0, true)));
    }

    @Test
    public void testShrinkAndExpandIsr() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        assertEquals(OptionalInt.of(0), ctx.currentLeader(topicIdPartition));
        long brokerEpoch = ctx.currentBrokerEpoch(0);
        PartitionData shrinkIsrRequest = newAlterPartition(
            replicationControl, topicIdPartition, asList(0, 1), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> shrinkIsrResult = sendAlterIsr(
            replicationControl, 0, brokerEpoch, "foo", shrinkIsrRequest);
        AlterPartitionResponseData.PartitionData shrinkIsrResponse = assertAlterPartitionResponse(
            shrinkIsrResult, topicPartition, NONE);
        assertConsistentAlterPartitionResponse(replicationControl, topicIdPartition, shrinkIsrResponse);

        PartitionData expandIsrRequest = newAlterPartition(
            replicationControl, topicIdPartition, asList(0, 1, 2), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> expandIsrResult = sendAlterIsr(
            replicationControl, 0, brokerEpoch, "foo", expandIsrRequest);
        AlterPartitionResponseData.PartitionData expandIsrResponse = assertAlterPartitionResponse(
            expandIsrResult, topicPartition, NONE);
        assertConsistentAlterPartitionResponse(replicationControl, topicIdPartition, expandIsrResponse);
    }

    @Test
    public void testInvalidAlterPartitionRequests() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        CreatableTopicResult createTopicResult = ctx.createTestTopic("foo",
            new int[][] {new int[] {0, 1, 2}});

        TopicIdPartition topicIdPartition = new TopicIdPartition(createTopicResult.topicId(), 0);
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        int leaderId = 0;
        int notLeaderId = 1;
        assertEquals(OptionalInt.of(leaderId), ctx.currentLeader(topicIdPartition));
        long brokerEpoch = ctx.currentBrokerEpoch(0);

        // Invalid leader
        PartitionData invalidLeaderRequest = newAlterPartition(
            replicationControl, topicIdPartition, asList(0, 1), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> invalidLeaderResult = sendAlterIsr(
            replicationControl, notLeaderId, ctx.currentBrokerEpoch(notLeaderId),
            "foo", invalidLeaderRequest);
        assertAlterPartitionResponse(invalidLeaderResult, topicPartition, Errors.INVALID_REQUEST);

        // Stale broker epoch
        PartitionData invalidBrokerEpochRequest = newAlterPartition(
            replicationControl, topicIdPartition, asList(0, 1), LeaderRecoveryState.RECOVERED);
        assertThrows(StaleBrokerEpochException.class, () -> sendAlterIsr(
            replicationControl, leaderId, brokerEpoch - 1, "foo", invalidBrokerEpochRequest));

        // Invalid leader epoch
        PartitionData invalidLeaderEpochRequest = newAlterPartition(
            replicationControl, topicIdPartition, asList(0, 1), LeaderRecoveryState.RECOVERED);
        invalidLeaderEpochRequest.setLeaderEpoch(500);
        ControllerResult<AlterPartitionResponseData> invalidLeaderEpochResult = sendAlterIsr(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            "foo", invalidLeaderEpochRequest);
        assertAlterPartitionResponse(invalidLeaderEpochResult, topicPartition, FENCED_LEADER_EPOCH);

        // Invalid ISR (3 is not a valid replica)
        PartitionData invalidIsrRequest1 = newAlterPartition(
            replicationControl, topicIdPartition, asList(0, 1, 3), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> invalidIsrResult1 = sendAlterIsr(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            "foo", invalidIsrRequest1);
        assertAlterPartitionResponse(invalidIsrResult1, topicPartition, Errors.INVALID_REQUEST);

        // Invalid ISR (does not include leader 0)
        PartitionData invalidIsrRequest2 = newAlterPartition(
            replicationControl, topicIdPartition, asList(1, 2), LeaderRecoveryState.RECOVERED);
        ControllerResult<AlterPartitionResponseData> invalidIsrResult2 = sendAlterIsr(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            "foo", invalidIsrRequest2);
        assertAlterPartitionResponse(invalidIsrResult2, topicPartition, Errors.INVALID_REQUEST);

        // Invalid ISR length and recovery state
        PartitionData invalidIsrRecoveryRequest = newAlterPartition(
            replicationControl, topicIdPartition, asList(0, 1), LeaderRecoveryState.RECOVERING);
        ControllerResult<AlterPartitionResponseData> invalidIsrRecoveryResult = sendAlterIsr(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            "foo", invalidIsrRecoveryRequest);
        assertAlterPartitionResponse(invalidIsrRecoveryResult, topicPartition, Errors.INVALID_REQUEST);

        // Invalid recovery state transtion from RECOVERED to RECOVERING
        PartitionData invalidRecoveryRequest = newAlterPartition(
            replicationControl, topicIdPartition, asList(0), LeaderRecoveryState.RECOVERING);
        ControllerResult<AlterPartitionResponseData> invalidRecoveryResult = sendAlterIsr(
            replicationControl, leaderId, ctx.currentBrokerEpoch(leaderId),
            "foo", invalidRecoveryRequest);
        assertAlterPartitionResponse(invalidRecoveryResult, topicPartition, Errors.INVALID_REQUEST);
    }

    private PartitionData newAlterPartition(
        ReplicationControlManager replicationControl,
        TopicIdPartition topicIdPartition,
        List<Integer> newIsr,
        LeaderRecoveryState leaderRecoveryState
    ) {
        PartitionRegistration partitionControl =
            replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        return new AlterPartitionRequestData.PartitionData()
            .setPartitionIndex(0)
            .setLeaderEpoch(partitionControl.leaderEpoch)
            .setPartitionEpoch(partitionControl.partitionEpoch)
            .setNewIsr(newIsr)
            .setLeaderRecoveryState(leaderRecoveryState.value());
    }

    private ControllerResult<AlterPartitionResponseData> sendAlterIsr(
        ReplicationControlManager replicationControl,
        int brokerId,
        long brokerEpoch,
        String topic,
        AlterPartitionRequestData.PartitionData partitionData
    ) throws Exception {
        AlterPartitionRequestData request = new AlterPartitionRequestData()
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);

        AlterPartitionRequestData.TopicData topicData = new AlterPartitionRequestData.TopicData()
            .setName(topic);
        request.topics().add(topicData);
        topicData.partitions().add(partitionData);

        ControllerResult<AlterPartitionResponseData> result = replicationControl.alterPartition(request);
        RecordTestUtils.replayAll(replicationControl, result.records());
        return result;
    }

    private AlterPartitionResponseData.PartitionData assertAlterPartitionResponse(
        ControllerResult<AlterPartitionResponseData> alterPartitionResult,
        TopicPartition topicPartition,
        Errors expectedError
    ) {
        AlterPartitionResponseData response = alterPartitionResult.response();
        assertEquals(1, response.topics().size());

        AlterPartitionResponseData.TopicData topicData = response.topics().get(0);
        assertEquals(topicPartition.topic(), topicData.name());
        assertEquals(1, topicData.partitions().size());

        AlterPartitionResponseData.PartitionData partitionData = topicData.partitions().get(0);
        assertEquals(topicPartition.partition(), partitionData.partitionIndex());
        assertEquals(expectedError, Errors.forCode(partitionData.errorCode()));
        return partitionData;
    }

    private void assertConsistentAlterPartitionResponse(
        ReplicationControlManager replicationControl,
        TopicIdPartition topicIdPartition,
        AlterPartitionResponseData.PartitionData partitionData
    ) {
        PartitionRegistration partitionControl =
            replicationControl.getPartition(topicIdPartition.topicId(), topicIdPartition.partitionId());
        assertEquals(partitionControl.leader, partitionData.leaderId());
        assertEquals(partitionControl.leaderEpoch, partitionData.leaderEpoch());
        assertEquals(partitionControl.partitionEpoch, partitionData.partitionEpoch());
        List<Integer> expectedIsr = IntStream.of(partitionControl.isr).boxed().collect(Collectors.toList());
        assertEquals(expectedIsr, partitionData.isr());
    }

    private void assertCreatedTopicConfigs(
        ReplicationControlTestContext ctx,
        String topic,
        CreateTopicsRequestData.CreateableTopicConfigCollection requestConfigs
    ) {
        Map<String, String> configs = ctx.configurationControl.getConfigs(
            new ConfigResource(ConfigResource.Type.TOPIC, topic));
        assertEquals(requestConfigs.size(), configs.size());
        for (CreateTopicsRequestData.CreateableTopicConfig requestConfig : requestConfigs) {
            String value = configs.get(requestConfig.name());
            assertEquals(requestConfig.value(), value);
        }
    }

    private void assertEmptyTopicConfigs(
        ReplicationControlTestContext ctx,
        String topic
    ) {
        Map<String, String> configs = ctx.configurationControl.getConfigs(
            new ConfigResource(ConfigResource.Type.TOPIC, topic));
        assertEquals(Collections.emptyMap(), configs);
    }

    @Test
    public void testDeleteTopics() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreateTopicsRequestData.CreateableTopicConfigCollection requestConfigs =
            new CreateTopicsRequestData.CreateableTopicConfigCollection();
        requestConfigs.add(new CreateTopicsRequestData.CreateableTopicConfig().
            setName("cleanup.policy").setValue("compact"));
        requestConfigs.add(new CreateTopicsRequestData.CreateableTopicConfig().
            setName("min.cleanable.dirty.ratio").setValue("0.1"));
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2).
            setConfigs(requestConfigs));
        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);
        ControllerResult<CreateTopicsResponseData> createResult =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        Uuid topicId = createResult.response().topics().find("foo").topicId();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2).
            setErrorMessage(null).setErrorCode((short) 0).
            setTopicId(topicId));
        assertEquals(expectedResponse, createResult.response());
        // Until the records are replayed, no changes are made
        assertNull(replicationControl.getPartition(topicId, 0));
        assertEmptyTopicConfigs(ctx, "foo");
        ctx.replay(createResult.records());
        assertNotNull(replicationControl.getPartition(topicId, 0));
        assertNotNull(replicationControl.getPartition(topicId, 1));
        assertNotNull(replicationControl.getPartition(topicId, 2));
        assertNull(replicationControl.getPartition(topicId, 3));
        assertCreatedTopicConfigs(ctx, "foo", requestConfigs);

        assertEquals(singletonMap(topicId, new ResultOrError<>("foo")),
            replicationControl.findTopicNames(Long.MAX_VALUE, Collections.singleton(topicId)));
        assertEquals(singletonMap("foo", new ResultOrError<>(topicId)),
            replicationControl.findTopicIds(Long.MAX_VALUE, Collections.singleton("foo")));
        Uuid invalidId = new Uuid(topicId.getMostSignificantBits() + 1,
            topicId.getLeastSignificantBits());
        assertEquals(singletonMap(invalidId,
            new ResultOrError<>(new ApiError(UNKNOWN_TOPIC_ID))),
                replicationControl.findTopicNames(Long.MAX_VALUE, Collections.singleton(invalidId)));
        assertEquals(singletonMap("bar",
            new ResultOrError<>(new ApiError(UNKNOWN_TOPIC_OR_PARTITION))),
                replicationControl.findTopicIds(Long.MAX_VALUE, Collections.singleton("bar")));

        ControllerResult<Map<Uuid, ApiError>> invalidDeleteResult = replicationControl.
            deleteTopics(Collections.singletonList(invalidId));
        assertEquals(0, invalidDeleteResult.records().size());
        assertEquals(singletonMap(invalidId, new ApiError(UNKNOWN_TOPIC_ID, null)),
            invalidDeleteResult.response());
        ControllerResult<Map<Uuid, ApiError>> deleteResult = replicationControl.
            deleteTopics(Collections.singletonList(topicId));
        assertTrue(deleteResult.isAtomic());
        assertEquals(singletonMap(topicId, new ApiError(NONE, null)),
            deleteResult.response());
        assertEquals(1, deleteResult.records().size());
        ctx.replay(deleteResult.records());
        assertNull(replicationControl.getPartition(topicId, 0));
        assertNull(replicationControl.getPartition(topicId, 1));
        assertNull(replicationControl.getPartition(topicId, 2));
        assertNull(replicationControl.getPartition(topicId, 3));
        assertEquals(singletonMap(topicId, new ResultOrError<>(
            new ApiError(UNKNOWN_TOPIC_ID))), replicationControl.findTopicNames(
                Long.MAX_VALUE, Collections.singleton(topicId)));
        assertEquals(singletonMap("foo", new ResultOrError<>(
            new ApiError(UNKNOWN_TOPIC_OR_PARTITION))), replicationControl.findTopicIds(
                Long.MAX_VALUE, Collections.singleton("foo")));
        assertEmptyTopicConfigs(ctx, "foo");
    }


    @Test
    public void testCreatePartitions() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replicationControl = ctx.replicationControl;
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreatableTopic().setName("foo").
            setNumPartitions(3).setReplicationFactor((short) 2));
        request.topics().add(new CreatableTopic().setName("bar").
            setNumPartitions(4).setReplicationFactor((short) 2));
        request.topics().add(new CreatableTopic().setName("quux").
            setNumPartitions(2).setReplicationFactor((short) 2));
        request.topics().add(new CreatableTopic().setName("foo2").
            setNumPartitions(2).setReplicationFactor((short) 2));
        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);
        ControllerResult<CreateTopicsResponseData> createTopicResult =
            replicationControl.createTopics(request);
        ctx.replay(createTopicResult.records());
        List<CreatePartitionsTopic> topics = new ArrayList<>();
        topics.add(new CreatePartitionsTopic().
            setName("foo").setCount(5).setAssignments(null));
        topics.add(new CreatePartitionsTopic().
            setName("bar").setCount(3).setAssignments(null));
        topics.add(new CreatePartitionsTopic().
            setName("baz").setCount(3).setAssignments(null));
        topics.add(new CreatePartitionsTopic().
            setName("quux").setCount(2).setAssignments(null));
        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult =
            replicationControl.createPartitions(topics);
        assertEquals(asList(new CreatePartitionsTopicResult().
                setName("foo").
                setErrorCode(NONE.code()).
                setErrorMessage(null),
            new CreatePartitionsTopicResult().
                setName("bar").
                setErrorCode(INVALID_PARTITIONS.code()).
                setErrorMessage("The topic bar currently has 4 partition(s); 3 would not be an increase."),
            new CreatePartitionsTopicResult().
                setName("baz").
                setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
                setErrorMessage(null),
            new CreatePartitionsTopicResult().
                setName("quux").
                setErrorCode(INVALID_PARTITIONS.code()).
                setErrorMessage("Topic already has 2 partition(s).")),
            createPartitionsResult.response());
        ctx.replay(createPartitionsResult.records());
        List<CreatePartitionsTopic> topics2 = new ArrayList<>();
        topics2.add(new CreatePartitionsTopic().
            setName("foo").setCount(6).setAssignments(asList(
                new CreatePartitionsAssignment().setBrokerIds(asList(1, 0)))));
        topics2.add(new CreatePartitionsTopic().
            setName("bar").setCount(5).setAssignments(asList(
            new CreatePartitionsAssignment().setBrokerIds(asList(1)))));
        topics2.add(new CreatePartitionsTopic().
            setName("quux").setCount(4).setAssignments(asList(
            new CreatePartitionsAssignment().setBrokerIds(asList(1, 0)))));
        topics2.add(new CreatePartitionsTopic().
            setName("foo2").setCount(3).setAssignments(asList(
            new CreatePartitionsAssignment().setBrokerIds(asList(2, 0)))));
        ControllerResult<List<CreatePartitionsTopicResult>> createPartitionsResult2 =
            replicationControl.createPartitions(topics2);
        assertEquals(asList(new CreatePartitionsTopicResult().
                setName("foo").
                setErrorCode(NONE.code()).
                setErrorMessage(null),
            new CreatePartitionsTopicResult().
                setName("bar").
                setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                setErrorMessage("The manual partition assignment includes a partition " +
                    "with 1 replica(s), but this is not consistent with previous " +
                    "partitions, which have 2 replica(s)."),
            new CreatePartitionsTopicResult().
                setName("quux").
                setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                setErrorMessage("Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified."),
            new CreatePartitionsTopicResult().
                setName("foo2").
                setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                setErrorMessage("The manual partition assignment includes broker 2, but " +
                    "no such broker is registered.")),
            createPartitionsResult2.response());
        ctx.replay(createPartitionsResult2.records());
    }

    @Test
    public void testValidateGoodManualPartitionAssignments() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ctx.registerBrokers(1, 2, 3);
        ctx.replicationControl.validateManualPartitionAssignment(asList(1),
            OptionalInt.of(1));
        ctx.replicationControl.validateManualPartitionAssignment(asList(1),
            OptionalInt.empty());
        ctx.replicationControl.validateManualPartitionAssignment(asList(1, 2, 3),
            OptionalInt.of(3));
        ctx.replicationControl.validateManualPartitionAssignment(asList(1, 2, 3),
            OptionalInt.empty());
    }

    @Test
    public void testValidateBadManualPartitionAssignments() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ctx.registerBrokers(1, 2);
        assertEquals("The manual partition assignment includes an empty replica list.",
            assertThrows(InvalidReplicaAssignmentException.class, () ->
                ctx.replicationControl.validateManualPartitionAssignment(asList(),
                    OptionalInt.empty())).getMessage());
        assertEquals("The manual partition assignment includes broker 3, but no such " +
            "broker is registered.", assertThrows(InvalidReplicaAssignmentException.class, () ->
                ctx.replicationControl.validateManualPartitionAssignment(asList(1, 2, 3),
                    OptionalInt.empty())).getMessage());
        assertEquals("The manual partition assignment includes the broker 2 more than " +
            "once.", assertThrows(InvalidReplicaAssignmentException.class, () ->
                ctx.replicationControl.validateManualPartitionAssignment(asList(1, 2, 2),
                    OptionalInt.empty())).getMessage());
        assertEquals("The manual partition assignment includes a partition with 2 " +
            "replica(s), but this is not consistent with previous partitions, which have " +
                "3 replica(s).", assertThrows(InvalidReplicaAssignmentException.class, () ->
                    ctx.replicationControl.validateManualPartitionAssignment(asList(1, 2),
                        OptionalInt.of(3))).getMessage());
    }

    private final static ListPartitionReassignmentsResponseData NONE_REASSIGNING =
        new ListPartitionReassignmentsResponseData().setErrorMessage(null);

    @Test
    public void testReassignPartitions() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {
            new int[] {1, 2, 3}, new int[] {3, 2, 1}}).topicId();
        ctx.createTestTopic("bar", new int[][] {
            new int[] {1, 2, 3}}).topicId();
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null));
        ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(3, 2, 1)),
                        new ReassignablePartition().setPartitionIndex(1).
                            setReplicas(asList(0, 2, 1)),
                        new ReassignablePartition().setPartitionIndex(2).
                            setReplicas(asList(0, 2, 1)))),
                new ReassignableTopic().setName("bar"))));
        assertEquals(new AlterPartitionReassignmentsResponseData().
                setErrorMessage(null).setResponses(asList(
                    new ReassignableTopicResponse().setName("foo").setPartitions(asList(
                        new ReassignablePartitionResponse().setPartitionIndex(0).
                            setErrorMessage(null),
                        new ReassignablePartitionResponse().setPartitionIndex(1).
                            setErrorMessage(null),
                        new ReassignablePartitionResponse().setPartitionIndex(2).
                            setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
                            setErrorMessage("Unable to find partition foo:2."))),
                    new ReassignableTopicResponse().
                        setName("bar"))),
            alterResult.response());
        ctx.replay(alterResult.records());
        ListPartitionReassignmentsResponseData currentReassigning =
            new ListPartitionReassignmentsResponseData().setErrorMessage(null).
                setTopics(asList(new OngoingTopicReassignment().
                    setName("foo").setPartitions(asList(
                    new OngoingPartitionReassignment().setPartitionIndex(1).
                        setRemovingReplicas(asList(3)).
                        setAddingReplicas(asList(0)).
                        setReplicas(asList(0, 2, 1, 3))))));
        assertEquals(currentReassigning, replication.listPartitionReassignments(null));
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(asList(
                new ListPartitionReassignmentsTopics().setName("bar").
                    setPartitionIndexes(asList(0, 1, 2)))));
        assertEquals(currentReassigning, replication.listPartitionReassignments(asList(
            new ListPartitionReassignmentsTopics().setName("foo").
                setPartitionIndexes(asList(0, 1, 2)))));
        ControllerResult<AlterPartitionReassignmentsResponseData> cancelResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null),
                        new ReassignablePartition().setPartitionIndex(1).
                            setReplicas(null),
                        new ReassignablePartition().setPartitionIndex(2).
                            setReplicas(null))),
                    new ReassignableTopic().setName("bar").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null))))));
        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
            new PartitionChangeRecord().setTopicId(fooId).
                setPartitionId(1).
                setReplicas(asList(2, 1, 3)).
                setLeader(3).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()), (short) 0)),
            new AlterPartitionReassignmentsResponseData().setErrorMessage(null).setResponses(asList(
                new ReassignableTopicResponse().setName("foo").setPartitions(asList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorCode(NO_REASSIGNMENT_IN_PROGRESS.code()).setErrorMessage(null),
                    new ReassignablePartitionResponse().setPartitionIndex(1).
                        setErrorCode(NONE.code()).setErrorMessage(null),
                    new ReassignablePartitionResponse().setPartitionIndex(2).
                        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()).
                        setErrorMessage("Unable to find partition foo:2."))),
                new ReassignableTopicResponse().setName("bar").setPartitions(asList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorCode(NO_REASSIGNMENT_IN_PROGRESS.code()).
                        setErrorMessage(null)))))),
            cancelResult);
        log.info("running final alterPartition...");
        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            new AlterPartitionRequestData().setBrokerId(3).setBrokerEpoch(103).
                setTopics(asList(new TopicData().setName("foo").setPartitions(asList(
                    new PartitionData().setPartitionIndex(1).setPartitionEpoch(1).
                        setLeaderEpoch(0).setNewIsr(asList(3, 0, 2, 1)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(asList(
            new AlterPartitionResponseData.TopicData().setName("foo").setPartitions(asList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(1).
                    setErrorCode(FENCED_LEADER_EPOCH.code()))))),
            alterPartitionResult.response());
        ctx.replay(alterPartitionResult.records());
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null));
    }

    @Test
    public void testCancelReassignPartitions() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {
            new int[] {1, 2, 3, 4}, new int[] {0, 1, 2, 3}, new int[] {4, 3, 1, 0},
            new int[] {2, 3, 4, 1}}).topicId();
        Uuid barId = ctx.createTestTopic("bar", new int[][] {
            new int[] {4, 3, 2}}).topicId();
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null));
        List<ApiMessageAndVersion> fenceRecords = new ArrayList<>();
        replication.handleBrokerFenced(3, fenceRecords);
        ctx.replay(fenceRecords);
        assertEquals(new PartitionRegistration(new int[] {1, 2, 3, 4}, new int[] {1, 2, 4},
            new int[] {}, new int[] {}, 1, LeaderRecoveryState.RECOVERED, 1, 1), replication.getPartition(fooId, 0));
        ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(1, 2, 3)),
                        new ReassignablePartition().setPartitionIndex(1).
                            setReplicas(asList(1, 2, 3, 0)),
                        new ReassignablePartition().setPartitionIndex(2).
                            setReplicas(asList(5, 6, 7)),
                        new ReassignablePartition().setPartitionIndex(3).
                            setReplicas(asList()))),
                new ReassignableTopic().setName("bar").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(asList(1, 2, 3, 4, 0)))))));
        assertEquals(new AlterPartitionReassignmentsResponseData().
                setErrorMessage(null).setResponses(asList(
            new ReassignableTopicResponse().setName("foo").setPartitions(asList(
                new ReassignablePartitionResponse().setPartitionIndex(0).
                    setErrorMessage(null),
                new ReassignablePartitionResponse().setPartitionIndex(1).
                    setErrorMessage(null),
                new ReassignablePartitionResponse().setPartitionIndex(2).
                    setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                    setErrorMessage("The manual partition assignment includes broker 5, " +
                        "but no such broker is registered."),
                new ReassignablePartitionResponse().setPartitionIndex(3).
                    setErrorCode(INVALID_REPLICA_ASSIGNMENT.code()).
                    setErrorMessage("The manual partition assignment includes an empty " +
                        "replica list."))),
            new ReassignableTopicResponse().setName("bar").setPartitions(asList(
                new ReassignablePartitionResponse().setPartitionIndex(0).
                    setErrorMessage(null))))),
            alterResult.response());
        ctx.replay(alterResult.records());
        assertEquals(new PartitionRegistration(new int[] {1, 2, 3}, new int[] {1, 2},
            new int[] {}, new int[] {}, 1, LeaderRecoveryState.RECOVERED, 2, 2), replication.getPartition(fooId, 0));
        assertEquals(new PartitionRegistration(new int[] {1, 2, 3, 0}, new int[] {0, 1, 2},
            new int[] {}, new int[] {}, 0, LeaderRecoveryState.RECOVERED, 1, 2), replication.getPartition(fooId, 1));
        assertEquals(new PartitionRegistration(new int[] {1, 2, 3, 4, 0}, new int[] {4, 2},
            new int[] {}, new int[] {0, 1}, 4, LeaderRecoveryState.RECOVERED, 1, 2), replication.getPartition(barId, 0));
        ListPartitionReassignmentsResponseData currentReassigning =
            new ListPartitionReassignmentsResponseData().setErrorMessage(null).
                setTopics(asList(new OngoingTopicReassignment().
                    setName("bar").setPartitions(asList(
                    new OngoingPartitionReassignment().setPartitionIndex(0).
                        setRemovingReplicas(Collections.emptyList()).
                        setAddingReplicas(asList(0, 1)).
                        setReplicas(asList(1, 2, 3, 4, 0))))));
        assertEquals(currentReassigning, replication.listPartitionReassignments(null));
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(asList(
            new ListPartitionReassignmentsTopics().setName("foo").
                setPartitionIndexes(asList(0, 1, 2)))));
        assertEquals(currentReassigning, replication.listPartitionReassignments(asList(
            new ListPartitionReassignmentsTopics().setName("bar").
                setPartitionIndexes(asList(0, 1, 2)))));
        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            new AlterPartitionRequestData().setBrokerId(4).setBrokerEpoch(104).
                setTopics(asList(new TopicData().setName("bar").setPartitions(asList(
                    new PartitionData().setPartitionIndex(0).setPartitionEpoch(2).
                        setLeaderEpoch(1).setNewIsr(asList(4, 1, 2, 3, 0)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(asList(
            new AlterPartitionResponseData.TopicData().setName("bar").setPartitions(asList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(0).
                    setLeaderId(4).
                    setLeaderEpoch(1).
                    setIsr(asList(4, 1, 2, 3, 0)).
                    setPartitionEpoch(3).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());
        ControllerResult<AlterPartitionReassignmentsResponseData> cancelResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(asList(
                    new ReassignableTopic().setName("foo").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null))),
                    new ReassignableTopic().setName("bar").setPartitions(asList(
                        new ReassignablePartition().setPartitionIndex(0).
                            setReplicas(null))))));
        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(barId).
                    setPartitionId(0).
                    setLeader(4).
                    setReplicas(asList(2, 3, 4)).
                    setRemovingReplicas(null).
                    setAddingReplicas(Collections.emptyList()), (short) 0)),
            new AlterPartitionReassignmentsResponseData().setErrorMessage(null).setResponses(asList(
                new ReassignableTopicResponse().setName("foo").setPartitions(asList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorCode(NO_REASSIGNMENT_IN_PROGRESS.code()).setErrorMessage(null))),
                new ReassignableTopicResponse().setName("bar").setPartitions(asList(
                    new ReassignablePartitionResponse().setPartitionIndex(0).
                        setErrorMessage(null)))))),
            cancelResult);
        ctx.replay(cancelResult.records());
        assertEquals(NONE_REASSIGNING, replication.listPartitionReassignments(null));
        assertEquals(new PartitionRegistration(new int[] {2, 3, 4}, new int[] {4, 2},
            new int[] {}, new int[] {}, 4, LeaderRecoveryState.RECOVERED, 2, 3), replication.getPartition(barId, 0));
    }

    @Test
    public void testManualPartitionAssignmentOnAllFencedBrokers() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}},
            INVALID_REPLICA_ASSIGNMENT.code());
    }

    @Test
    public void testCreatePartitionsFailsWithManualAssignmentWithAllFenced() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ctx.registerBrokers(0, 1, 2, 3, 4, 5);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid fooId = ctx.createTestTopic("foo", new int[][] {new int[] {0, 1, 2}}).topicId();
        ctx.createPartitions(2, "foo", new int[][] {new int[] {3, 4, 5}},
            INVALID_REPLICA_ASSIGNMENT.code());
        ctx.createPartitions(2, "foo", new int[][] {new int[] {2, 4, 5}}, NONE.code());
        assertEquals(new PartitionRegistration(new int[] {2, 4, 5},
                new int[] {2}, Replicas.NONE, Replicas.NONE, 2, LeaderRecoveryState.RECOVERED, 0, 0),
            ctx.replicationControl.getPartition(fooId, 1));
    }

    private void assertLeaderAndIsr(
        ReplicationControlManager replication,
        TopicIdPartition topicIdPartition,
        int leaderId,
        int[] isr
    ) {
        PartitionRegistration registration = replication.getPartition(
            topicIdPartition.topicId(),
            topicIdPartition.partitionId()
        );
        assertArrayEquals(isr, registration.isr);
        assertEquals(leaderId, registration.leader);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testElectUncleanLeaders(boolean electAllPartitions) throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);

        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();

        TopicIdPartition partition0 = new TopicIdPartition(fooId, 0);
        TopicIdPartition partition1 = new TopicIdPartition(fooId, 1);
        TopicIdPartition partition2 = new TopicIdPartition(fooId, 2);

        ctx.fenceBrokers(Utils.mkSet(2, 3));
        ctx.fenceBrokers(Utils.mkSet(1, 2, 3));

        assertLeaderAndIsr(replication, partition0, NO_LEADER, new int[]{1});
        assertLeaderAndIsr(replication, partition1, 4, new int[]{4});
        assertLeaderAndIsr(replication, partition2, 0, new int[]{0});

        ElectLeadersRequestData request = buildElectLeadersRequest(
            ElectionType.UNCLEAN,
            electAllPartitions ? null : singletonMap("foo", asList(0, 1, 2))
        );

        // No election can be done yet because no replicas are available for partition 0
        ControllerResult<ElectLeadersResponseData> result1 = replication.electLeaders(request);
        assertEquals(Collections.emptyList(), result1.records());

        ElectLeadersResponseData expectedResponse1 = buildElectLeadersResponse(NONE, electAllPartitions, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                new ApiError(ELIGIBLE_LEADERS_NOT_AVAILABLE)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 2),
                new ApiError(ELECTION_NOT_NEEDED)
            )
        ));
        assertElectLeadersResponse(expectedResponse1, result1.response());

        // Now we bring 2 back online which should allow the unclean election of partition 0
        ctx.unfenceBrokers(Utils.mkSet(2));

        // Bring 2 back into the ISR for partition 1. This allows us to verify that
        // preferred election does not occur as a result of the unclean election request.
        ctx.alterPartition(partition1, 4, asList(2, 4), LeaderRecoveryState.RECOVERED);

        ControllerResult<ElectLeadersResponseData> result = replication.electLeaders(request);
        assertEquals(1, result.records().size());

        ApiMessageAndVersion record = result.records().get(0);
        assertTrue(record.message() instanceof PartitionChangeRecord);

        PartitionChangeRecord partitionChangeRecord = (PartitionChangeRecord) record.message();
        assertEquals(0, partitionChangeRecord.partitionId());
        assertEquals(2, partitionChangeRecord.leader());
        assertEquals(singletonList(2), partitionChangeRecord.isr());
        ctx.replay(result.records());

        assertLeaderAndIsr(replication, partition0, 2, new int[]{2});
        assertLeaderAndIsr(replication, partition1, 4, new int[]{2, 4});
        assertLeaderAndIsr(replication, partition2, 0, new int[]{0});

        ElectLeadersResponseData expectedResponse = buildElectLeadersResponse(NONE, electAllPartitions, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                ApiError.NONE
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 2),
                new ApiError(ELECTION_NOT_NEEDED)
            )
        ));
        assertElectLeadersResponse(expectedResponse, result.response());
    }

    @Test
    public void testPreferredElectionDoesNotTriggerUncleanElection() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(1, 2, 3, 4);
        ctx.unfenceBrokers(1, 2, 3, 4);

        Uuid fooId = ctx.createTestTopic("foo", new int[][]{new int[]{1, 2, 3}}).topicId();
        TopicIdPartition partition = new TopicIdPartition(fooId, 0);

        ctx.fenceBrokers(Utils.mkSet(2, 3));
        ctx.fenceBrokers(Utils.mkSet(1, 2, 3));
        ctx.unfenceBrokers(Utils.mkSet(2));

        assertLeaderAndIsr(replication, partition, NO_LEADER, new int[]{1});

        ctx.alterTopicConfig("foo", "unclean.leader.election.enable", "true");

        ElectLeadersRequestData request = buildElectLeadersRequest(
            ElectionType.PREFERRED,
            singletonMap("foo", singletonList(0))
        );

        // No election should be done even though unclean election is available
        ControllerResult<ElectLeadersResponseData> result = replication.electLeaders(request);
        assertEquals(Collections.emptyList(), result.records());

        ElectLeadersResponseData expectedResponse = buildElectLeadersResponse(NONE, false, singletonMap(
            new TopicPartition("foo", 0), new ApiError(PREFERRED_LEADER_NOT_AVAILABLE)
        ));
        assertEquals(expectedResponse, result.response());
    }

    private ElectLeadersRequestData buildElectLeadersRequest(
        ElectionType electionType,
        Map<String, List<Integer>> partitions
    ) {
        ElectLeadersRequestData request = new ElectLeadersRequestData().
            setElectionType(electionType.value);

        if (partitions == null) {
            request.setTopicPartitions(null);
        } else {
            partitions.forEach((topic, partitionIds) -> {
                request.topicPartitions().add(new TopicPartitions()
                    .setTopic(topic)
                    .setPartitions(partitionIds)
                );
            });
        }
        return request;
    }

    @Test
    public void testFenceMultipleBrokers() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(0, 1, 2, 3, 4);

        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();

        assertTrue(ctx.clusterControl.fencedBrokerIds().isEmpty());
        ctx.fenceBrokers(Utils.mkSet(2, 3));

        PartitionRegistration partition0 = replication.getPartition(fooId, 0);
        PartitionRegistration partition1 = replication.getPartition(fooId, 1);
        PartitionRegistration partition2 = replication.getPartition(fooId, 2);

        assertArrayEquals(new int[]{1, 2, 3}, partition0.replicas);
        assertArrayEquals(new int[]{1}, partition0.isr);
        assertEquals(1, partition0.leader);

        assertArrayEquals(new int[]{2, 3, 4}, partition1.replicas);
        assertArrayEquals(new int[]{4}, partition1.isr);
        assertEquals(4, partition1.leader);

        assertArrayEquals(new int[]{0, 2, 1}, partition2.replicas);
        assertArrayEquals(new int[]{0, 1}, partition2.isr);
        assertNotEquals(2, partition2.leader);
    }

    @Test
    public void testElectPreferredLeaders() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(2, 3, 4);
        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();
        ElectLeadersRequestData request1 = new ElectLeadersRequestData().
            setElectionType(ElectionType.PREFERRED.value).
            setTopicPartitions(new TopicPartitionsCollection(asList(
                new TopicPartitions().setTopic("foo").
                    setPartitions(asList(0, 1)),
                new TopicPartitions().setTopic("bar").
                    setPartitions(asList(0, 1))).iterator()));
        ControllerResult<ElectLeadersResponseData> election1Result =
            replication.electLeaders(request1);
        ElectLeadersResponseData expectedResponse1 = buildElectLeadersResponse(NONE, false, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                new ApiError(PREFERRED_LEADER_NOT_AVAILABLE)
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 0),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 1),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            )
        ));
        assertElectLeadersResponse(expectedResponse1, election1Result.response());
        assertEquals(Collections.emptyList(), election1Result.records());
        ctx.unfenceBrokers(0, 1);

        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            new AlterPartitionRequestData().setBrokerId(2).setBrokerEpoch(102).
                setTopics(asList(new AlterPartitionRequestData.TopicData().setName("foo").
                    setPartitions(asList(new AlterPartitionRequestData.PartitionData().
                        setPartitionIndex(0).setPartitionEpoch(0).
                        setLeaderEpoch(0).setNewIsr(asList(1, 2, 3)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(asList(
            new AlterPartitionResponseData.TopicData().setName("foo").setPartitions(asList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(0).
                    setLeaderId(2).
                    setLeaderEpoch(0).
                    setIsr(asList(1, 2, 3)).
                    setPartitionEpoch(1).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());

        ElectLeadersResponseData expectedResponse2 = buildElectLeadersResponse(NONE, false, Utils.mkMap(
            Utils.mkEntry(
                new TopicPartition("foo", 0),
                ApiError.NONE
            ),
            Utils.mkEntry(
                new TopicPartition("foo", 1),
                new ApiError(ELECTION_NOT_NEEDED)
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 0),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            ),
            Utils.mkEntry(
                new TopicPartition("bar", 1),
                new ApiError(UNKNOWN_TOPIC_OR_PARTITION, "No such topic as bar")
            )
        ));

        ctx.replay(alterPartitionResult.records());
        ControllerResult<ElectLeadersResponseData> election2Result =
            replication.electLeaders(request1);
        assertElectLeadersResponse(expectedResponse2, election2Result.response());
        assertEquals(asList(new ApiMessageAndVersion(new PartitionChangeRecord().
            setPartitionId(0).
            setTopicId(fooId).
            setLeader(1), (short) 0)), election2Result.records());
    }

    @Test
    public void testBalancePartitionLeaders() throws Exception {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3, 4);
        ctx.unfenceBrokers(2, 3, 4);
        Uuid fooId = ctx.createTestTopic("foo", new int[][]{
            new int[]{1, 2, 3}, new int[]{2, 3, 4}, new int[]{0, 2, 1}}).topicId();

        assertTrue(replication.arePartitionLeadersImbalanced());
        assertEquals(2, ctx.metrics.preferredReplicaImbalanceCount());

        ctx.unfenceBrokers(1);

        ControllerResult<AlterPartitionResponseData> alterPartitionResult = replication.alterPartition(
            new AlterPartitionRequestData().setBrokerId(2).setBrokerEpoch(102).
                setTopics(asList(new AlterPartitionRequestData.TopicData().setName("foo").
                    setPartitions(asList(new AlterPartitionRequestData.PartitionData().
                        setPartitionIndex(0).setPartitionEpoch(0).
                        setLeaderEpoch(0).setNewIsr(asList(1, 2, 3)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(asList(
            new AlterPartitionResponseData.TopicData().setName("foo").setPartitions(asList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(0).
                    setLeaderId(2).
                    setLeaderEpoch(0).
                    setIsr(asList(1, 2, 3)).
                    setPartitionEpoch(1).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());
        ctx.replay(alterPartitionResult.records());

        ControllerResult<Boolean> balanceResult = replication.maybeBalancePartitionLeaders();
        ctx.replay(balanceResult.records());

        PartitionChangeRecord expectedChangeRecord = new PartitionChangeRecord()
            .setPartitionId(0)
            .setTopicId(fooId)
            .setLeader(1);
        assertEquals(asList(new ApiMessageAndVersion(expectedChangeRecord, (short) 0)), balanceResult.records());
        assertTrue(replication.arePartitionLeadersImbalanced());
        assertEquals(1, ctx.metrics.preferredReplicaImbalanceCount());
        assertFalse(balanceResult.response());

        ctx.unfenceBrokers(0);

        alterPartitionResult = replication.alterPartition(
            new AlterPartitionRequestData().setBrokerId(2).setBrokerEpoch(102).
                setTopics(asList(new AlterPartitionRequestData.TopicData().setName("foo").
                    setPartitions(asList(new AlterPartitionRequestData.PartitionData().
                        setPartitionIndex(2).setPartitionEpoch(0).
                        setLeaderEpoch(0).setNewIsr(asList(0, 2, 1)))))));
        assertEquals(new AlterPartitionResponseData().setTopics(asList(
            new AlterPartitionResponseData.TopicData().setName("foo").setPartitions(asList(
                new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(2).
                    setLeaderId(2).
                    setLeaderEpoch(0).
                    setIsr(asList(0, 2, 1)).
                    setPartitionEpoch(1).
                    setErrorCode(NONE.code()))))),
            alterPartitionResult.response());
        ctx.replay(alterPartitionResult.records());

        balanceResult = replication.maybeBalancePartitionLeaders();
        ctx.replay(balanceResult.records());

        expectedChangeRecord = new PartitionChangeRecord()
            .setPartitionId(2)
            .setTopicId(fooId)
            .setLeader(0);
        assertEquals(asList(new ApiMessageAndVersion(expectedChangeRecord, (short) 0)), balanceResult.records());
        assertFalse(replication.arePartitionLeadersImbalanced());
        assertEquals(0, ctx.metrics.preferredReplicaImbalanceCount());
        assertFalse(balanceResult.response());
    }

    private void assertElectLeadersResponse(
        ElectLeadersResponseData expected,
        ElectLeadersResponseData actual
    ) {
        assertEquals(Errors.forCode(expected.errorCode()), Errors.forCode(actual.errorCode()));
        assertEquals(collectElectLeadersErrors(expected), collectElectLeadersErrors(actual));
    }

    private Map<TopicPartition, PartitionResult> collectElectLeadersErrors(ElectLeadersResponseData response) {
        Map<TopicPartition, PartitionResult> res = new HashMap<>();
        response.replicaElectionResults().forEach(topicResult -> {
            String topic = topicResult.topic();
            topicResult.partitionResult().forEach(partitionResult -> {
                TopicPartition topicPartition = new TopicPartition(topic, partitionResult.partitionId());
                res.put(topicPartition, partitionResult);
            });
        });
        return res;
    }

    private ElectLeadersResponseData buildElectLeadersResponse(
        Errors topLevelError,
        boolean electAllPartitions,
        Map<TopicPartition, ApiError> errors
    ) {
        Map<String, List<Map.Entry<TopicPartition, ApiError>>> errorsByTopic = errors.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> entry.getKey().topic()));

        ElectLeadersResponseData response = new ElectLeadersResponseData()
            .setErrorCode(topLevelError.code());

        errorsByTopic.forEach((topic, partitionErrors) -> {
            ReplicaElectionResult electionResult = new ReplicaElectionResult().setTopic(topic);
            electionResult.setPartitionResult(partitionErrors.stream()
                .filter(entry -> !electAllPartitions || entry.getValue().error() != ELECTION_NOT_NEEDED)
                .map(entry -> {
                    TopicPartition topicPartition = entry.getKey();
                    ApiError error = entry.getValue();
                    return new PartitionResult()
                        .setPartitionId(topicPartition.partition())
                        .setErrorCode(error.error().code())
                        .setErrorMessage(error.message());
                })
                .collect(Collectors.toList()));
            response.replicaElectionResults().add(electionResult);
        });

        return response;
    }

}
