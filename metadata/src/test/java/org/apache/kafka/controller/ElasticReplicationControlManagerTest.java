/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.es.ElasticStreamSwitch;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.util.MockRandom;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("S3Unit")
@Timeout(40)
public class ElasticReplicationControlManagerTest {
    private TestContext context;

    @BeforeEach
    public void setUp() {
        ElasticStreamSwitch.setSwitch(false);
        context = new TestContext();
    }

    @AfterEach
    public void tearDown() {
        ElasticStreamSwitch.setSwitch(false);
    }

    /**
     * Given a leaderless Elastic Partition assigned to an active Broker, when another Broker
     * unfences, then the in-progress assignment is preserved for the active owner.
     */
    @Test
    public void testUnfencePreservesLeaderlessPartitionAssignedToActiveBroker() {
        context.registerBrokers(0, 1, 2);
        context.unfenceBrokers(0, 1);
        Uuid topicId = context.createTopic("active-owner", 0);
        ElasticStreamSwitch.setSwitch(true);
        context.fenceBroker(0);
        context.assertPartition(topicId, NO_LEADER, 1);

        context.unfenceBrokers(2);

        context.assertPartition(topicId, NO_LEADER, 1);
    }

    /**
     * Given a leaderless Elastic Partition whose assigned Broker is fenced, when a new Broker
     * unfences, then the abandoned assignment is claimed to recover from scale zero.
     */
    @Test
    public void testUnfenceClaimsPartitionAssignedToFencedBroker() {
        context.registerBrokers(0, 1);
        context.unfenceBrokers(0);
        Uuid topicId = context.createTopic("fenced-owner", 0);
        ElasticStreamSwitch.setSwitch(true);
        context.fenceBroker(0);
        context.assertPartition(topicId, NO_LEADER, 0);

        context.unfenceBrokers(1);

        context.assertPartition(topicId, NO_LEADER, 1);
    }

    /**
     * Given a leaderless Elastic Partition whose assigned Broker is in controlled shutdown,
     * when another Broker unfences, then the shutting-down owner is treated as inactive.
     */
    @Test
    public void testUnfenceClaimsPartitionAssignedToBrokerInControlledShutdown() {
        context.registerBrokers(0, 1, 2);
        context.unfenceBrokers(0, 1);
        Uuid topicId = context.createTopic("shutting-down-owner", 0);
        ElasticStreamSwitch.setSwitch(true);
        context.fenceBroker(0);
        context.assertPartition(topicId, NO_LEADER, 1);
        context.putBrokerInControlledShutdown(1);

        context.unfenceBrokers(2);

        context.assertPartition(topicId, NO_LEADER, 2);
    }

    /**
     * Given an active Elastic Broker that still leads a Partition, when it enters controlled
     * shutdown, then the Controller records the Broker state without migrating the Partition.
     */
    @Test
    public void testControlledShutdownDefersElasticPartitionMigration() {
        context.registerBrokers(0, 1);
        context.unfenceBrokers(0, 1);
        Uuid topicId = context.createTopic("deferred-drain", 0);
        ElasticStreamSwitch.setSwitch(true);

        List<ApiMessageAndVersion> records = context.controlledShutdownRecords(0);

        assertEquals(1, records.size());
        assertEquals(BrokerRegistrationChangeRecord.class, records.get(0).message().getClass());
        context.replay(records);
        context.assertPartition(topicId, 0, 0);
    }

    /**
     * Given an Elastic Broker leading 205 Partitions in one Topic, when three drain ticks run,
     * then the fixed minimum target splits the Topic into batches of 100, 100, and 5.
     */
    @Test
    public void testDrainUsesFixedBatchTargetAndSplitsTopicAtBoundary() {
        context.registerBrokers(0, 1);
        context.unfenceBrokers(0, 1);
        Uuid topicId = context.createTopic("paced-drain", 205, 0);
        ElasticStreamSwitch.setSwitch(true);
        context.replay(context.controlledShutdownRecords(0));

        List<ApiMessageAndVersion> firstBatch = context.drainBroker(0);
        assertEquals(100, firstBatch.size());
        context.assertPartitionChangesTarget(firstBatch, topicId, 1);
        context.replay(firstBatch);

        List<ApiMessageAndVersion> secondBatch = context.drainBroker(0);
        assertEquals(100, secondBatch.size());
        context.assertPartitionChangesTarget(secondBatch, topicId, 1);
        context.replay(secondBatch);

        List<ApiMessageAndVersion> finalBatch = context.drainBroker(0);
        assertEquals(5, finalBatch.size());
        context.assertPartitionChangesTarget(finalBatch, topicId, 1);
    }

    /**
     * Given an Elastic Broker leading 6001 Partitions, when a drain receives a scaled target,
     * then record generation honors that 101-Partition batch boundary.
     */
    @Test
    public void testDrainHonorsScaledBatchTarget() {
        context.registerBrokers(0, 1);
        context.unfenceBrokers(0, 1);
        context.createTopic("scaled-drain", 6001, 0);
        ElasticStreamSwitch.setSwitch(true);
        context.replay(context.controlledShutdownRecords(0));

        List<ApiMessageAndVersion> batch = context.drainBroker(0, 101);

        assertEquals(101, batch.size());
    }

    /**
     * Given an Elastic drain with no active target, when a batch is generated, then the existing
     * leader-election fallback makes the Partition leaderless without changing its assignment.
     */
    @Test
    public void testDrainWithoutTargetUsesExistingLeaderElectionFallback() {
        context.registerBrokers(0);
        context.unfenceBrokers(0);
        Uuid topicId = context.createTopic("no-target", 0);
        ElasticStreamSwitch.setSwitch(true);
        context.replay(context.controlledShutdownRecords(0));

        List<ApiMessageAndVersion> batch = context.drainBroker(0);

        assertEquals(1, batch.size());
        context.replay(batch);
        context.assertPartition(topicId, NO_LEADER, 0);
    }

    /**
     * Given Partitions from two Topics fit only partially in one batch, when a drain tick runs,
     * then Topic grouping is retained and the second Topic is split at the fixed boundary.
     */
    @Test
    public void testDrainKeepsTopicGroupsContiguousWithinBatch() {
        context.registerBrokers(0, 1);
        context.unfenceBrokers(0, 1);
        Uuid firstTopicId = context.createTopic("first-topic", 60, 0);
        Uuid secondTopicId = context.createTopic("second-topic", 60, 0);
        ElasticStreamSwitch.setSwitch(true);
        context.replay(context.controlledShutdownRecords(0));

        List<ApiMessageAndVersion> batch = context.drainBroker(0);

        assertEquals(100, batch.size());
        int firstTopicChanges = context.countPartitionChanges(batch, firstTopicId);
        int secondTopicChanges = context.countPartitionChanges(batch, secondTopicId);
        assertEquals(100, firstTopicChanges + secondTopicChanges);
        assertEquals(60, Math.max(firstTopicChanges, secondTopicChanges));
        assertEquals(40, Math.min(firstTopicChanges, secondTopicChanges));
        context.assertTopicChangesContiguous(batch);
    }

    /**
     * Given the non-Elastic storage path, when a Broker enters controlled shutdown,
     * then the upstream all-at-once Partition migration remains unchanged.
     */
    @Test
    public void testNonElasticControlledShutdownMigratesImmediately() {
        context.registerBrokers(0, 1);
        context.unfenceBrokers(0, 1);
        context.createTopic("kafka-compatible", 0);

        List<ApiMessageAndVersion> records = context.controlledShutdownRecords(0);

        assertEquals(2, records.size());
        assertEquals(PartitionChangeRecord.class, records.get(1).message().getClass());
    }

    /**
     * Given the Elastic storage path, when a Broker is fenced unexpectedly,
     * then failure recovery migrates its Partition immediately rather than waiting for a tick.
     */
    @Test
    public void testElasticFencingStillMigratesImmediately() {
        context.registerBrokers(0, 1);
        context.unfenceBrokers(0, 1);
        Uuid topicId = context.createTopic("fencing-recovery", 0);
        ElasticStreamSwitch.setSwitch(true);

        context.fenceBroker(0);

        context.assertPartition(topicId, NO_LEADER, 1);
    }

    private static final class TestContext {
        private final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        private final ClusterControlManager clusterControl;
        private final ConfigurationControlManager configurationControl;
        private final ReplicationControlManager replicationControl;

        private TestContext() {
            LogContext logContext = new LogContext();
            FeatureControlManager featureControl = new FeatureControlManager.Builder()
                .setSnapshotRegistry(snapshotRegistry)
                .setQuorumFeatures(new QuorumFeatures(0,
                    QuorumFeatures.defaultFeatureMap(true), Collections.singletonList(0)))
                .setMetadataVersion(MetadataVersion.latestTesting())
                .build();
            configurationControl = new ConfigurationControlManager.Builder()
                .setSnapshotRegistry(snapshotRegistry)
                .setStaticConfig(Collections.emptyMap())
                .setKafkaConfigSchema(KafkaConfigSchema.EMPTY)
                .build();
            clusterControl = new ClusterControlManager.Builder()
                .setLogContext(logContext)
                .setTime(new MockTime())
                .setSnapshotRegistry(snapshotRegistry)
                .setSessionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .setReplicaPlacer(new StripedReplicaPlacer(new MockRandom()))
                .setFeatureControlManager(featureControl)
                .setBrokerUncleanShutdownHandler((brokerId, records) -> { })
                .setQuorumVoters(Collections.emptyList())
                .build();
            replicationControl = new ReplicationControlManager.Builder()
                .setSnapshotRegistry(snapshotRegistry)
                .setLogContext(logContext)
                .setMaxElectionsPerImbalance(Integer.MAX_VALUE)
                .setConfigurationControl(configurationControl)
                .setClusterControl(clusterControl)
                .setCreateTopicPolicy(Optional.empty())
                .setFeatureControl(featureControl)
                .build();
            clusterControl.activate();
        }

        private void registerBrokers(int... brokerIds) {
            for (int brokerId : brokerIds) {
                RegisterBrokerRecord record = new RegisterBrokerRecord()
                    .setBrokerId(brokerId)
                    .setBrokerEpoch(brokerEpoch(brokerId))
                    .setLogDirs(Collections.singletonList(Uuid.randomUuid()));
                replay(Collections.singletonList(new ApiMessageAndVersion(record, (short) 3)));
            }
        }

        private void unfenceBrokers(int... brokerIds) {
            List<ApiMessageAndVersion> records = new ArrayList<>();
            for (int brokerId : brokerIds) {
                replicationControl.handleBrokerUnfenced(brokerId, brokerEpoch(brokerId), records);
            }
            replay(records);
        }

        private void fenceBroker(int brokerId) {
            List<ApiMessageAndVersion> records = new ArrayList<>();
            replicationControl.handleBrokerFenced(brokerId, records);
            replay(records);
        }

        private void putBrokerInControlledShutdown(int brokerId) {
            BrokerRegistrationChangeRecord record = new BrokerRegistrationChangeRecord()
                .setBrokerId(brokerId)
                .setBrokerEpoch(brokerEpoch(brokerId))
                .setInControlledShutdown(
                    BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value());
            replay(Collections.singletonList(new ApiMessageAndVersion(record, (short) 1)));
        }

        private List<ApiMessageAndVersion> controlledShutdownRecords(int brokerId) {
            List<ApiMessageAndVersion> records = new ArrayList<>();
            replicationControl.handleBrokerInControlledShutdown(
                brokerId, brokerEpoch(brokerId), records);
            return records;
        }

        private List<ApiMessageAndVersion> drainBroker(int brokerId) {
            return drainBroker(brokerId, 100);
        }

        private List<ApiMessageAndVersion> drainBroker(int brokerId, int batchTarget) {
            return replicationControl.maybeDrainControlledShutdownBroker(brokerId, batchTarget).records();
        }

        private Uuid createTopic(String name, int brokerId) {
            return createTopic(name, 1, brokerId);
        }

        private Uuid createTopic(String name, int partitionCount, int brokerId) {
            CreateTopicsRequestData request = new CreateTopicsRequestData();
            CreatableTopic creatableTopic = new CreatableTopic()
                .setName(name)
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1);
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                creatableTopic.assignments().add(new CreatableReplicaAssignment()
                    .setPartitionIndex(partitionId)
                    .setBrokerIds(Collections.singletonList(brokerId)));
            }
            request.topics().add(creatableTopic);
            ControllerResult<CreateTopicsResponseData> result = replicationControl.createTopics(
                anonymousContextFor(ApiKeys.CREATE_TOPICS), request, Collections.singleton(name));
            CreatableTopicResult topic = result.response().topics().find(name);
            assertNotNull(topic);
            assertEquals(NONE.code(), topic.errorCode());
            replay(result.records());
            return topic.topicId();
        }

        private void assertPartitionChangesTarget(
            List<ApiMessageAndVersion> records,
            Uuid topicId,
            int targetBroker
        ) {
            for (ApiMessageAndVersion apiMessageAndVersion : records) {
                PartitionChangeRecord record = (PartitionChangeRecord) apiMessageAndVersion.message();
                assertEquals(topicId, record.topicId());
                assertEquals(Collections.singletonList(targetBroker), record.replicas());
                assertEquals(Collections.singletonList(targetBroker), record.isr());
                assertEquals(NO_LEADER, record.leader());
            }
        }

        private int countPartitionChanges(List<ApiMessageAndVersion> records, Uuid topicId) {
            int count = 0;
            for (ApiMessageAndVersion record : records) {
                if (((PartitionChangeRecord) record.message()).topicId().equals(topicId)) {
                    count++;
                }
            }
            return count;
        }

        private void assertTopicChangesContiguous(List<ApiMessageAndVersion> records) {
            Uuid previousTopicId = null;
            Uuid completedTopicId = null;
            for (ApiMessageAndVersion apiMessageAndVersion : records) {
                Uuid topicId = ((PartitionChangeRecord) apiMessageAndVersion.message()).topicId();
                if (previousTopicId != null && !previousTopicId.equals(topicId)) {
                    completedTopicId = previousTopicId;
                }
                if (completedTopicId != null) {
                    assertFalse(completedTopicId.equals(topicId));
                }
                previousTopicId = topicId;
            }
        }

        private void assertPartition(Uuid topicId, int leader, int assignedBroker) {
            PartitionRegistration partition = replicationControl.getPartition(topicId, 0);
            assertNotNull(partition);
            assertEquals(leader, partition.leader);
            assertArrayEquals(new int[] {assignedBroker}, partition.replicas);
            assertArrayEquals(new int[] {assignedBroker}, partition.isr);
        }

        private void replay(List<ApiMessageAndVersion> records) {
            RecordTestUtils.replayAll(clusterControl, records);
            RecordTestUtils.replayAll(configurationControl, records);
            RecordTestUtils.replayAll(replicationControl, records);
        }

        private static long brokerEpoch(int brokerId) {
            return 100L + brokerId;
        }
    }
}
