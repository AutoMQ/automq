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
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupMember;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.apache.kafka.common.requests.OffsetFetchResponse.INVALID_OFFSET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OffsetMetadataManagerTest {
    static class OffsetMetadataManagerTestContext {
        public static class Builder {
            private final MockTime time = new MockTime();
            private final MockCoordinatorTimer<Void, Record> timer = new MockCoordinatorTimer<>(time);
            private final LogContext logContext = new LogContext();
            private final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
            private GroupMetadataManager groupMetadataManager = null;
            private MetadataImage metadataImage = null;
            private GroupCoordinatorConfig config = null;

            Builder withOffsetMetadataMaxSize(int offsetMetadataMaxSize) {
                config = GroupCoordinatorConfigTest.createGroupCoordinatorConfig(offsetMetadataMaxSize, 60000L, 24 * 60 * 1000);
                return this;
            }

            Builder withOffsetsRetentionMs(long offsetsRetentionMs) {
                config = GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 60000L, offsetsRetentionMs);
                return this;
            }

            Builder withGroupMetadataManager(GroupMetadataManager groupMetadataManager) {
                this.groupMetadataManager = groupMetadataManager;
                return this;
            }

            OffsetMetadataManagerTestContext build() {
                if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
                if (config == null) {
                    config = GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 60000L, 24 * 60 * 1000);
                }

                if (groupMetadataManager == null) {
                    groupMetadataManager = new GroupMetadataManager.Builder()
                        .withTime(time)
                        .withTimer(timer)
                        .withSnapshotRegistry(snapshotRegistry)
                        .withLogContext(logContext)
                        .withMetadataImage(metadataImage)
                        .withConsumerGroupAssignors(Collections.singletonList(new RangeAssignor()))
                        .build();
                }

                OffsetMetadataManager offsetMetadataManager = new OffsetMetadataManager.Builder()
                    .withTime(time)
                    .withLogContext(logContext)
                    .withSnapshotRegistry(snapshotRegistry)
                    .withMetadataImage(metadataImage)
                    .withGroupMetadataManager(groupMetadataManager)
                    .withGroupCoordinatorConfig(config)
                    .build();

                return new OffsetMetadataManagerTestContext(
                    time,
                    timer,
                    snapshotRegistry,
                    groupMetadataManager,
                    offsetMetadataManager
                );
            }
        }

        final MockTime time;
        final MockCoordinatorTimer<Void, Record> timer;
        final SnapshotRegistry snapshotRegistry;
        final GroupMetadataManager groupMetadataManager;
        final OffsetMetadataManager offsetMetadataManager;

        long lastCommittedOffset = 0L;
        long lastWrittenOffset = 0L;

        OffsetMetadataManagerTestContext(
            MockTime time,
            MockCoordinatorTimer<Void, Record> timer,
            SnapshotRegistry snapshotRegistry,
            GroupMetadataManager groupMetadataManager,
            OffsetMetadataManager offsetMetadataManager
        ) {
            this.time = time;
            this.timer = timer;
            this.snapshotRegistry = snapshotRegistry;
            this.groupMetadataManager = groupMetadataManager;
            this.offsetMetadataManager = offsetMetadataManager;
        }

        public CoordinatorResult<OffsetCommitResponseData, Record> commitOffset(
            OffsetCommitRequestData request
        ) {
            return commitOffset(ApiKeys.OFFSET_COMMIT.latestVersion(), request);
        }

        public CoordinatorResult<OffsetCommitResponseData, Record> commitOffset(
            short version,
            OffsetCommitRequestData request
        ) {
            snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);

            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.OFFSET_COMMIT,
                    version,
                    "client",
                    0
                ),
                "1",
                InetAddress.getLoopbackAddress(),
                KafkaPrincipal.ANONYMOUS,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT,
                ClientInformation.EMPTY,
                false
            );

            CoordinatorResult<OffsetCommitResponseData, Record> result = offsetMetadataManager.commitOffset(
                context,
                request
            );

            result.records().forEach(this::replay);
            return result;
        }

        public CoordinatorResult<OffsetDeleteResponseData, Record> deleteOffsets(
            OffsetDeleteRequestData request
        ) {
            CoordinatorResult<OffsetDeleteResponseData, Record> result = offsetMetadataManager.deleteOffsets(request);
            result.records().forEach(this::replay);
            return result;
        }

        public int deleteAllOffsets(
            String groupId,
            List<Record> records
        ) {
            List<Record> addedRecords = new ArrayList<>();
            int numDeletedOffsets = offsetMetadataManager.deleteAllOffsets(groupId, addedRecords);
            addedRecords.forEach(this::replay);

            records.addAll(addedRecords);
            return numDeletedOffsets;
        }

        public boolean cleanupExpiredOffsets(String groupId, List<Record> records) {
            List<Record> addedRecords = new ArrayList<>();
            boolean isOffsetsEmptyForGroup = offsetMetadataManager.cleanupExpiredOffsets(groupId, addedRecords);
            addedRecords.forEach(this::replay);

            records.addAll(addedRecords);
            return isOffsetsEmptyForGroup;
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchOffsets(
            String groupId,
            List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics,
            long committedOffset
        ) {
            return fetchOffsets(
                groupId,
                null,
                -1,
                topics,
                committedOffset
            );
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchOffsets(
            String groupId,
            String memberId,
            int memberEpoch,
            List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics,
            long committedOffset
        ) {
            OffsetFetchResponseData.OffsetFetchResponseGroup response = offsetMetadataManager.fetchOffsets(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(memberEpoch)
                    .setTopics(topics),
                committedOffset
            );
            assertEquals(groupId, response.groupId());
            return response.topics();
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchAllOffsets(
            String groupId,
            long committedOffset
        ) {
            return fetchAllOffsets(
                groupId,
                null,
                -1,
                committedOffset
            );
        }

        public List<OffsetFetchResponseData.OffsetFetchResponseTopics> fetchAllOffsets(
            String groupId,
            String memberId,
            int memberEpoch,
            long committedOffset
        ) {
            OffsetFetchResponseData.OffsetFetchResponseGroup response = offsetMetadataManager.fetchAllOffsets(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(memberEpoch),
                committedOffset
            );
            assertEquals(groupId, response.groupId());
            return response.topics();
        }

        public List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> sleep(long ms) {
            time.sleep(ms);
            List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> timeouts = timer.poll();
            timeouts.forEach(timeout -> {
                if (timeout.result.replayRecords()) {
                    timeout.result.records().forEach(this::replay);
                }
            });
            return timeouts;
        }

        public void commitOffset(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch
        ) {
            commitOffset(groupId, topic, partition, offset, leaderEpoch, time.milliseconds());

        }

        public void commitOffset(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            long commitTimestamp
        ) {
            replay(RecordHelpers.newOffsetCommitRecord(
                groupId,
                topic,
                partition,
                new OffsetAndMetadata(
                    offset,
                    OptionalInt.of(leaderEpoch),
                    "metadata",
                    commitTimestamp,
                    OptionalLong.empty()
                ),
                MetadataVersion.latest()
            ));
        }

        public void deleteOffset(
            String groupId,
            String topic,
            int partition
        ) {
            replay(RecordHelpers.newOffsetCommitTombstoneRecord(
                groupId,
                topic,
                partition
            ));
        }

        private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
            if (apiMessageAndVersion == null) {
                return null;
            } else {
                return apiMessageAndVersion.message();
            }
        }

        private void replay(
            Record record
        ) {
            snapshotRegistry.getOrCreateSnapshot(lastWrittenOffset);

            ApiMessageAndVersion key = record.key();
            ApiMessageAndVersion value = record.value();

            if (key == null) {
                throw new IllegalStateException("Received a null key in " + record);
            }

            switch (key.version()) {
                case OffsetCommitKey.HIGHEST_SUPPORTED_VERSION:
                    offsetMetadataManager.replay(
                        (OffsetCommitKey) key.message(),
                        (OffsetCommitValue) messageOrNull(value)
                    );
                    break;

                default:
                    throw new IllegalStateException("Received an unknown record type " + key.version()
                        + " in " + record);
            }

            lastWrittenOffset++;
        }

        public void testOffsetDeleteWith(
            String groupId,
            String topic,
            int partition,
            Errors expectedError
        ) {
            final OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
                new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(Collections.singletonList(
                    new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                        .setName(topic)
                        .setPartitions(Collections.singletonList(
                            new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(partition)
                        ))
                ).iterator());

            final OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection expectedResponsePartitionCollection =
                new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection();
            if (hasOffset(groupId, topic, partition)) {
                expectedResponsePartitionCollection.add(
                    new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                        .setPartitionIndex(partition)
                        .setErrorCode(expectedError.code())
                );
            }

            final OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection expectedResponseTopicCollection =
                new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(Collections.singletonList(
                    new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
                        .setName(topic)
                        .setPartitions(expectedResponsePartitionCollection)
                ).iterator());

            List<Record> expectedRecords = Collections.emptyList();
            if (hasOffset(groupId, topic, partition) && expectedError == Errors.NONE) {
                expectedRecords = Collections.singletonList(
                    RecordHelpers.newOffsetCommitTombstoneRecord(groupId, topic, partition)
                );
            }

            final CoordinatorResult<OffsetDeleteResponseData, Record> coordinatorResult = deleteOffsets(
                new OffsetDeleteRequestData()
                    .setGroupId(groupId)
                    .setTopics(requestTopicCollection)
            );

            assertEquals(new OffsetDeleteResponseData().setTopics(expectedResponseTopicCollection), coordinatorResult.response());
            assertEquals(expectedRecords, coordinatorResult.records());
        }

        public boolean hasOffset(
            String groupId,
            String topic,
            int partition
        ) {
            return offsetMetadataManager.offset(groupId, topic, partition) != null;
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testOffsetCommitWithUnknownGroup(short version) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        Class<? extends Throwable> expectedType;
        if (version >= 9) {
            expectedType = GroupIdNotFoundException.class;
        } else {
            expectedType = IllegalGenerationException.class;
        }

        // Verify that the request is rejected with the correct exception.
        assertThrows(expectedType, () -> context.commitOffset(
            version,
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithDeadGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a dead group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );
        group.transitionTo(GenericGroupState.DEAD);

        // Verify that the request is rejected with the correct exception.
        assertThrows(CoordinatorNotAvailableException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithIllegalGeneration() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(IllegalGenerationException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithUnknownInstanceId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member without static id.
        group.add(mkGenericMember("member", Optional.empty()));

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGroupInstanceId("instanceid")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithFencedInstanceId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member with static id.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGroupInstanceId("old-instance-id")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWhileInCompletingRebalanceState() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(RebalanceInProgressException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithoutMemberIdAndGeneration() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testGenericGroupOffsetCommitWithRetentionTime() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        group.add(mkGenericMember("member", Optional.of("new-instance-id")));

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(GenericGroupState.STABLE);

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setRetentionTimeMs(1234L)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.of(context.time.milliseconds() + 1234L)
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testGenericGroupOffsetCommitMaintainsSession() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );

        // Add member.
        GenericGroupMember member = mkGenericMember("member", Optional.empty());
        group.add(member);

        // Transition to next generation.
        group.transitionTo(GenericGroupState.PREPARING_REBALANCE);
        group.initNextGeneration();
        assertEquals(1, group.generationId());
        group.transitionTo(GenericGroupState.STABLE);

        // Schedule session timeout. This would be normally done when
        // the group transitions to stable.
        context.groupMetadataManager.rescheduleGenericGroupMemberHeartbeat(group, member);

        // Advance time by half of the session timeout. No timeouts are
        // expired.
        assertEquals(Collections.emptyList(), context.sleep(5000 / 2));

        // Commit.
        context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(1)
                .setRetentionTimeMs(1234L)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        // Advance time by half of the session timeout. No timeouts are
        // expired.
        assertEquals(Collections.emptyList(), context.sleep(5000 / 2));

        // Advance time by half of the session timeout again. The timeout should
        // expire and the member is removed from the group.
        List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> timeouts =
            context.sleep(5000 / 2);
        assertEquals(1, timeouts.size());
        assertFalse(group.hasMemberId(member.memberId()));
    }

    @Test
    public void testSimpleGroupOffsetCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );

        // A generic should have been created.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            false
        );
        assertNotNull(group);
        assertEquals("foo", group.groupId());
    }

    @Test
    public void testSimpleGroupOffsetCommitWithInstanceId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                // Instance id should be ignored.
                .setGroupInstanceId("instance-id")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnknownMemberIdException.class, () -> context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        OffsetCommitRequestData request = new OffsetCommitRequestData()
            .setGroupId("foo")
            .setMemberId("member")
            .setGenerationIdOrMemberEpoch(9)
            .setTopics(Collections.singletonList(
                new OffsetCommitRequestData.OffsetCommitRequestTopic()
                    .setName("bar")
                    .setPartitions(Collections.singletonList(
                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                            .setPartitionIndex(0)
                            .setCommittedOffset(100L)
                    ))
            ));

        // Verify that a smaller epoch is rejected.
        assertThrows(StaleMemberEpochException.class, () -> context.commitOffset(request));

        // Verify that a larger epoch is rejected.
        request.setGenerationIdOrMemberEpoch(11);
        assertThrows(StaleMemberEpochException.class, () -> context.commitOffset(request));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testConsumerGroupOffsetCommitWithVersionSmallerThanVersion9(short version) {
        // All the newer versions are fine.
        if (version >= 9) return;
        // Version 0 does not support MemberId and GenerationIdOrMemberEpoch fields.
        if (version == 0) return;

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        // Verify that the request is rejected with the correct exception.
        assertThrows(UnsupportedVersionException.class, () -> context.commitOffset(
            version,
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(9)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
            )
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitFromAdminClient() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.empty(),
                    "",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupOffsetCommit() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("metadata")
                                .setCommitTimestamp(context.time.milliseconds())
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Collections.singletonList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                0,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "metadata",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testConsumerGroupOffsetCommitWithOffsetMetadataTooLarge() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withOffsetMetadataMaxSize(5)
            .build();

        // Create an empty group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );

        // Add member.
        group.updateMember(new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()
        );

        CoordinatorResult<OffsetCommitResponseData, Record> result = context.commitOffset(
            new OffsetCommitRequestData()
                .setGroupId("foo")
                .setMemberId("member")
                .setGenerationIdOrMemberEpoch(10)
                .setTopics(Collections.singletonList(
                    new OffsetCommitRequestData.OffsetCommitRequestTopic()
                        .setName("bar")
                        .setPartitions(Arrays.asList(
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("toolarge")
                                .setCommitTimestamp(context.time.milliseconds()),
                            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                .setPartitionIndex(1)
                                .setCommittedOffset(100L)
                                .setCommittedLeaderEpoch(10)
                                .setCommittedMetadata("small")
                                .setCommitTimestamp(context.time.milliseconds())
                        ))
                ))
        );

        assertEquals(
            new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponseTopic()
                        .setName("bar")
                        .setPartitions(Arrays.asList(
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.OFFSET_METADATA_TOO_LARGE.code()),
                            new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                .setPartitionIndex(1)
                                .setErrorCode(Errors.NONE.code())
                        ))
                )),
            result.response()
        );

        assertEquals(
            Collections.singletonList(RecordHelpers.newOffsetCommitRecord(
                "foo",
                "bar",
                1,
                new OffsetAndMetadata(
                    100L,
                    OptionalInt.of(10),
                    "small",
                    context.time.milliseconds(),
                    OptionalLong.empty()
                ),
                MetadataImage.EMPTY.features().metadataVersion()
            )),
            result.records()
        );
    }

    @Test
    public void testGenericGroupFetchOffsetsWithDeadGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a dead group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "group",
            true
        );
        group.transitionTo(GenericGroupState.DEAD);

        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = Arrays.asList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Arrays.asList(0, 1)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setPartitionIndexes(Collections.singletonList(0))
        );

        List<OffsetFetchResponseData.OffsetFetchResponseTopics> expectedResponse = Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Collections.singletonList(
                    mkInvalidOffsetPartitionResponse(0)
                ))
        );

        assertEquals(expectedResponse, context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testFetchOffsetsWithUnknownGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = Arrays.asList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Arrays.asList(0, 1)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setPartitionIndexes(Collections.singletonList(0))
        );

        List<OffsetFetchResponseData.OffsetFetchResponseTopics> expectedResponse = Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Collections.singletonList(
                    mkInvalidOffsetPartitionResponse(0)
                ))
        );

        assertEquals(expectedResponse, context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testFetchOffsetsAtDifferentCommittedOffset() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreateConsumerGroup("group", true);

        assertEquals(0, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 0, 100L, 1);
        assertEquals(1, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 110L, 1);
        assertEquals(2, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 0, 200L, 1);
        assertEquals(3, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 111L, 2);
        assertEquals(4, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 1, 210L, 2);
        assertEquals(5, context.lastWrittenOffset);

        // Always use the same request.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> request = Arrays.asList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Arrays.asList(0, 1)),
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("bar")
                .setPartitionIndexes(Arrays.asList(0, 1))
        );

        // Fetching with 0 should return all invalid offsets.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 0L));

        // Fetching with 1 should return data up to offset 1.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkInvalidOffsetPartitionResponse(1)
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 1L));

        // Fetching with 2 should return data up to offset 2.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkInvalidOffsetPartitionResponse(0),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 2L));

        // Fetching with 3 should return data up to offset 3.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 3L));

        // Fetching with 4 should return data up to offset 4.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkInvalidOffsetPartitionResponse(1)
                ))
        ), context.fetchOffsets("group", request, 4L));

        // Fetching with 5 should return data up to offset 5.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 210L, 2, "metadata")
                ))
        ), context.fetchOffsets("group", request, 5L));

        // Fetching with Long.MAX_VALUE should return all offsets.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 210L, 2, "metadata")
                ))
        ), context.fetchOffsets("group", request, Long.MAX_VALUE));
    }

    @Test
    public void testGenericGroupFetchAllOffsetsWithDeadGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Create a dead group.
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "group",
            true
        );
        group.transitionTo(GenericGroupState.DEAD);

        assertEquals(Collections.emptyList(), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testFetchAllOffsetsWithUnknownGroup() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        assertEquals(Collections.emptyList(), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testFetchAllOffsetsAtDifferentCommittedOffset() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        context.groupMetadataManager.getOrMaybeCreateConsumerGroup("group", true);

        assertEquals(0, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 0, 100L, 1);
        assertEquals(1, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 110L, 1);
        assertEquals(2, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 0, 200L, 1);
        assertEquals(3, context.lastWrittenOffset);
        context.commitOffset("group", "foo", 1, 111L, 2);
        assertEquals(4, context.lastWrittenOffset);
        context.commitOffset("group", "bar", 1, 210L, 2);
        assertEquals(5, context.lastWrittenOffset);

        // Fetching with 0 should no offsets.
        assertEquals(Collections.emptyList(), context.fetchAllOffsets("group", 0L));

        // Fetching with 1 should return data up to offset 1.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", 1L));

        // Fetching with 2 should return data up to offset 2.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", 2L));

        // Fetching with 3 should return data up to offset 3.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 110L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", 3L));

        // Fetching with 4 should return data up to offset 4.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                ))
        ), context.fetchAllOffsets("group", 4L));

        // Fetching with Long.MAX_VALUE should return all offsets.
        assertEquals(Arrays.asList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("bar")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 200L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 210L, 2, "metadata")
                )),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Arrays.asList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata"),
                    mkOffsetPartitionResponse(1, 111L, 2, "metadata")
                ))
        ), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchWithMemberIdAndEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        // Create consumer group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup("group", true);
        // Create member.
        group.getOrMaybeCreateMember("member", true);
        // Commit offset.
        context.commitOffset("group", "foo", 0, 100L, 1);

        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = Collections.singletonList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Collections.singletonList(0))
        );

        assertEquals(Collections.singletonList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Collections.singletonList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchOffsets("group", "member", 0, topics, Long.MAX_VALUE));

        // Fetch all offsets case.
        assertEquals(Collections.singletonList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Collections.singletonList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", "member", 0, Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchFromAdminClient() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        // Create consumer group.
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup("group", true);
        // Create member.
        group.getOrMaybeCreateMember("member", true);
        // Commit offset.
        context.commitOffset("group", "foo", 0, 100L, 1);

        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = Collections.singletonList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Collections.singletonList(0))
        );

        assertEquals(Collections.singletonList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Collections.singletonList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchOffsets("group", topics, Long.MAX_VALUE));

        // Fetch all offsets case.
        assertEquals(Collections.singletonList(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("foo")
                .setPartitions(Collections.singletonList(
                    mkOffsetPartitionResponse(0, 100L, 1, "metadata")
                ))
        ), context.fetchAllOffsets("group", Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchWithUnknownMemberId() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.groupMetadataManager.getOrMaybeCreateConsumerGroup("group", true);

        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = Collections.singletonList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Collections.singletonList(0))
        );

        // Fetch offsets cases.
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchOffsets("group", "", 0, topics, Long.MAX_VALUE));
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchOffsets("group", "member", 0, topics, Long.MAX_VALUE));

        // Fetch all offsets cases.
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchAllOffsets("group", "", 0, Long.MAX_VALUE));
        assertThrows(UnknownMemberIdException.class,
            () -> context.fetchAllOffsets("group", "member", 0, Long.MAX_VALUE));
    }

    @Test
    public void testConsumerGroupOffsetFetchWithStaleMemberEpoch() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup("group", true);
        group.getOrMaybeCreateMember("member", true);

        // Fetch offsets case.
        List<OffsetFetchRequestData.OffsetFetchRequestTopics> topics = Collections.singletonList(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(Collections.singletonList(0))
        );

        // Fetch offsets case.
        assertThrows(StaleMemberEpochException.class,
            () -> context.fetchOffsets("group", "member", 10, topics, Long.MAX_VALUE));

        // Fetch all offsets case.
        assertThrows(StaleMemberEpochException.class,
            () -> context.fetchAllOffsets("group", "member", 10, Long.MAX_VALUE));
    }

    @Test
    public void testGenericGroupOffsetDelete() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );
        context.commitOffset("foo", "bar", 0, 100L, 0);
        group.setSubscribedTopics(Optional.of(Collections.emptySet()));
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.NONE);
    }

    @Test
    public void testGenericGroupOffsetDeleteWithErrors() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        GenericGroup group = context.groupMetadataManager.getOrMaybeCreateGenericGroup(
            "foo",
            true
        );
        group.setSubscribedTopics(Optional.of(Collections.singleton("bar")));
        context.commitOffset("foo", "bar", 0, 100L, 0);

        // Delete the offset whose topic partition doesn't exist.
        context.testOffsetDeleteWith("foo", "bar1", 0, Errors.NONE);
        // Delete the offset from the topic that the group is subscribed to.
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
    }

    @Test
    public void testConsumerGroupOffsetDelete() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );
        context.commitOffset("foo", "bar", 0, 100L, 0);
        assertFalse(group.isSubscribedToTopic("bar"));
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.NONE);
    }

    @Test
    public void testConsumerGroupOffsetDeleteWithErrors() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        ConsumerGroup group = context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
            "foo",
            true
        );
        MetadataImage image = new GroupMetadataManagerTest.MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "foo", 1)
            .addRacks()
            .build();
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("bar"))
            .build();
        group.computeSubscriptionMetadata(
            null,
            member1,
            image.topics(),
            image.cluster()
        );
        group.updateMember(member1);
        context.commitOffset("foo", "bar", 0, 100L, 0);
        assertTrue(group.isSubscribedToTopic("bar"));

        // Delete the offset whose topic partition doesn't exist.
        context.testOffsetDeleteWith("foo", "bar1", 0, Errors.NONE);
        // Delete the offset from the topic that the group is subscribed to.
        context.testOffsetDeleteWith("foo", "bar", 0, Errors.GROUP_SUBSCRIBED_TO_TOPIC);
    }

    @ParameterizedTest
    @EnumSource(Group.GroupType.class)
    public void testDeleteGroupAllOffsets(Group.GroupType groupType) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        switch (groupType) {
            case GENERIC:
                context.groupMetadataManager.getOrMaybeCreateGenericGroup(
                    "foo",
                    true
                );
                break;
            case CONSUMER:
                context.groupMetadataManager.getOrMaybeCreateConsumerGroup(
                    "foo",
                    true
                );
                break;
            default:
                throw new IllegalArgumentException("Invalid group type: " + groupType);
        }
        context.commitOffset("foo", "bar-0", 0, 100L, 0);
        context.commitOffset("foo", "bar-0", 1, 100L, 0);
        context.commitOffset("foo", "bar-1", 0, 100L, 0);

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-1", 0),
            RecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-0", 0),
            RecordHelpers.newOffsetCommitTombstoneRecord("foo", "bar-0", 1)
        );

        List<Record> records = new ArrayList<>();
        int numDeleteOffsets = context.deleteAllOffsets("foo", records);

        assertEquals(expectedRecords, records);
        assertEquals(3, numDeleteOffsets);
    }

    @Test
    public void testCleanupExpiredOffsetsGroupHasNoOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .build();

        List<Record> records = new ArrayList<>();
        assertTrue(context.cleanupExpiredOffsets("unknown-group-id", records));
        assertEquals(Collections.emptyList(), records);
    }

    @Test
    public void testCleanupExpiredOffsetsGroupDoesNotExist() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .build();

        when(groupMetadataManager.group("unknown-group-id")).thenThrow(GroupIdNotFoundException.class);
        context.commitOffset("unknown-group-id", "topic", 0, 100L, 0);
        assertThrows(GroupIdNotFoundException.class, () -> context.cleanupExpiredOffsets("unknown-group-id", new ArrayList<>()));
    }

    @Test
    public void testCleanupExpiredOffsetsEmptyOffsetExpirationCondition() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .build();

        context.commitOffset("group-id", "topic", 0, 100L, 0);

        when(groupMetadataManager.group("group-id")).thenReturn(group);
        when(group.offsetExpirationCondition()).thenReturn(Optional.empty());

        List<Record> records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(Collections.emptyList(), records);
    }

    @Test
    public void testCleanupExpiredOffsets() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        Group group = mock(Group.class);

        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder()
            .withGroupMetadataManager(groupMetadataManager)
            .withOffsetsRetentionMs(1000)
            .build();

        long commitTimestamp = context.time.milliseconds();

        context.commitOffset("group-id", "firstTopic", 0, 100L, 0, commitTimestamp);
        context.commitOffset("group-id", "secondTopic", 0, 100L, 0, commitTimestamp);
        context.commitOffset("group-id", "secondTopic", 1, 100L, 0, commitTimestamp + 500);

        context.time.sleep(1000);

        // firstTopic-0: group is still subscribed to firstTopic. Do not expire.
        // secondTopic-0: should expire as offset retention has passed.
        // secondTopic-1: has not passed offset retention. Do not expire.
        List<Record> expectedRecords = Collections.singletonList(
            RecordHelpers.newOffsetCommitTombstoneRecord("group-id", "secondTopic", 0)
        );

        when(groupMetadataManager.group("group-id")).thenReturn(group);
        when(group.offsetExpirationCondition()).thenReturn(Optional.of(
            new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs)));
        when(group.isSubscribedToTopic("firstTopic")).thenReturn(true);
        when(group.isSubscribedToTopic("secondTopic")).thenReturn(false);

        List<Record> records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);

        // Expire secondTopic-1.
        context.time.sleep(500);
        expectedRecords = Collections.singletonList(
            RecordHelpers.newOffsetCommitTombstoneRecord("group-id", "secondTopic", 1)
        );

        records = new ArrayList<>();
        assertFalse(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);

        // Add 2 more commits, then expire all.
        when(group.isSubscribedToTopic("firstTopic")).thenReturn(false);
        context.commitOffset("group-id", "firstTopic", 1, 100L, 0, commitTimestamp + 500);
        context.commitOffset("group-id", "secondTopic", 0, 101L, 0, commitTimestamp + 500);

        expectedRecords = Arrays.asList(
            RecordHelpers.newOffsetCommitTombstoneRecord("group-id", "firstTopic", 0),
            RecordHelpers.newOffsetCommitTombstoneRecord("group-id", "firstTopic", 1),
            RecordHelpers.newOffsetCommitTombstoneRecord("group-id", "secondTopic", 0)
        );

        records = new ArrayList<>();
        assertTrue(context.cleanupExpiredOffsets("group-id", records));
        assertEquals(expectedRecords, records);
    }

    static private OffsetFetchResponseData.OffsetFetchResponsePartitions mkOffsetPartitionResponse(
        int partition,
        long offset,
        int leaderEpoch,
        String metadata
    ) {
        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(partition)
            .setCommittedOffset(offset)
            .setCommittedLeaderEpoch(leaderEpoch)
            .setMetadata(metadata);
    }

    static private OffsetFetchResponseData.OffsetFetchResponsePartitions mkInvalidOffsetPartitionResponse(int partition) {
        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(partition)
            .setCommittedOffset(INVALID_OFFSET)
            .setCommittedLeaderEpoch(-1)
            .setMetadata("");
    }

    @Test
    public void testReplay() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            200L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        verifyReplay(context, "foo", "bar", 1, new OffsetAndMetadata(
            200L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        verifyReplay(context, "foo", "bar", 1, new OffsetAndMetadata(
            300L,
            OptionalInt.of(10),
            "small",
            context.time.milliseconds(),
            OptionalLong.of(12345L)
        ));
    }

    @Test
    public void testReplayWithTombstone() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();

        // Verify replay adds the offset the map.
        verifyReplay(context, "foo", "bar", 0, new OffsetAndMetadata(
            100L,
            OptionalInt.empty(),
            "small",
            context.time.milliseconds(),
            OptionalLong.empty()
        ));

        // Create a tombstone record and replay it to delete the record.
        context.replay(RecordHelpers.newOffsetCommitTombstoneRecord(
            "foo",
            "bar",
            0
        ));

        // Verify that the offset is gone.
        assertNull(context.offsetMetadataManager.offset("foo", "bar", 0));
    }

    private void verifyReplay(
        OffsetMetadataManagerTestContext context,
        String groupId,
        String topic,
        int partition,
        OffsetAndMetadata offsetAndMetadata
    ) {
        context.replay(RecordHelpers.newOffsetCommitRecord(
            groupId,
            topic,
            partition,
            offsetAndMetadata,
            MetadataImage.EMPTY.features().metadataVersion()
        ));

        assertEquals(offsetAndMetadata, context.offsetMetadataManager.offset(
            groupId,
            topic,
            partition
        ));
    }

    private GenericGroupMember mkGenericMember(
        String memberId,
        Optional<String> groupInstanceId
    ) {
        return new GenericGroupMember(
            memberId,
            groupInstanceId,
            "client-id",
            "host",
            5000,
            5000,
            "consumer",
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName("range")
                    .setMetadata(new byte[0])
                ).iterator()
            )
        );
    }
}
