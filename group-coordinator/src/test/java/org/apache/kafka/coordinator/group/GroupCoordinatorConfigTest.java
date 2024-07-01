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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("deprecation")
public class GroupCoordinatorConfigTest {
    private static final List<ConfigDef> GROUP_COORDINATOR_CONFIG_DEFS = Arrays.asList(
            GroupCoordinatorConfig.GROUP_COORDINATOR_CONFIG_DEF,
            GroupCoordinatorConfig.NEW_GROUP_CONFIG_DEF,
            GroupCoordinatorConfig.OFFSET_MANAGEMENT_CONFIG_DEF,
            GroupCoordinatorConfig.CONSUMER_GROUP_CONFIG_DEF);

    @Test
    public void testConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 30);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, 55);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, Collections.singletonList(RangeAssignor.class));
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG, 2222);
        configs.put(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG, 3333);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, 60);
        configs.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 3000);
        configs.put(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 120);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 10 * 60 * 1000);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, 600000);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, 24 * 60 * 60 * 1000);
        configs.put(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 5000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DISABLED.name());
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, (int) CompressionType.GZIP.id);
        configs.put(GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG, 555);
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, 111);
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, (short) 11);
        configs.put(GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG, (short) 0);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 333);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 666);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 111);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 222);

        GroupCoordinatorConfig config = new GroupCoordinatorConfig(
                new AbstractConfig(Utils.mergeConfigs(GROUP_COORDINATOR_CONFIG_DEFS), configs, false));

        assertEquals(10, config.numThreads());
        assertEquals(30, config.consumerGroupSessionTimeoutMs());
        assertEquals(10, config.consumerGroupHeartbeatIntervalMs());
        assertEquals(55, config.consumerGroupMaxSize());
        assertEquals(1, config.consumerGroupAssignors().size());
        assertEquals(RangeAssignor.RANGE_ASSIGNOR_NAME, config.consumerGroupAssignors().get(0).name());
        assertEquals(2222, config.offsetsTopicSegmentBytes());
        assertEquals(3333, config.offsetMetadataMaxSize());
        assertEquals(60, config.classicGroupMaxSize());
        assertEquals(3000, config.classicGroupInitialRebalanceDelayMs());
        assertEquals(5 * 60 * 1000, config.classicGroupNewMemberJoinTimeoutMs());
        assertEquals(120, config.classicGroupMinSessionTimeoutMs());
        assertEquals(10 * 60 * 1000, config.classicGroupMaxSessionTimeoutMs());
        assertEquals(10 * 60 * 1000, config.offsetsRetentionCheckIntervalMs());
        assertEquals(Duration.ofMinutes(24 * 60 * 60 * 1000L).toMillis(), config.offsetsRetentionMs());
        assertEquals(5000, config.offsetCommitTimeoutMs());
        assertEquals(CompressionType.GZIP, config.offsetTopicCompressionType());
        assertEquals(10, config.appendLingerMs());
        assertEquals(555, config.offsetsLoadBufferSize());
        assertEquals(111, config.offsetsTopicPartitions());
        assertEquals(11, config.offsetsTopicReplicationFactor());
        assertEquals(0, config.offsetCommitRequiredAcks());
        assertEquals(333, config.consumerGroupMinSessionTimeoutMs());
        assertEquals(666, config.consumerGroupMaxSessionTimeoutMs());
        assertEquals(111, config.consumerGroupMinHeartbeatIntervalMs());
        assertEquals(222, config.consumerGroupMaxHeartbeatIntervalMs());
    }

    public static GroupCoordinatorConfig createGroupCoordinatorConfig(
        int offsetMetadataMaxSize,
        long offsetsRetentionCheckIntervalMs,
        int offsetsRetentionMinutes
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, 1);
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 45);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 5);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, Integer.MAX_VALUE);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, Collections.singletonList(RangeAssignor.class));
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG, 1000);
        configs.put(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG, offsetMetadataMaxSize);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, Integer.MAX_VALUE);
        configs.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 3000);
        configs.put(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 120);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 10 * 5 * 1000);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, offsetsRetentionCheckIntervalMs);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, offsetsRetentionMinutes);
        configs.put(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 5000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DISABLED.name());
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, (int) CompressionType.NONE.id);

        return new GroupCoordinatorConfig(
                new AbstractConfig(Utils.mergeConfigs(GROUP_COORDINATOR_CONFIG_DEFS), configs, false));
    }
}
