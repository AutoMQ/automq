/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.metadata;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers kafka-go-specific predictive leader rewrites in Metadata responses.
 */
@Tag("S3Unit")
public class KafkaGoMetadataResponseRewriterTest {

    private static final String KAFKA_GO_CLIENT_ID = "service/github.com/segmentio/kafka-go";

    /**
     * Given a kafka-go client and leader-unavailable partition with replicas, the first replica becomes the leader.
     */
    @Test
    public void testRewriteLeaderNotAvailableForKafkaGo() {
        MetadataResponsePartition partition = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2, 1));

        enabledRewriter().rewrite(KAFKA_GO_CLIENT_ID, List.of(topic(partition)));

        assertEquals(Errors.NONE.code(), partition.errorCode());
        assertEquals(2, partition.leaderId());
    }

    /**
     * Given an empty client ID used by the default kafka-go Writer, leader-unavailable metadata is rewritten.
     */
    @Test
    public void testRewriteLeaderNotAvailableForEmptyClientId() {
        MetadataResponsePartition partition = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2, 1));

        enabledRewriter().rewrite("", List.of(topic(partition)));

        assertEquals(Errors.NONE.code(), partition.errorCode());
        assertEquals(2, partition.leaderId());
    }

    /**
     * Given a non-kafka-go client, leader-unavailable metadata remains unchanged.
     */
    @Test
    public void testDoesNotRewriteOtherClients() {
        MetadataResponsePartition partition = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2));

        enabledRewriter().rewrite("apache-kafka-java", List.of(topic(partition)));

        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), partition.errorCode());
        assertEquals(-1, partition.leaderId());
    }

    /**
     * Given a kafka-go client with no replicas, leader-unavailable metadata remains unchanged.
     */
    @Test
    public void testDoesNotRewriteEmptyReplicas() {
        MetadataResponsePartition partition = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of());

        enabledRewriter().rewrite(KAFKA_GO_CLIENT_ID, List.of(topic(partition)));

        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), partition.errorCode());
        assertEquals(-1, partition.leaderId());
    }

    /**
     * Given a kafka-go client and a different partition error, metadata remains unchanged.
     */
    @Test
    public void testDoesNotRewriteOtherErrors() {
        MetadataResponsePartition partition = partition(Errors.NOT_LEADER_OR_FOLLOWER, -1, List.of(2));

        enabledRewriter().rewrite(KAFKA_GO_CLIENT_ID, List.of(topic(partition)));

        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code(), partition.errorCode());
        assertEquals(-1, partition.leaderId());
    }

    /**
     * Given mixed partition states, only eligible leader-unavailable partitions are rewritten.
     */
    @Test
    public void testRewritesOnlyEligiblePartitions() {
        MetadataResponsePartition eligible = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2));
        MetadataResponsePartition emptyReplicas = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of());
        MetadataResponsePartition otherError = partition(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, List.of(1));

        enabledRewriter().rewrite(KAFKA_GO_CLIENT_ID,
            List.of(topic(eligible, emptyReplicas), topic(otherError)));

        assertEquals(Errors.NONE.code(), eligible.errorCode());
        assertEquals(2, eligible.leaderId());
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), emptyReplicas.errorCode());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), otherError.errorCode());
    }

    /**
     * Given an eligible rewrite, leader epoch and replica health fields are preserved.
     */
    @Test
    public void testPreservesOtherPartitionFields() {
        MetadataResponsePartition partition = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2, 1))
            .setPartitionIndex(3)
            .setLeaderEpoch(9)
            .setIsrNodes(List.of(1))
            .setOfflineReplicas(List.of(2));

        enabledRewriter().rewrite(KAFKA_GO_CLIENT_ID, List.of(topic(partition)));

        assertEquals(3, partition.partitionIndex());
        assertEquals(9, partition.leaderEpoch());
        assertEquals(List.of(2, 1), partition.replicaNodes());
        assertEquals(List.of(1), partition.isrNodes());
        assertEquals(List.of(2), partition.offlineReplicas());
    }

    /**
     * Given compatibility mode is disabled, kafka-go metadata remains unchanged.
     */
    @Test
    public void testDoesNotRewriteWhenCompatibilityModeDisabled() {
        MetadataResponsePartition partition = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2, 1));

        KafkaConfig config = mock(KafkaConfig.class);
        when(config.kafkaGoMetadataCompatibilityEnabled()).thenReturn(false);

        new KafkaGoMetadataResponseRewriter(config).rewrite(KAFKA_GO_CLIENT_ID, List.of(topic(partition)));

        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), partition.errorCode());
        assertEquals(-1, partition.leaderId());
    }

    /**
     * Given compatibility mode is disabled dynamically, the existing rewriter observes the new value.
     */
    @Test
    public void testReadsCompatibilityModeForEachRewrite() {
        KafkaConfig config = mock(KafkaConfig.class);
        when(config.kafkaGoMetadataCompatibilityEnabled()).thenReturn(true);
        KafkaGoMetadataResponseRewriter rewriter = new KafkaGoMetadataResponseRewriter(config);
        MetadataResponsePartition first = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2));
        MetadataResponsePartition second = partition(Errors.LEADER_NOT_AVAILABLE, -1, List.of(2));

        rewriter.rewrite(KAFKA_GO_CLIENT_ID, List.of(topic(first)));
        rewriter.reconfigure(Map.of(AutoMQConfig.KAFKA_GO_METADATA_COMPATIBILITY_ENABLED_CONFIG, false));
        rewriter.rewrite(KAFKA_GO_CLIENT_ID, List.of(topic(second)));

        assertEquals(Errors.NONE.code(), first.errorCode());
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), second.errorCode());
    }

    private static KafkaGoMetadataResponseRewriter enabledRewriter() {
        KafkaConfig config = mock(KafkaConfig.class);
        when(config.kafkaGoMetadataCompatibilityEnabled()).thenReturn(true);
        return new KafkaGoMetadataResponseRewriter(config);
    }

    private static MetadataResponsePartition partition(Errors error, int leaderId, List<Integer> replicas) {
        return new MetadataResponsePartition()
            .setErrorCode(error.code())
            .setLeaderId(leaderId)
            .setReplicaNodes(replicas);
    }

    private static MetadataResponseTopic topic(MetadataResponsePartition... partitions) {
        return new MetadataResponseTopic().setPartitions(List.of(partitions));
    }
}
