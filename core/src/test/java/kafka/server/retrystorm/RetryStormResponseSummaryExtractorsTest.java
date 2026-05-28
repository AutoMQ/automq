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

package kafka.server.retrystorm;

import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers API-specific response summary extractors and their resource classification rules. */
@Tag("S3Unit")
public class RetryStormResponseSummaryExtractorsTest {

    /** Given Produce has one success and one leader transient error, extractor reports both valid and delayable resources. */
    @Test
    public void testProduceExtractorClassifiesSuccessAndTransient() {
        ProduceResponseData data = new ProduceResponseData();
        data.responses().add(new ProduceResponseData.TopicProduceResponse().setName("topic").setPartitionResponses(List.of(
            new ProduceResponseData.PartitionProduceResponse().setIndex(0).setErrorCode(Errors.NONE.code()),
            new ProduceResponseData.PartitionProduceResponse().setIndex(1).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
        )));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.PRODUCE.apply(null, new ProduceResponse(data));
        assertTrue(summary.resources().stream().anyMatch(ResourceResult::valid));
        assertTrue(summary.resources().stream().anyMatch(ResourceResult::delayableTransient));
    }

    /** Given Fetch has a NONE partition and a fenced epoch partition, extractor reports valid and transient resources. */
    @Test
    public void testFetchExtractorClassifiesNoneAsValidAndTransientErrors() {
        FetchResponseData data = new FetchResponseData();
        data.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("topic").setPartitions(List.of(
            new FetchResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.NONE.code()),
            new FetchResponseData.PartitionData().setPartitionIndex(1).setErrorCode(Errors.FENCED_LEADER_EPOCH.code())
        )));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.FETCH.apply(null, new FetchResponse(data));
        assertTrue(summary.resources().stream().anyMatch(ResourceResult::valid));
        assertTrue(summary.resources().stream().anyMatch(ResourceResult::delayableTransient));
    }

    /** Given ListOffsets has a usable offset and unknown partition, extractor separates valid from protective error. */
    @Test
    public void testListOffsetsExtractorRequiresValidOffset() {
        ListOffsetsResponseData data = new ListOffsetsResponseData();
        data.topics().add(new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("topic").setPartitions(List.of(
            new ListOffsetsResponseData.ListOffsetsPartitionResponse().setPartitionIndex(0).setErrorCode(Errors.NONE.code()).setOffset(42L),
            new ListOffsetsResponseData.ListOffsetsPartitionResponse().setPartitionIndex(1).setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()).setOffset(-1L)
        )));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.LIST_OFFSETS.apply(null, new ListOffsetsResponse(data));
        assertTrue(summary.resources().stream().anyMatch(ResourceResult::valid));
        assertTrue(summary.resources().stream().anyMatch(resource -> resource.protective() && !resource.delayableTransient()));
    }

    /** Given old ListOffsets response carries old-style offsets, extractor treats it as a valid result. */
    @Test
    public void testListOffsetsExtractorTreatsOldStyleOffsetsAsValid() {
        ListOffsetsResponseData data = new ListOffsetsResponseData();
        data.topics().add(new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("topic").setPartitions(List.of(
            new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code())
                .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                .setOldStyleOffsets(List.of(42L))
        )));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.LIST_OFFSETS.apply(null, new ListOffsetsResponse(data));
        assertTrue(summary.resources().stream().anyMatch(resource -> resource.resourceKey().equals("topic-0") && resource.valid()));
    }

    /** Given OffsetForLeaderEpoch returns leader unavailable, extractor marks the resource delayable-transient. */
    @Test
    public void testOffsetForLeaderEpochExtractorClassifiesTransient() {
        OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResultCollection topics =
            new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResultCollection();
        topics.add(new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult().setTopic("topic").setPartitions(List.of(
            new OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setPartition(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setEndOffset(-1L)
        )).duplicate());
        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData().setTopics(topics);

        ResponseSummary summary = RetryStormResponseSummaryExtractors.OFFSET_FOR_LEADER_EPOCH.apply(
            null,
            new OffsetsForLeaderEpochResponse(data)
        );
        assertTrue(summary.resources().get(0).delayableTransient());
    }

    /** Given Metadata returns leader unavailable, extractor exposes the transient metadata error. */
    @Test
    public void testMetadataExtractorClassifiesLeaderUnavailable() {
        MetadataResponseData data = new MetadataResponseData();
        data.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
            .setPartitions(List.of(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.METADATA.apply(null, new MetadataResponse(data, (short) 12));
        assertTrue(summary.resources().stream().anyMatch(ResourceResult::delayableTransient));
    }

    /** Given all-topics Metadata includes cluster metadata, extractor reports cluster validity for immediate response. */
    @Test
    public void testMetadataExtractorTreatsAllTopicsClusterMetadataAsValid() {
        MetadataResponseData data = new MetadataResponseData();
        data.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(9092));
        data.setControllerId(1);
        data.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
            .setPartitions(List.of(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));

        MetadataRequest request = MetadataRequest.Builder.allTopics().build((short) 12);
        ResponseSummary summary = RetryStormResponseSummaryExtractors.METADATA.apply(request, new MetadataResponse(data, (short) 12));
        assertTrue(summary.resources().stream().anyMatch(resource -> resource.resourceKey().equals("cluster") && resource.valid()));
    }

    /** Given explicit-topic Metadata has cluster metadata but requested topic errors, cluster validity does not mask errors. */
    @Test
    public void testMetadataExtractorDoesNotLetClusterMetadataMaskExplicitTopicErrors() {
        MetadataRequest request = new MetadataRequest.Builder(List.of("topic"), true).build((short) 12);
        MetadataResponseData data = new MetadataResponseData();
        data.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(9092));
        data.setControllerId(1);
        data.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
            .setPartitions(List.of(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.METADATA.apply(request, new MetadataResponse(data, (short) 12));
        assertFalse(summary.resources().stream().anyMatch(resource -> resource.resourceKey().equals("cluster") && resource.valid()));
        assertFalse(summary.resources().stream().anyMatch(ResourceResult::valid));
        assertTrue(summary.resources().stream().anyMatch(ResourceResult::delayableTransient));
    }

    /** Given Metadata topic-level NONE with partition transient errors, topic validity does not mask partition errors. */
    @Test
    public void testMetadataExtractorDoesNotLetTopicNoneMaskPartitionTransientErrors() {
        MetadataRequest request = new MetadataRequest.Builder(List.of("topic"), true).build((short) 12);
        MetadataResponseData data = new MetadataResponseData();
        data.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.NONE.code())
            .setPartitions(List.of(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.METADATA.apply(request, new MetadataResponse(data, (short) 12));
        assertFalse(summary.resources().stream().anyMatch(ResourceResult::valid));
        assertTrue(summary.resources().stream().anyMatch(resource -> resource.resourceKey().equals("topic-0") && resource.delayableTransient()));
    }

    /** Given DescribeTopicPartitions topic-level NONE with partition transient errors, partition errors remain visible. */
    @Test
    public void testDescribeTopicPartitionsExtractorDoesNotLetTopicNoneMaskPartitionTransientErrors() {
        DescribeTopicPartitionsResponseData data = new DescribeTopicPartitionsResponseData();
        data.topics().add(new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.NONE.code())
            .setPartitions(List.of(new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.DESCRIBE_TOPIC_PARTITIONS.apply(
            null,
            new DescribeTopicPartitionsResponse(data)
        );
        assertFalse(summary.resources().stream().anyMatch(ResourceResult::valid));
        assertTrue(summary.resources().stream().anyMatch(resource -> resource.resourceKey().equals("topic-0") && resource.delayableTransient()));
    }

    /** Given FindCoordinator returns coordinator loading, extractor marks the coordinator resource delayable-transient. */
    @Test
    public void testFindCoordinatorExtractorClassifiesCoordinatorLoading() {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        data.coordinators().add(new FindCoordinatorResponseData.Coordinator()
            .setKey("group")
            .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            .setNodeId(-1));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.FIND_COORDINATOR.apply(null, new FindCoordinatorResponse(data));
        assertTrue(summary.resources().get(0).delayableTransient());
    }

    /** Given batch FindCoordinator uses a transaction key, extractor includes coordinator type in the resource key. */
    @Test
    public void testFindCoordinatorBatchResourceKeyIncludesCoordinatorType() {
        FindCoordinatorRequest request = new FindCoordinatorRequest.Builder(
            new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                .setCoordinatorKeys(List.of("shared-key"))
        ).build((short) 4);
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        data.coordinators().add(new FindCoordinatorResponseData.Coordinator()
            .setKey("shared-key")
            .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            .setNodeId(-1));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.FIND_COORDINATOR.apply(request, new FindCoordinatorResponse(data));
        assertEquals("transaction-shared-key", summary.resources().get(0).resourceKey());
        assertTrue(summary.resources().get(0).delayableTransient());
    }

    /** Given default registry, non-heartbeat coordinator APIs are eligible while heartbeat APIs are excluded. */
    @Test
    public void testCoordinatorApisRegisteredButHeartbeatExcluded() {
        assertTrue(RetryStormResponseSummaryExtractors.DEFAULT_REGISTRY.containsKey(ApiKeys.JOIN_GROUP));
        assertTrue(RetryStormResponseSummaryExtractors.DEFAULT_REGISTRY.containsKey(ApiKeys.OFFSET_COMMIT));
        assertFalse(RetryStormResponseSummaryExtractors.DEFAULT_REGISTRY.containsKey(ApiKeys.HEARTBEAT));
        assertFalse(RetryStormResponseSummaryExtractors.DEFAULT_REGISTRY.containsKey(ApiKeys.CONSUMER_GROUP_HEARTBEAT));
        assertFalse(RetryStormResponseSummaryExtractors.DEFAULT_REGISTRY.containsKey(ApiKeys.SHARE_GROUP_HEARTBEAT));
    }

    /** Given JoinGroup coordinator loading, extractor scopes the resource key to the requested group id. */
    @Test
    public void testCoordinatorExtractorUsesGroupResourceKey() {
        JoinGroupRequest request = new JoinGroupRequest(
            new JoinGroupRequestData()
                .setGroupId("group-a")
                .setSessionTimeoutMs(10000)
                .setRebalanceTimeoutMs(10000),
            (short) 5
        );
        short version = ApiKeys.JOIN_GROUP.latestVersion();
        JoinGroupResponse response = new JoinGroupResponse(
            new JoinGroupResponseData().setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
            version
        );

        ResponseSummary summary = RetryStormResponseSummaryExtractors.COORDINATOR.apply(request, response);
        assertEquals("group-group-a", summary.resources().get(0).resourceKey());
        assertTrue(summary.resources().get(0).delayableTransient());
    }

    /** Given OffsetFetch v8 batch has two groups, extractor keeps separate per-group resource keys. */
    @Test
    public void testOffsetFetchBatchExtractorUsesPerGroupResourceKeys() {
        OffsetFetchResponseData data = new OffsetFetchResponseData();
        data.groups().add(offsetFetchGroup("group-a", Errors.COORDINATOR_LOAD_IN_PROGRESS));
        data.groups().add(offsetFetchGroup("group-b", Errors.NOT_COORDINATOR));

        ResponseSummary summary = RetryStormResponseSummaryExtractors.COORDINATOR.apply(null, new OffsetFetchResponse(data, (short) 8));
        Set<String> resourceKeys = summary.resources().stream()
            .map(ResourceResult::resourceKey)
            .collect(Collectors.toSet());
        assertEquals(Set.of("group-group-a", "group-group-b"), resourceKeys);
        assertTrue(summary.resources().stream().allMatch(ResourceResult::delayableTransient));
    }

    private static OffsetFetchResponseData.OffsetFetchResponseGroup offsetFetchGroup(String groupId, Errors error) {
        return new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId(groupId)
            .setErrorCode(error.code())
            .setTopics(List.of(new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName("topic")
                .setPartitions(List.of(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(0)
                    .setErrorCode(error.code())
                    .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)))));
    }
}
