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

package kafka.server;

import kafka.network.RequestChannel;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Covers shared resource error extraction for request error accumulation and retry storm policy.
 */
@Tag("S3Unit")
@SuppressWarnings("ClassDataAbstractionCoupling")
public class ResourceErrorExtractorTest {

    /**
     * Given Produce has one success and one error, the error is partition-scoped but not all-error.
     */
    @Test
    public void testProducePartialSuccessReturnsPartitionErrorButNotAllError() throws Exception {
        ProduceRequest request = new ProduceRequest.Builder(
            ApiKeys.PRODUCE.latestVersion(),
            ApiKeys.PRODUCE.latestVersion(),
            new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000)
        ).build();
        ProduceResponseData responseData = new ProduceResponseData();
        responseData.responses().add(new ProduceResponseData.TopicProduceResponse().setName("topic").setPartitionResponses(List.of(
            new ProduceResponseData.PartitionProduceResponse().setIndex(0).setErrorCode(Errors.NONE.code()),
            new ProduceResponseData.PartitionProduceResponse().setIndex(1).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
        )));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new ProduceResponse(responseData));

        assertFalse(view.allErrorCandidate());
        assertEquals(List.of(new ResourceErrorExtractor.ResourceError(Errors.NOT_LEADER_OR_FOLLOWER.code(), "topic-1")), view.errors());
    }

    /**
     * Given Produce has only partition errors, the view is an all-error candidate with partition keys.
     */
    @Test
    public void testProduceAllErrorsReturnsAllErrorCandidate() throws Exception {
        ProduceRequest request = new ProduceRequest.Builder(
            ApiKeys.PRODUCE.latestVersion(),
            ApiKeys.PRODUCE.latestVersion(),
            new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000)
        ).build();
        ProduceResponseData responseData = new ProduceResponseData();
        responseData.responses().add(new ProduceResponseData.TopicProduceResponse().setName("topic").setPartitionResponses(List.of(
            new ProduceResponseData.PartitionProduceResponse().setIndex(0).setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()),
            new ProduceResponseData.PartitionProduceResponse().setIndex(1).setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
        )));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new ProduceResponse(responseData));

        assertTrue(view.allErrorCandidate());
        Set<String> resourceKeys = view.errors().stream()
            .map(ResourceErrorExtractor.ResourceError::resource)
            .collect(Collectors.toSet());
        assertEquals(Set.of("topic-0", "topic-1"), resourceKeys);
    }

    /**
     * Given Fetch has a NONE partition with empty records and another error, it is not all-error.
     */
    @Test
    public void testFetchNoneEmptyRecordsCountsAsValidResult() throws Exception {
        FetchRequest request = FetchRequest.Builder
            .forConsumer(ApiKeys.FETCH.latestVersion(), 100, 1024, FetchRequestDataFixtures.emptyFetchData())
            .build();
        FetchResponseData responseData = new FetchResponseData();
        responseData.responses().add(new FetchResponseData.FetchableTopicResponse().setTopic("topic").setPartitions(List.of(
            new FetchResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.NONE.code()),
            new FetchResponseData.PartitionData().setPartitionIndex(1).setErrorCode(Errors.FENCED_LEADER_EPOCH.code())
        )));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new FetchResponse(responseData));

        assertFalse(view.allErrorCandidate());
        assertEquals(List.of(new ResourceErrorExtractor.ResourceError(Errors.FENCED_LEADER_EPOCH.code(), "topic-1")), view.errors());
    }

    /**
     * Given ListOffsets has a valid offset and another partition error, it is not all-error.
     */
    @Test
    public void testListOffsetsValidOffsetCountsAsValidResult() throws Exception {
        ListOffsetsRequest request = ListOffsetsRequest.Builder
            .forConsumer(true, org.apache.kafka.common.IsolationLevel.READ_UNCOMMITTED, false)
            .setTargetTimes(List.of(new ListOffsetsRequestData.ListOffsetsTopic()
                .setName("topic")
                .setPartitions(List.of(
                    new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setTimestamp(1000L),
                    new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(1).setTimestamp(1000L)
                ))))
            .build();
        ListOffsetsResponseData responseData = new ListOffsetsResponseData();
        responseData.topics().add(new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("topic").setPartitions(List.of(
            new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code())
                .setOffset(12L),
            new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                .setPartitionIndex(1)
                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        )));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new ListOffsetsResponse(responseData));

        assertFalse(view.allErrorCandidate());
        assertEquals(List.of(new ResourceErrorExtractor.ResourceError(Errors.NOT_LEADER_OR_FOLLOWER.code(), "topic-1")), view.errors());
    }

    /**
     * Given OffsetForLeaderEpoch has only partition errors, the view is an all-error candidate.
     */
    @Test
    public void testOffsetForLeaderEpochAllErrorsReturnsAllErrorCandidate() throws Exception {
        OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection topics =
            new OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection();
        topics.add(new OffsetForLeaderEpochRequestData.OffsetForLeaderTopic()
            .setTopic("topic")
            .setPartitions(List.of(new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition()
                .setPartition(0)
                .setLeaderEpoch(1))));
        OffsetsForLeaderEpochRequest request = OffsetsForLeaderEpochRequest.Builder.forConsumer(topics).build((short) 3);
        OffsetForLeaderEpochResponseData responseData = new OffsetForLeaderEpochResponseData();
        responseData.topics().add(new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult()
            .setTopic("topic")
            .setPartitions(List.of(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                .setPartition(0)
                .setErrorCode(Errors.FENCED_LEADER_EPOCH.code())
                .setEndOffset(OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET))));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new OffsetsForLeaderEpochResponse(responseData));

        assertTrue(view.allErrorCandidate());
        assertEquals(List.of(new ResourceErrorExtractor.ResourceError(Errors.FENCED_LEADER_EPOCH.code(), "topic-0")), view.errors());
    }

    /**
     * Given all-topics Metadata includes cluster metadata, broker/controller data makes errors non-all-error.
     */
    @Test
    public void testAllTopicsMetadataClusterDataCountsAsValidResult() throws Exception {
        MetadataRequest request = MetadataRequest.Builder.allTopics().build((short) 12);
        MetadataResponseData responseData = metadataWithLeaderUnavailable();
        responseData.brokers().add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(1)
            .setHost("localhost")
            .setPort(9092));
        responseData.setControllerId(1);

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new MetadataResponse(responseData, (short) 12));

        assertFalse(view.allErrorCandidate());
        assertTrue(resources(view).contains("topic"));
        assertTrue(resources(view).contains("topic-0"));
    }

    /**
     * Given explicit-topic Metadata has cluster metadata but requested topic errors, cluster data does not mask errors.
     */
    @Test
    public void testExplicitTopicMetadataClusterDataDoesNotMaskTopicErrors() throws Exception {
        MetadataRequest request = new MetadataRequest.Builder(List.of("topic"), true).build((short) 12);
        MetadataResponseData responseData = metadataWithLeaderUnavailable();
        responseData.brokers().add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(1)
            .setHost("localhost")
            .setPort(9092));
        responseData.setControllerId(1);

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new MetadataResponse(responseData, (short) 12));

        assertTrue(view.allErrorCandidate());
        assertEquals(Set.of("topic", "topic-0"), resources(view));
    }

    /**
     * Given Metadata topic-level NONE with partition errors, topic NONE does not count as a valid result.
     */
    @Test
    public void testMetadataTopicNoneDoesNotMaskPartitionErrors() throws Exception {
        MetadataRequest request = new MetadataRequest.Builder(List.of("topic"), true).build((short) 12);
        MetadataResponseData responseData = new MetadataResponseData();
        responseData.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.NONE.code())
            .setPartitions(List.of(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new MetadataResponse(responseData, (short) 12));

        assertTrue(view.allErrorCandidate());
        assertEquals(Set.of("topic-0"), resources(view));
    }

    /**
     * Given DescribeTopicPartitions topic-level NONE with partition errors, partition errors remain all-error.
     */
    @Test
    public void testDescribeTopicPartitionsTopicNoneDoesNotMaskPartitionErrors() throws Exception {
        DescribeTopicPartitionsRequest request = new DescribeTopicPartitionsRequest.Builder(List.of("topic")).build((short) 0);
        DescribeTopicPartitionsResponseData responseData = new DescribeTopicPartitionsResponseData();
        responseData.topics().add(new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.NONE.code())
            .setPartitions(List.of(new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new DescribeTopicPartitionsResponse(responseData));

        assertTrue(view.allErrorCandidate());
        assertEquals(Set.of("topic-0"), resources(view));
    }

    /**
     * Given batch FindCoordinator uses a transaction key, resource keys include the coordinator type prefix.
     */
    @Test
    public void testFindCoordinatorBatchResourceKeyIncludesCoordinatorType() throws Exception {
        FindCoordinatorRequest request = new FindCoordinatorRequest.Builder(
            new FindCoordinatorRequestData()
                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                .setCoordinatorKeys(List.of("shared-key"))
        ).build((short) 4);
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData();
        responseData.coordinators().add(new FindCoordinatorResponseData.Coordinator()
            .setKey("shared-key")
            .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            .setNodeId(-1));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new FindCoordinatorResponse(responseData));

        assertTrue(view.allErrorCandidate());
        assertEquals(List.of(new ResourceErrorExtractor.ResourceError(
            Errors.COORDINATOR_LOAD_IN_PROGRESS.code(),
            "transaction-shared-key"
        )), view.errors());
    }

    /**
     * Given OffsetFetch v8 batch has two group errors, extractor keeps separate per-group resource keys.
     */
    @Test
    public void testOffsetFetchBatchUsesPerGroupResourceKeys() throws Exception {
        OffsetFetchRequest request = new OffsetFetchRequest.Builder(Map.of(
            "group-a", List.of(new TopicPartition("topic", 0)),
            "group-b", List.of(new TopicPartition("topic", 1))
        ), false, false).build((short) 8);
        OffsetFetchResponseData responseData = new OffsetFetchResponseData();
        responseData.groups().add(offsetFetchGroup("group-a", Errors.COORDINATOR_LOAD_IN_PROGRESS));
        responseData.groups().add(offsetFetchGroup("group-b", Errors.NOT_COORDINATOR));

        ResourceErrorExtractor.ResourceErrorView view =
            ResourceErrorExtractor.extractView(request(request), new OffsetFetchResponse(responseData, (short) 8));

        assertTrue(view.allErrorCandidate());
        assertEquals(Set.of("group-group-a", "group-group-b"), resources(view));
    }

    private static RequestChannel.Request request(AbstractRequest request) throws Exception {
        ByteBuffer buffer = request.serializeWithHeader(new RequestHeader(request.apiKey(), request.version(), "client-id", 1));
        RequestHeader header = RequestHeader.parse(buffer);
        RequestContext context = new RequestContext(
            header,
            "connection-id",
            InetAddress.getLoopbackAddress(),
            Optional.empty(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false,
            Optional.empty()
        );
        return new RequestChannel.Request(
            1,
            context,
            0L,
            MemoryPool.NONE,
            buffer,
            mock(RequestChannel.Metrics.class),
            scala.Option.apply(null)
        );
    }

    private static MetadataResponseData metadataWithLeaderUnavailable() {
        MetadataResponseData responseData = new MetadataResponseData();
        responseData.topics().add(new MetadataResponseData.MetadataResponseTopic()
            .setName("topic")
            .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
            .setPartitions(List.of(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(-1))));
        return responseData;
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

    private static Set<String> resources(ResourceErrorExtractor.ResourceErrorView view) {
        return view.errors().stream()
            .map(ResourceErrorExtractor.ResourceError::resource)
            .collect(Collectors.toSet());
    }

    private static final class FetchRequestDataFixtures {
        private static Map<TopicPartition, FetchRequest.PartitionData> emptyFetchData() {
            return new HashMap<>();
        }
    }
}
