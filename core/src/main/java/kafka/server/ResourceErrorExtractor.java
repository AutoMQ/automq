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
package kafka.server;

import kafka.network.RequestChannel;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Extracts (errorCode, resource) pairs from request/response for recording in RequestErrorAccumulator.
 */
public class ResourceErrorExtractor {

    public record ResourceError(short errorCode, String resource) { }

    /**
     * Extract recordable errors with their associated resource from a response.
     */
    public static List<ResourceError> extract(RequestChannel.Request request, AbstractResponse response) {
        try {
            return doExtract(request, response);
        } catch (Exception e) {
            // Never let extraction failures break request processing
            return Collections.emptyList();
        }
    }

    /**
     * Best-effort resource extraction from request only (for closeConnection path).
     */
    public static String extractResourceFromRequest(RequestChannel.Request request) {
        try {
            return doExtractResourceFromRequest(request);
        } catch (Exception e) {
            return "";
        }
    }

    private static List<ResourceError> doExtract(RequestChannel.Request request, AbstractResponse response) {
        ApiKeys apiKey = request.header().apiKey();
        switch (apiKey) {
            case PRODUCE:
                return extractProduce((ProduceResponse) response);
            case FETCH:
                return extractFetch((FetchResponse) response);
            case LIST_OFFSETS:
                return extractListOffsets((ListOffsetsResponse) response);
            case METADATA:
                return extractMetadata((MetadataResponse) response);
            case OFFSET_COMMIT:
                return extractOffsetCommit(request, (OffsetCommitResponse) response);
            case OFFSET_FETCH:
                return extractOffsetFetch((OffsetFetchResponse) response);
            case FIND_COORDINATOR:
                return extractFindCoordinator(request, (FindCoordinatorResponse) response);
            case CREATE_TOPICS:
                return extractCreateTopics((CreateTopicsResponse) response);
            case DELETE_TOPICS:
                return extractDeleteTopics((DeleteTopicsResponse) response);
            case CREATE_PARTITIONS:
                return extractCreatePartitions((CreatePartitionsResponse) response);
            default:
                return doExtractOther(request, response, apiKey);
        }
    }

    private static List<ResourceError> doExtractOther(RequestChannel.Request request, AbstractResponse response, ApiKeys apiKey) {
        switch (apiKey) {
            case JOIN_GROUP:
                return extractSingleErrorFromRequest(response, () -> request.body(JoinGroupRequest.class).data().groupId());
            case SYNC_GROUP:
                return extractSingleErrorFromRequest(response, () -> request.body(SyncGroupRequest.class).data().groupId());
            case HEARTBEAT:
                return extractSingleErrorFromRequest(response, () -> request.body(HeartbeatRequest.class).data().groupId());
            case LEAVE_GROUP:
                return extractSingleErrorFromRequest(response, () -> request.body(LeaveGroupRequest.class).data().groupId());
            case DESCRIBE_GROUPS:
                return extractDescribeGroups((DescribeGroupsResponse) response);
            case DELETE_GROUPS:
                return extractDeleteGroups((DeleteGroupsResponse) response);
            case LIST_GROUPS:
                return extractSingleError(response, "cluster");
            case INIT_PRODUCER_ID:
                return extractSingleErrorFromRequest(response, () -> {
                    String txnId = request.body(InitProducerIdRequest.class).data().transactionalId();
                    return txnId != null ? txnId : "";
                });
            case ADD_PARTITIONS_TO_TXN:
                return extractAddPartitionsToTxn((AddPartitionsToTxnResponse) response);
            case ADD_OFFSETS_TO_TXN:
                return extractSingleErrorFromRequest(response, () -> request.body(AddOffsetsToTxnRequest.class).data().transactionalId());
            case END_TXN:
                return extractSingleErrorFromRequest(response, () -> request.body(EndTxnRequest.class).data().transactionalId());
            case TXN_OFFSET_COMMIT:
                return extractTxnOffsetCommit((TxnOffsetCommitResponse) response);
            default:
                return doExtractAdmin(request, response, apiKey);
        }
    }

    private static List<ResourceError> doExtractAdmin(RequestChannel.Request request, AbstractResponse response, ApiKeys apiKey) {
        switch (apiKey) {
            case ALTER_CONFIGS:
                return extractAlterConfigs((AlterConfigsResponse) response);
            case DESCRIBE_CONFIGS:
                return extractDescribeConfigs((DescribeConfigsResponse) response);
            case INCREMENTAL_ALTER_CONFIGS:
                return extractIncrementalAlterConfigs((IncrementalAlterConfigsResponse) response);
            case CREATE_DELEGATION_TOKEN:
            case RENEW_DELEGATION_TOKEN:
            case EXPIRE_DELEGATION_TOKEN:
                return extractSingleError(response, "delegation-token");
            case SASL_HANDSHAKE:
            case SASL_AUTHENTICATE:
                return extractSingleError(response, "sasl");
            case CREATE_ACLS:
            case DELETE_ACLS:
            case DESCRIBE_ACLS:
                return extractSingleError(response, "cluster");
            case CONSUMER_GROUP_HEARTBEAT:
                return extractSingleErrorFromRequest(response, () -> request.body(ConsumerGroupHeartbeatRequest.class).data().groupId());
            case CONSUMER_GROUP_DESCRIBE:
                return extractSingleErrorFromRequest(response, () -> {
                    List<String> ids = request.body(ConsumerGroupDescribeRequest.class).data().groupIds();
                    return ids.isEmpty() ? "" : ids.get(0);
                });
            default:
                return extractDefaultErrors(response);
        }
    }

    // ---- Per-topic response extractors ----

    private static List<ResourceError> extractProduce(ProduceResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().responses().forEach(topic ->
            topic.partitionResponses().forEach(p ->
                maybeAdd(result, p.errorCode(), topic.name())));
        return result;
    }

    private static List<ResourceError> extractFetch(FetchResponse response) {
        List<ResourceError> result = new ArrayList<>();
        // top-level error
        maybeAdd(result, response.error().code(), "");
        response.data().responses().forEach(topic ->
            topic.partitions().forEach(p ->
                maybeAdd(result, p.errorCode(), topic.topic())));
        return result;
    }

    private static List<ResourceError> extractListOffsets(ListOffsetsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().topics().forEach(topic ->
            topic.partitions().forEach(p ->
                maybeAdd(result, p.errorCode(), topic.name())));
        return result;
    }

    private static List<ResourceError> extractMetadata(MetadataResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().topics().forEach(topic ->
            maybeAdd(result, topic.errorCode(), topic.name() != null ? topic.name() : ""));
        return result;
    }

    private static List<ResourceError> extractOffsetCommit(RequestChannel.Request request, OffsetCommitResponse response) {
        List<ResourceError> result = new ArrayList<>();
        String groupId = request.body(OffsetCommitRequest.class).data().groupId();
        response.data().topics().forEach(topic ->
            topic.partitions().forEach(p -> {
                short code = p.errorCode();
                if (code == Errors.GROUP_AUTHORIZATION_FAILED.code()) {
                    maybeAdd(result, code, groupId);
                } else {
                    maybeAdd(result, code, topic.name());
                }
            }));
        return result;
    }

    private static List<ResourceError> extractOffsetFetch(OffsetFetchResponse response) {
        List<ResourceError> result = new ArrayList<>();
        // v8+ batched groups
        response.data().groups().forEach(group ->
            maybeAdd(result, group.errorCode(), group.groupId()));
        // older single-group
        if (response.data().groups().isEmpty()) {
            maybeAdd(result, response.data().errorCode(), "");
        }
        return result;
    }

    private static List<ResourceError> extractFindCoordinator(RequestChannel.Request request, FindCoordinatorResponse response) {
        List<ResourceError> result = new ArrayList<>();
        if (!response.data().coordinators().isEmpty()) {
            response.data().coordinators().forEach(c ->
                maybeAdd(result, c.errorCode(), c.key()));
        } else {
            String key = request.body(FindCoordinatorRequest.class).data().key();
            maybeAdd(result, response.data().errorCode(), key != null ? key : "");
        }
        return result;
    }

    private static List<ResourceError> extractCreateTopics(CreateTopicsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().topics().forEach(topic ->
            maybeAdd(result, topic.errorCode(), topic.name()));
        return result;
    }

    private static List<ResourceError> extractDeleteTopics(DeleteTopicsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().responses().forEach(topic ->
            maybeAdd(result, topic.errorCode(), topic.name() != null ? topic.name() : ""));
        return result;
    }

    private static List<ResourceError> extractCreatePartitions(CreatePartitionsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().results().forEach(r ->
            maybeAdd(result, r.errorCode(), r.name()));
        return result;
    }

    // ---- Per-group response extractors ----

    private static List<ResourceError> extractDescribeGroups(DescribeGroupsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().groups().forEach(g ->
            maybeAdd(result, g.errorCode(), g.groupId()));
        return result;
    }

    private static List<ResourceError> extractDeleteGroups(DeleteGroupsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().results().forEach(r ->
            maybeAdd(result, r.errorCode(), r.groupId()));
        return result;
    }

    // ---- Transaction extractors ----

    private static List<ResourceError> extractAddPartitionsToTxn(AddPartitionsToTxnResponse response) {
        List<ResourceError> result = new ArrayList<>();
        // v3 and below: per-topic results
        response.data().resultsByTopicV3AndBelow().forEach(topic ->
            topic.resultsByPartition().forEach(p ->
                maybeAdd(result, p.partitionErrorCode(), topic.name())));
        // v4+: per-transaction, per-topic results
        response.data().resultsByTransaction().forEach(txn ->
            txn.topicResults().forEach(topic ->
                topic.resultsByPartition().forEach(p ->
                    maybeAdd(result, p.partitionErrorCode(), topic.name()))));
        return result;
    }

    private static List<ResourceError> extractTxnOffsetCommit(TxnOffsetCommitResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().topics().forEach(topic ->
            topic.partitions().forEach(p ->
                maybeAdd(result, p.errorCode(), topic.name())));
        return result;
    }

    // ---- Config extractors ----

    private static List<ResourceError> extractAlterConfigs(AlterConfigsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().responses().forEach(r ->
            maybeAdd(result, r.errorCode(), r.resourceName()));
        return result;
    }

    private static List<ResourceError> extractDescribeConfigs(DescribeConfigsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().results().forEach(r ->
            maybeAdd(result, r.errorCode(), r.resourceName()));
        return result;
    }

    private static List<ResourceError> extractIncrementalAlterConfigs(IncrementalAlterConfigsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.data().responses().forEach(r ->
            maybeAdd(result, r.errorCode(), r.resourceName()));
        return result;
    }

    // ---- Helpers ----

    private static List<ResourceError> extractSingleError(AbstractResponse response, String resource) {
        List<ResourceError> result = new ArrayList<>();
        response.errorCounts().forEach((error, count) ->
            maybeAdd(result, error.code(), resource));
        return result;
    }

    @FunctionalInterface
    private interface ResourceSupplier {
        String get();
    }

    private static List<ResourceError> extractSingleErrorFromRequest(
            AbstractResponse response, ResourceSupplier resourceSupplier) {
        List<ResourceError> result = new ArrayList<>();
        String resource = resourceSupplier.get();
        response.errorCounts().forEach((error, count) ->
            maybeAdd(result, error.code(), resource));
        return result;
    }

    private static List<ResourceError> extractDefaultErrors(AbstractResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.errorCounts().forEach((error, count) ->
            maybeAdd(result, error.code(), ""));
        return result;
    }

    private static void maybeAdd(List<ResourceError> result, short errorCode, String resource) {
        if (errorCode != 0) {
            result.add(new ResourceError(errorCode, resource));
        }
    }

    private static String doExtractResourceFromRequest(RequestChannel.Request request) {
        ApiKeys apiKey = request.header().apiKey();
        switch (apiKey) {
            case PRODUCE:
                var produceTopics = request.body(org.apache.kafka.common.requests.ProduceRequest.class).data().topicData();
                return produceTopics.isEmpty() ? "" : produceTopics.iterator().next().name();
            case JOIN_GROUP:
                return request.body(JoinGroupRequest.class).data().groupId();
            case SYNC_GROUP:
                return request.body(SyncGroupRequest.class).data().groupId();
            case HEARTBEAT:
                return request.body(HeartbeatRequest.class).data().groupId();
            case LEAVE_GROUP:
                return request.body(LeaveGroupRequest.class).data().groupId();
            case OFFSET_COMMIT:
                return request.body(OffsetCommitRequest.class).data().groupId();
            case INIT_PRODUCER_ID: {
                String txnId = request.body(InitProducerIdRequest.class).data().transactionalId();
                return txnId != null ? txnId : "";
            }
            case ADD_OFFSETS_TO_TXN:
                return request.body(AddOffsetsToTxnRequest.class).data().transactionalId();
            case END_TXN:
                return request.body(EndTxnRequest.class).data().transactionalId();
            case FIND_COORDINATOR: {
                String key = request.body(FindCoordinatorRequest.class).data().key();
                return key != null ? key : "";
            }
            case CONSUMER_GROUP_HEARTBEAT:
                return request.body(ConsumerGroupHeartbeatRequest.class).data().groupId();
            default:
                return "";
        }
    }
}
