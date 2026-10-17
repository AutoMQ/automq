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
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Extracts resource-scoped response errors for request error accounting and retry storm backoff.
 *
 * <p>The extractor owns schema-specific parsing and valid-result detection. It returns response
 * facts only and does not decide whether an error is retry storm delayable, protective, or eligible
 * for any policy action.</p>
 */
public class ResourceErrorExtractor {

    /**
     * Resource-scoped non-NONE response error.
     *
     * @param errorCode Kafka protocol error code from the final response
     * @param resource partition, topic, coordinator, transaction, or cluster resource key
     */
    public record ResourceError(short errorCode, String resource) { }

    /**
     * Shared resource error view for request error accumulation and retry storm backoff.
     *
     * <p>The view contains response facts only. It does not classify errors as retry storm
     * delayable-transient or protective; policy code owns those decisions.</p>
     */
    public record ResourceErrorView(boolean allErrorCandidate, List<ResourceError> errors) {
        private static final ResourceErrorView EMPTY = new ResourceErrorView(false, List.of());

        /**
         * Returns an empty view for responses without resource-level errors.
         */
        public static ResourceErrorView empty() {
            return EMPTY;
        }
    }

    /**
     * Extract recordable errors with their associated resource from a response.
     */
    public static List<ResourceError> extract(RequestChannel.Request request, AbstractResponse response) {
        return extractView(request, response).errors();
    }

    /**
     * Extract resource-level errors and whether the response has no valid result.
     */
    public static ResourceErrorView extractView(RequestChannel.Request request, AbstractResponse response) {
        try {
            if (!hasNonNoneError(response)) {
                return ResourceErrorView.empty();
            }
            return doExtractView(request, response);
        } catch (Exception e) {
            // Never let extraction failures break request processing
            return ResourceErrorView.empty();
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

    private static ResourceErrorView doExtractView(RequestChannel.Request request, AbstractResponse response) {
        ApiKeys apiKey = request.header().apiKey();
        switch (apiKey) {
            case PRODUCE:
                return extractProduce((ProduceResponse) response);
            case FETCH:
                return extractFetch((FetchResponse) response);
            case LIST_OFFSETS:
                return extractListOffsets((ListOffsetsResponse) response);
            case OFFSET_FOR_LEADER_EPOCH:
                return extractOffsetForLeaderEpoch((OffsetsForLeaderEpochResponse) response);
            case METADATA:
                return extractMetadata(request, (MetadataResponse) response);
            case DESCRIBE_TOPIC_PARTITIONS:
                return extractDescribeTopicPartitions((org.apache.kafka.common.requests.DescribeTopicPartitionsResponse) response);
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

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static ResourceErrorView doExtractOther(RequestChannel.Request request, AbstractResponse response, ApiKeys apiKey) {
        switch (apiKey) {
            case JOIN_GROUP:
                return extractSingleErrorFromRequest(response, () -> groupKey(request.body(JoinGroupRequest.class).data().groupId()));
            case SYNC_GROUP:
                return extractSingleErrorFromRequest(response, () -> groupKey(request.body(SyncGroupRequest.class).data().groupId()));
            case HEARTBEAT:
                return extractSingleErrorFromRequest(response, () -> request.body(HeartbeatRequest.class).data().groupId());
            case LEAVE_GROUP:
                return extractSingleErrorFromRequest(response, () -> groupKey(request.body(LeaveGroupRequest.class).data().groupId()));
            case DESCRIBE_GROUPS:
                return extractDescribeGroups((DescribeGroupsResponse) response);
            case DELETE_GROUPS:
                return extractDeleteGroups((DeleteGroupsResponse) response);
            case LIST_GROUPS:
                return extractSingleError(response, "cluster");
            case INIT_PRODUCER_ID:
                return extractSingleErrorFromRequest(response, () -> {
                    String txnId = request.body(InitProducerIdRequest.class).data().transactionalId();
                    return txnId != null && !txnId.isEmpty()
                        ? transactionKey(txnId)
                        : "producer-" + request.body(InitProducerIdRequest.class).data().producerId();
                });
            case ADD_PARTITIONS_TO_TXN:
                return extractAddPartitionsToTxn((AddPartitionsToTxnResponse) response);
            case ADD_OFFSETS_TO_TXN:
                return extractSingleErrorFromRequest(response, () -> transactionKey(request.body(AddOffsetsToTxnRequest.class).data().transactionalId()));
            case END_TXN:
                return extractSingleErrorFromRequest(response, () -> {
                    String txnId = request.body(EndTxnRequest.class).data().transactionalId();
                    return txnId != null && !txnId.isEmpty()
                        ? transactionKey(txnId)
                        : "producer-" + request.body(EndTxnRequest.class).data().producerId();
                });
            case TXN_OFFSET_COMMIT:
                return extractTxnOffsetCommit(request, (TxnOffsetCommitResponse) response);
            default:
                return doExtractAdmin(request, response, apiKey);
        }
    }

    private static ResourceErrorView doExtractAdmin(RequestChannel.Request request, AbstractResponse response, ApiKeys apiKey) {
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

    private static ResourceErrorView extractProduce(ProduceResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().responses().forEach(topic ->
            topic.partitionResponses().forEach(p -> {
                if (p.errorCode() == Errors.NONE.code()) {
                    hasValid[0] = true;
                }
                maybeAdd(result, p.errorCode(), topic.name() + "-" + p.index());
            }));
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractFetch(FetchResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        // top-level error
        maybeAdd(result, response.error().code(), "");
        response.data().responses().forEach(topic ->
            topic.partitions().forEach(p -> {
                if (p.errorCode() == Errors.NONE.code()) {
                    hasValid[0] = true;
                }
                maybeAdd(result, p.errorCode(), topic.topic() + "-" + p.partitionIndex());
            }));
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractListOffsets(ListOffsetsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().topics().forEach(topic ->
            topic.partitions().forEach(p -> {
                if (p.errorCode() == Errors.NONE.code()
                    && (p.offset() != ListOffsetsResponse.UNKNOWN_OFFSET || !p.oldStyleOffsets().isEmpty())) {
                    hasValid[0] = true;
                }
                maybeAdd(result, p.errorCode(), topic.name() + "-" + p.partitionIndex());
            }));
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractOffsetForLeaderEpoch(OffsetsForLeaderEpochResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().topics().forEach(topic ->
            topic.partitions().forEach(partition -> {
                if (partition.errorCode() == Errors.NONE.code()
                    && partition.endOffset() != OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET) {
                    hasValid[0] = true;
                }
                maybeAdd(result, partition.errorCode(), topic.topic() + "-" + partition.partition());
            }));
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractMetadata(RequestChannel.Request request, MetadataResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {includeClusterValidity(request)
            && (!response.data().brokers().isEmpty() || response.data().controllerId() >= 0)};
        response.data().topics().forEach(topic -> {
            String topicName = topic.name() != null ? topic.name() : topic.topicId().toString();
            boolean[] topicHasValidPartition = {false};
            topic.partitions().forEach(partition -> {
                if (partition.errorCode() == Errors.NONE.code() && partition.leaderId() >= 0) {
                    topicHasValidPartition[0] = true;
                    hasValid[0] = true;
                }
                maybeAdd(result, partition.errorCode(), topicName + "-" + partition.partitionIndex());
            });
            if (topic.errorCode() == Errors.NONE.code() && topicHasValidPartition[0]) {
                hasValid[0] = true;
            }
            maybeAdd(result, topic.errorCode(), topicName);
        });
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractDescribeTopicPartitions(org.apache.kafka.common.requests.DescribeTopicPartitionsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().topics().forEach(topic -> {
            String topicName = topic.name() != null ? topic.name() : "";
            boolean[] topicHasValidPartition = {false};
            topic.partitions().forEach(partition -> {
                if (partition.errorCode() == Errors.NONE.code() && partition.leaderId() >= 0) {
                    topicHasValidPartition[0] = true;
                    hasValid[0] = true;
                }
                maybeAdd(result, partition.errorCode(), topicName + "-" + partition.partitionIndex());
            });
            if (topic.errorCode() == Errors.NONE.code() && topicHasValidPartition[0]) {
                hasValid[0] = true;
            }
            maybeAdd(result, topic.errorCode(), topicName);
        });
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractOffsetCommit(RequestChannel.Request request, OffsetCommitResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        String resource = groupKey(request.body(OffsetCommitRequest.class).data().groupId());
        response.data().topics().forEach(topic ->
            topic.partitions().forEach(p -> {
                short code = p.errorCode();
                if (code == Errors.NONE.code()) {
                    hasValid[0] = true;
                }
                maybeAdd(result, code, resource);
            }));
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractOffsetFetch(OffsetFetchResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        // v8+ batched groups
        response.data().groups().forEach(group -> {
            if (group.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, group.errorCode(), groupKey(group.groupId()));
        });
        // older single-group
        if (response.data().groups().isEmpty()) {
            if (response.data().errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, response.data().errorCode(), "");
        }
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractFindCoordinator(RequestChannel.Request request, FindCoordinatorResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        if (!response.data().coordinators().isEmpty()) {
            response.data().coordinators().forEach(c -> {
                if (c.errorCode() == Errors.NONE.code() && c.nodeId() >= 0) {
                    hasValid[0] = true;
                }
                maybeAdd(result, c.errorCode(), coordinatorType(request) + "-" + c.key());
            });
        } else {
            String key = request.body(FindCoordinatorRequest.class).data().key();
            if (response.data().errorCode() == Errors.NONE.code() && response.data().nodeId() >= 0) {
                hasValid[0] = true;
            }
            maybeAdd(result, response.data().errorCode(), coordinatorType(request) + "-" + (key != null ? key : ""));
        }
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractCreateTopics(CreateTopicsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().topics().forEach(topic -> {
            if (topic.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, topic.errorCode(), topic.name());
        });
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractDeleteTopics(DeleteTopicsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().responses().forEach(topic -> {
            if (topic.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, topic.errorCode(), topic.name() != null ? topic.name() : "");
        });
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractCreatePartitions(CreatePartitionsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().results().forEach(r -> {
            if (r.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, r.errorCode(), r.name());
        });
        return view(result, hasValid[0]);
    }

    // ---- Per-group response extractors ----

    private static ResourceErrorView extractDescribeGroups(DescribeGroupsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().groups().forEach(g -> {
            if (g.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, g.errorCode(), g.groupId());
        });
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractDeleteGroups(DeleteGroupsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().results().forEach(r -> {
            if (r.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, r.errorCode(), r.groupId());
        });
        return view(result, hasValid[0]);
    }

    // ---- Transaction extractors ----

    private static ResourceErrorView extractAddPartitionsToTxn(AddPartitionsToTxnResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        // v3 and below: per-topic results
        response.data().resultsByTopicV3AndBelow().forEach(topic ->
            topic.resultsByPartition().forEach(p -> {
                if (p.partitionErrorCode() == Errors.NONE.code()) {
                    hasValid[0] = true;
                }
                maybeAdd(result, p.partitionErrorCode(), topic.name() + "-" + p.partitionIndex());
            }));
        // v4+: per-transaction, per-topic results
        response.data().resultsByTransaction().forEach(txn ->
            txn.topicResults().forEach(topic ->
                topic.resultsByPartition().forEach(p -> {
                    if (p.partitionErrorCode() == Errors.NONE.code()) {
                        hasValid[0] = true;
                    }
                    maybeAdd(result, p.partitionErrorCode(), topic.name() + "-" + p.partitionIndex());
                })));
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractTxnOffsetCommit(RequestChannel.Request request, TxnOffsetCommitResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        String resource = transactionKey(request.body(org.apache.kafka.common.requests.TxnOffsetCommitRequest.class).data().transactionalId());
        response.data().topics().forEach(topic ->
            topic.partitions().forEach(p -> {
                if (p.errorCode() == Errors.NONE.code()) {
                    hasValid[0] = true;
                }
                maybeAdd(result, p.errorCode(), resource);
            }));
        return view(result, hasValid[0]);
    }

    // ---- Config extractors ----

    private static ResourceErrorView extractAlterConfigs(AlterConfigsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().responses().forEach(r -> {
            if (r.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, r.errorCode(), r.resourceName());
        });
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractDescribeConfigs(DescribeConfigsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().results().forEach(r -> {
            if (r.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, r.errorCode(), r.resourceName());
        });
        return view(result, hasValid[0]);
    }

    private static ResourceErrorView extractIncrementalAlterConfigs(IncrementalAlterConfigsResponse response) {
        List<ResourceError> result = new ArrayList<>();
        boolean[] hasValid = {false};
        response.data().responses().forEach(r -> {
            if (r.errorCode() == Errors.NONE.code()) {
                hasValid[0] = true;
            }
            maybeAdd(result, r.errorCode(), r.resourceName());
        });
        return view(result, hasValid[0]);
    }

    // ---- Helpers ----

    private static ResourceErrorView extractSingleError(AbstractResponse response, String resource) {
        List<ResourceError> result = new ArrayList<>();
        response.errorCounts().forEach((error, count) ->
            maybeAdd(result, error.code(), resource));
        return view(result, hasNoneError(response));
    }

    @FunctionalInterface
    private interface ResourceSupplier {
        String get();
    }

    private static ResourceErrorView extractSingleErrorFromRequest(
            AbstractResponse response, ResourceSupplier resourceSupplier) {
        List<ResourceError> result = new ArrayList<>();
        String resource = resourceSupplier.get();
        response.errorCounts().forEach((error, count) ->
            maybeAdd(result, error.code(), resource));
        return view(result, hasNoneError(response));
    }

    private static ResourceErrorView extractDefaultErrors(AbstractResponse response) {
        List<ResourceError> result = new ArrayList<>();
        response.errorCounts().forEach((error, count) ->
            maybeAdd(result, error.code(), ""));
        return view(result, hasNoneError(response));
    }

    private static void maybeAdd(List<ResourceError> result, short errorCode, String resource) {
        if (errorCode != 0) {
            result.add(new ResourceError(errorCode, resource));
        }
    }

    private static ResourceErrorView view(List<ResourceError> errors, boolean hasValidResult) {
        return new ResourceErrorView(!errors.isEmpty() && !hasValidResult, List.copyOf(errors));
    }

    private static boolean hasNonNoneError(AbstractResponse response) {
        return response.errorCounts().keySet().stream().anyMatch(error -> error != Errors.NONE);
    }

    private static boolean hasNoneError(AbstractResponse response) {
        return response.errorCounts().containsKey(Errors.NONE);
    }

    private static boolean includeClusterValidity(RequestChannel.Request request) {
        org.apache.kafka.common.requests.MetadataRequest metadataRequest =
            request.body(org.apache.kafka.common.requests.MetadataRequest.class);
        return metadataRequest.isAllTopics() || metadataRequest.data().topics().isEmpty();
    }

    private static String coordinatorType(RequestChannel.Request request) {
        FindCoordinatorRequest body = request.body(FindCoordinatorRequest.class);
        return FindCoordinatorRequest.CoordinatorType.forId(body.data().keyType()).name().toLowerCase(Locale.ROOT);
    }

    private static String groupKey(String groupId) {
        return "group-" + (groupId == null ? "" : groupId);
    }

    private static String transactionKey(String transactionalId) {
        return "transaction-" + (transactionalId == null ? "" : transactionalId);
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
