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

import kafka.network.RequestChannel;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Registry and shared error classifiers for retry storm response summary extraction.
 */
public final class RetryStormResponseSummaryExtractors {
    public static final Set<Errors> DELAYABLE_LEADER_ERRORS = EnumSet.of(
        Errors.NOT_LEADER_OR_FOLLOWER,
        Errors.LEADER_NOT_AVAILABLE,
        Errors.FENCED_LEADER_EPOCH
    );

    public static final Set<Errors> DELAYABLE_COORDINATOR_ERRORS = EnumSet.of(
        Errors.COORDINATOR_LOAD_IN_PROGRESS,
        Errors.COORDINATOR_NOT_AVAILABLE,
        Errors.NOT_COORDINATOR
    );

    public static final RetryStormResponseSummaryExtractor PRODUCE =
        RetryStormResponseSummaryExtractors::produceSummary;
    public static final RetryStormResponseSummaryExtractor FETCH =
        RetryStormResponseSummaryExtractors::fetchSummary;
    public static final RetryStormResponseSummaryExtractor LIST_OFFSETS =
        RetryStormResponseSummaryExtractors::listOffsetsSummary;
    public static final RetryStormResponseSummaryExtractor OFFSET_FOR_LEADER_EPOCH =
        RetryStormResponseSummaryExtractors::offsetForLeaderEpochSummary;
    public static final RetryStormResponseSummaryExtractor METADATA =
        RetryStormResponseSummaryExtractors::metadataSummary;
    public static final RetryStormResponseSummaryExtractor DESCRIBE_TOPIC_PARTITIONS =
        RetryStormResponseSummaryExtractors::describeTopicPartitionsSummary;
    public static final RetryStormResponseSummaryExtractor FIND_COORDINATOR =
        RetryStormResponseSummaryExtractors::findCoordinatorSummary;
    public static final RetryStormResponseSummaryExtractor COORDINATOR =
        RetryStormResponseSummaryExtractors::coordinatorSummary;

    public static final Map<ApiKeys, RetryStormResponseSummaryExtractor> DEFAULT_REGISTRY = defaultRegistry();

    private RetryStormResponseSummaryExtractors() {
    }

    /**
     * Converts an error code into resource validity, delayable-transient, and protective classifications.
     */
    public static ResourceResult classify(String resourceKey, Errors error, Set<Errors> delayableErrors) {
        return new ResourceResult(
            resourceKey,
            error == Errors.NONE,
            delayableErrors.contains(error),
            error != Errors.NONE
        );
    }

    private static Map<ApiKeys, RetryStormResponseSummaryExtractor> defaultRegistry() {
        EnumMap<ApiKeys, RetryStormResponseSummaryExtractor> registry = new EnumMap<>(ApiKeys.class);
        registry.put(ApiKeys.PRODUCE, PRODUCE);
        registry.put(ApiKeys.FETCH, FETCH);
        registry.put(ApiKeys.LIST_OFFSETS, LIST_OFFSETS);
        registry.put(ApiKeys.OFFSET_FOR_LEADER_EPOCH, OFFSET_FOR_LEADER_EPOCH);
        registry.put(ApiKeys.METADATA, METADATA);
        registry.put(ApiKeys.DESCRIBE_TOPIC_PARTITIONS, DESCRIBE_TOPIC_PARTITIONS);
        registry.put(ApiKeys.FIND_COORDINATOR, FIND_COORDINATOR);
        registry.put(ApiKeys.JOIN_GROUP, COORDINATOR);
        registry.put(ApiKeys.SYNC_GROUP, COORDINATOR);
        registry.put(ApiKeys.OFFSET_COMMIT, COORDINATOR);
        registry.put(ApiKeys.OFFSET_FETCH, COORDINATOR);
        registry.put(ApiKeys.LEAVE_GROUP, COORDINATOR);
        registry.put(ApiKeys.TXN_OFFSET_COMMIT, COORDINATOR);
        registry.put(ApiKeys.ADD_OFFSETS_TO_TXN, COORDINATOR);
        registry.put(ApiKeys.INIT_PRODUCER_ID, COORDINATOR);
        registry.put(ApiKeys.END_TXN, COORDINATOR);
        return Map.copyOf(registry);
    }

    /**
     * Extracts per-partition Produce results for retry storm policy evaluation.
     */
    public static ResponseSummary produceSummary(Object request, Object response) {
        ProduceResponse produceResponse = (ProduceResponse) response;
        List<ResourceResult> resources = new ArrayList<>();
        produceResponse.data().responses().forEach(topic ->
            topic.partitionResponses().forEach(partition ->
                resources.add(classify(
                    topic.name() + "-" + partition.index(),
                    Errors.forCode(partition.errorCode()),
                    DELAYABLE_LEADER_ERRORS
                ))
            )
        );
        return new ResponseSummary(resources);
    }

    /**
     * Extracts Fetch partition results and caps delay by the remaining Fetch max-wait budget.
     */
    public static ResponseSummary fetchSummary(Object request, Object response) {
        FetchResponse fetchResponse = (FetchResponse) response;
        List<ResourceResult> resources = new ArrayList<>();
        fetchResponse.data().responses().forEach(topic -> {
            String topicKey = topic.topic() != null && !topic.topic().isEmpty() ? topic.topic() : topic.topicId().toString();
            topic.partitions().forEach(partition -> {
                Errors error = Errors.forCode(partition.errorCode());
                Object records = partition.records();
                boolean hasRecords = records instanceof Records && ((Records) records).sizeInBytes() > 0;
                resources.add(new ResourceResult(
                    topicKey + "-" + partition.partitionIndex(),
                    error == Errors.NONE || hasRecords,
                    DELAYABLE_LEADER_ERRORS.contains(error),
                    error != Errors.NONE
                ));
            });
        });
        return new ResponseSummary(resources, fetchDelayCapMs(request));
    }

    /**
     * Extracts ListOffsets partition results, including old protocol offset-list successes.
     */
    public static ResponseSummary listOffsetsSummary(Object request, Object response) {
        ListOffsetsResponse listOffsetsResponse = (ListOffsetsResponse) response;
        List<ResourceResult> resources = new ArrayList<>();
        listOffsetsResponse.data().topics().forEach(topic ->
            topic.partitions().forEach(partition -> {
                Errors error = Errors.forCode(partition.errorCode());
                resources.add(new ResourceResult(
                    topic.name() + "-" + partition.partitionIndex(),
                    error == Errors.NONE && (
                        partition.offset() != ListOffsetsResponse.UNKNOWN_OFFSET ||
                            !partition.oldStyleOffsets().isEmpty()
                    ),
                    DELAYABLE_LEADER_ERRORS.contains(error),
                    error != Errors.NONE
                ));
            })
        );
        return new ResponseSummary(resources);
    }

    /**
     * Extracts OffsetForLeaderEpoch partition results for leader/epoch transient backoff.
     */
    public static ResponseSummary offsetForLeaderEpochSummary(Object request, Object response) {
        OffsetsForLeaderEpochResponse leaderEpochResponse = (OffsetsForLeaderEpochResponse) response;
        List<ResourceResult> resources = new ArrayList<>();
        leaderEpochResponse.data().topics().forEach(topic ->
            topic.partitions().forEach(partition -> {
                Errors error = Errors.forCode(partition.errorCode());
                resources.add(new ResourceResult(
                    topic.topic() + "-" + partition.partition(),
                    error == Errors.NONE && partition.endOffset() != OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET,
                    DELAYABLE_LEADER_ERRORS.contains(error),
                    error != Errors.NONE
                ));
            })
        );
        return new ResponseSummary(resources);
    }

    /**
     * Extracts Metadata topic and partition results while preserving explicit-topic error visibility.
     */
    public static ResponseSummary metadataSummary(Object request, Object response) {
        MetadataResponse metadataResponse = (MetadataResponse) response;
        boolean hasClusterMetadata = !metadataResponse.data().brokers().isEmpty() ||
            metadataResponse.data().controllerId() >= 0;
        List<ResourceResult> resources = new ArrayList<>();
        metadataResponse.data().topics().forEach(topic -> {
            String topicName = topic.name() != null ? topic.name() : topic.topicId().toString();
            List<ResourceResult> partitionResults = new ArrayList<>();
            topic.partitions().forEach(partition -> {
                Errors error = Errors.forCode(partition.errorCode());
                partitionResults.add(new ResourceResult(
                    topicName + "-" + partition.partitionIndex(),
                    error == Errors.NONE && partition.leaderId() >= 0,
                    DELAYABLE_LEADER_ERRORS.contains(error),
                    error != Errors.NONE
                ));
            });
            Errors topicError = Errors.forCode(topic.errorCode());
            resources.add(new ResourceResult(
                topicName,
                topicError == Errors.NONE && partitionResults.stream().anyMatch(ResourceResult::valid),
                DELAYABLE_LEADER_ERRORS.contains(topicError),
                topicError != Errors.NONE
            ));
            resources.addAll(partitionResults);
        });
        if (includeClusterValidity(request)) {
            resources.add(0, new ResourceResult("cluster", hasClusterMetadata, false, false));
        }
        return new ResponseSummary(resources);
    }

    /**
     * Extracts DescribeTopicPartitions topic and partition metadata results.
     */
    public static ResponseSummary describeTopicPartitionsSummary(Object request, Object response) {
        DescribeTopicPartitionsResponse describeResponse = (DescribeTopicPartitionsResponse) response;
        List<ResourceResult> resources = new ArrayList<>();
        describeResponse.data().topics().forEach(topic -> {
            String topicName = topic.name() != null ? topic.name() : Uuid.ZERO_UUID.toString();
            Errors topicError = Errors.forCode(topic.errorCode());
            List<ResourceResult> partitionResults = new ArrayList<>();
            topic.partitions().forEach(partition -> {
                Errors error = Errors.forCode(partition.errorCode());
                partitionResults.add(new ResourceResult(
                    topicName + "-" + partition.partitionIndex(),
                    error == Errors.NONE && partition.leaderId() >= 0,
                    DELAYABLE_LEADER_ERRORS.contains(error),
                    error != Errors.NONE
                ));
            });
            resources.add(new ResourceResult(
                topicName,
                topicError == Errors.NONE && partitionResults.stream().anyMatch(ResourceResult::valid),
                DELAYABLE_LEADER_ERRORS.contains(topicError),
                topicError != Errors.NONE
            ));
            resources.addAll(partitionResults);
        });
        return new ResponseSummary(resources);
    }

    /**
     * Extracts FindCoordinator single-key and batch coordinator results.
     */
    public static ResponseSummary findCoordinatorSummary(Object request, Object response) {
        FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) response;
        List<ResourceResult> resources = new ArrayList<>();
        if (!findCoordinatorResponse.data().coordinators().isEmpty()) {
            findCoordinatorResponse.data().coordinators().forEach(coordinator -> {
                Errors error = Errors.forCode(coordinator.errorCode());
                resources.add(new ResourceResult(
                    coordinatorType(request) + "-" + coordinator.key(),
                    error == Errors.NONE && coordinator.nodeId() >= 0,
                    DELAYABLE_COORDINATOR_ERRORS.contains(error),
                    error != Errors.NONE
                ));
            });
        } else {
            Errors error = findCoordinatorResponse.error();
            resources.add(new ResourceResult(
                coordinatorKey(request),
                error == Errors.NONE && findCoordinatorResponse.data().nodeId() >= 0,
                DELAYABLE_COORDINATOR_ERRORS.contains(error),
                error != Errors.NONE
            ));
        }
        return new ResponseSummary(resources);
    }

    /**
     * Extracts non-heartbeat group and transaction coordinator API response summaries.
     */
    public static ResponseSummary coordinatorSummary(Object request, Object response) {
        if (response instanceof OffsetFetchResponse) {
            OffsetFetchResponse offsetFetchResponse = (OffsetFetchResponse) response;
            if (!offsetFetchResponse.data().groups().isEmpty()) {
                return offsetFetchSummary(offsetFetchResponse);
            }
        }

        AbstractResponse abstractResponse = (AbstractResponse) response;
        Set<Errors> errors = abstractResponse.errorCounts().keySet();
        boolean hasValid = errors.contains(Errors.NONE);
        List<Errors> nonSuccessErrors = errors.stream()
            .filter(error -> error != Errors.NONE)
            .toList();
        boolean allCoordinatorTransient = !nonSuccessErrors.isEmpty() &&
            nonSuccessErrors.stream().allMatch(DELAYABLE_COORDINATOR_ERRORS::contains);
        return new ResponseSummary(List.of(new ResourceResult(
            coordinatorResourceKey(request),
            hasValid,
            allCoordinatorTransient,
            !nonSuccessErrors.isEmpty()
        )));
    }

    private static OptionalLong fetchDelayCapMs(Object request) {
        if (!(request instanceof RequestChannel.Request)) {
            return OptionalLong.empty();
        }
        RequestChannel.Request channelRequest = (RequestChannel.Request) request;
        FetchRequest fetchRequest = channelRequest.body(FetchRequest.class);
        long elapsedMs = channelRequest.requestDequeueTimeNanos() < 0
            ? 0L
            : TimeUnit.NANOSECONDS.toMillis(Time.SYSTEM.nanoseconds() - channelRequest.requestDequeueTimeNanos());
        return OptionalLong.of(Math.max(fetchRequest.maxWait() - elapsedMs, 0L));
    }

    private static boolean includeClusterValidity(Object request) {
        MetadataRequest metadataRequest;
        if (request instanceof RequestChannel.Request) {
            metadataRequest = ((RequestChannel.Request) request).body(MetadataRequest.class);
        } else if (request instanceof MetadataRequest) {
            metadataRequest = (MetadataRequest) request;
        } else {
            metadataRequest = null;
        }
        return metadataRequest == null || metadataRequest.isAllTopics() || metadataRequest.data().topics().isEmpty();
    }

    private static String coordinatorKey(Object request) {
        FindCoordinatorRequest body = findCoordinatorRequest(request);
        if (body == null) {
            return "coordinator";
        }
        String key = body.data().key() != null && !body.data().key().isEmpty()
            ? body.data().key()
            : body.data().coordinatorKeys().isEmpty() ? "" : body.data().coordinatorKeys().get(0);
        return coordinatorType(body) + "-" + key;
    }

    private static String coordinatorType(Object request) {
        FindCoordinatorRequest body = findCoordinatorRequest(request);
        if (body == null) {
            return "coordinator";
        }
        return FindCoordinatorRequest.CoordinatorType.forId(body.data().keyType()).name().toLowerCase();
    }

    private static FindCoordinatorRequest findCoordinatorRequest(Object request) {
        if (request instanceof RequestChannel.Request) {
            return ((RequestChannel.Request) request).body(FindCoordinatorRequest.class);
        }
        if (request instanceof FindCoordinatorRequest) {
            return (FindCoordinatorRequest) request;
        }
        return null;
    }

    private static ResponseSummary offsetFetchSummary(OffsetFetchResponse response) {
        List<ResourceResult> resources = new ArrayList<>();
        response.data().groups().forEach(group -> {
            Errors groupError = Errors.forCode(group.errorCode());
            List<Errors> partitionErrors = new ArrayList<>();
            group.topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                    partitionErrors.add(Errors.forCode(partition.errorCode()))
                )
            );
            List<Errors> errors = new ArrayList<>();
            if (groupError != Errors.NONE) {
                errors.add(groupError);
            }
            partitionErrors.stream()
                .filter(error -> error != Errors.NONE)
                .forEach(errors::add);
            resources.add(new ResourceResult(
                groupKey(group.groupId()),
                groupError == Errors.NONE || partitionErrors.contains(Errors.NONE),
                !errors.isEmpty() && errors.stream().allMatch(DELAYABLE_COORDINATOR_ERRORS::contains),
                !errors.isEmpty()
            ));
        });
        return new ResponseSummary(resources);
    }

    private static String coordinatorResourceKey(Object request) {
        AbstractRequest body;
        if (request instanceof RequestChannel.Request) {
            body = ((RequestChannel.Request) request).body(AbstractRequest.class);
        } else if (request instanceof AbstractRequest) {
            body = (AbstractRequest) request;
        } else {
            body = null;
        }

        if (body instanceof JoinGroupRequest) {
            return groupKey(((JoinGroupRequest) body).data().groupId());
        } else if (body instanceof SyncGroupRequest) {
            return groupKey(((SyncGroupRequest) body).data().groupId());
        } else if (body instanceof OffsetCommitRequest) {
            return groupKey(((OffsetCommitRequest) body).data().groupId());
        } else if (body instanceof OffsetFetchRequest) {
            return groupKey(((OffsetFetchRequest) body).groupId());
        } else if (body instanceof LeaveGroupRequest) {
            return groupKey(((LeaveGroupRequest) body).data().groupId());
        } else if (body instanceof TxnOffsetCommitRequest) {
            return transactionKey(((TxnOffsetCommitRequest) body).data().transactionalId());
        } else if (body instanceof AddOffsetsToTxnRequest) {
            return transactionKey(((AddOffsetsToTxnRequest) body).data().transactionalId());
        } else if (body instanceof InitProducerIdRequest) {
            InitProducerIdRequest initProducerId = (InitProducerIdRequest) body;
            String transactionalId = initProducerId.data().transactionalId();
            return transactionalId != null && !transactionalId.isEmpty()
                ? transactionKey(transactionalId)
                : "producer-" + initProducerId.data().producerId();
        } else if (body instanceof EndTxnRequest) {
            EndTxnRequest endTxn = (EndTxnRequest) body;
            String transactionalId = endTxn.data().transactionalId();
            return transactionalId != null && !transactionalId.isEmpty()
                ? transactionKey(transactionalId)
                : "producer-" + endTxn.data().producerId();
        }
        return "coordinator";
    }

    private static String groupKey(String groupId) {
        return "group-" + (groupId == null ? "" : groupId);
    }

    private static String transactionKey(String transactionalId) {
        return "transaction-" + (transactionalId == null ? "" : transactionalId);
    }
}
