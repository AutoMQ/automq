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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;

/**
 * Group Coordinator's internal API.
 */
public interface GroupCoordinator {

    /**
     * Heartbeat to a Consumer Group.
     *
     * @param context           The request context.
     * @param request           The ConsumerGroupHeartbeatResponse data.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    );

    /**
     * Join a Generic Group.
     *
     * @param context           The request context.
     * @param request           The JoinGroupRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<JoinGroupResponseData> joinGroup(
        RequestContext context,
        JoinGroupRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Sync a Generic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The SyncGroupRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<SyncGroupResponseData> syncGroup(
        RequestContext context,
        SyncGroupRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Heartbeat to a Generic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The HeartbeatRequest data.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<HeartbeatResponseData> heartbeat(
        RequestContext context,
        HeartbeatRequestData request
    );

    /**
     * Leave a Generic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The LeaveGroupRequest data.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<LeaveGroupResponseData> leaveGroup(
        RequestContext context,
        LeaveGroupRequestData request
    );

    /**
     * List Groups.
     *
     * @param context           The coordinator request context.
     * @param request           The ListGroupRequest data.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<ListGroupsResponseData> listGroups(
        RequestContext context,
        ListGroupsRequestData request
    );

    /**
     * Describe Groups.
     *
     * @param context           The coordinator request context.
     * @param groupIds          The group ids.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> describeGroups(
        RequestContext context,
        List<String> groupIds
    );

    /**
     * Delete Groups.
     *
     * @param context           The request context.
     * @param groupIds          The group ids.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> deleteGroups(
        RequestContext context,
        List<String> groupIds,
        BufferSupplier bufferSupplier
    );

    /**
     * Fetch offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The OffsetFetchRequestGroup request.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchOffsets(
        RequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable
    );

    /**
     * Fetch all offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The OffsetFetchRequestGroup request.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchAllOffsets(
        RequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable
    );

    /**
     * Commit offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The OffsetCommitRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<OffsetCommitResponseData> commitOffsets(
        RequestContext context,
        OffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Commit transactional offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The TnxOffsetCommitRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<TxnOffsetCommitResponseData> commitTransactionalOffsets(
        RequestContext context,
        TxnOffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Delete offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The OffsetDeleteRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<OffsetDeleteResponseData> deleteOffsets(
        RequestContext context,
        OffsetDeleteRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Return the partition index for the given Group.
     *
     * @param groupId           The group id.
     *
     * @return The partition index.
     */
    int partitionFor(String groupId);

    /**
     * Commit or abort the pending transactional offsets for the given partitions.
     *
     * @param producerId        The producer id.
     * @param partitions        The partitions.
     * @param transactionResult The result of the transaction.
     */
    void onTransactionCompleted(
        long producerId,
        Iterable<TopicPartition> partitions,
        TransactionResult transactionResult
    );

    /**
     * Remove the provided deleted partitions offsets.
     *
     * @param topicPartitions   The deleted partitions.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     */
    void onPartitionsDeleted(
        List<TopicPartition> topicPartitions,
        BufferSupplier bufferSupplier
    );

    /**
     * Group coordinator is now the leader for the given partition at the
     * given leader epoch. It should load cached state from the partition
     * and begin handling requests for groups mapped to it.
     *
     * @param groupMetadataPartitionIndex         The partition index.
     * @param groupMetadataPartitionLeaderEpoch   The leader epoch of the partition.
     */
    void onElection(
        int groupMetadataPartitionIndex,
        int groupMetadataPartitionLeaderEpoch
    );

    /**
     * Group coordinator is no longer the leader for the given partition
     * at the given leader epoch. It should unload cached state and stop
     * handling requests for groups mapped to it.
     *
     * @param groupMetadataPartitionIndex         The partition index.
     * @param groupMetadataPartitionLeaderEpoch   The leader epoch of the partition as an
     *                                            optional value. An empty value means that
     *                                            the topic was deleted.
     */
    void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    );

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The metadata delta.
     */
    void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    );

    /**
     * Return the configuration properties of the internal group
     * metadata topic.
     *
     * @return Properties of the internal topic.
     */
    Properties groupMetadataTopicConfigs();

    /**
     * Startup the group coordinator.
     *
     * @param groupMetadataTopicPartitionCount  A supplier to get the number of partitions
     *                                          of the consumer offsets topic.
     */
    void startup(IntSupplier groupMetadataTopicPartitionCount);

    /**
     * Shutdown the group coordinator.
     */
    void shutdown();
}
