/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.kafkalinking;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AutomqUpdateGroupRequestData;
import org.apache.kafka.common.message.AutomqUpdateGroupResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.coordinator.group.GroupCoordinator;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.util.FutureUtils;

import java.time.Duration;
import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.IntSupplier;

public class DefaultKafkaLinkingGroupCoordinator extends KafkaLinkingGroupCoordinator {
    protected final GroupCoordinator groupCoordinator;

    public DefaultKafkaLinkingGroupCoordinator(GroupCoordinator groupCoordinator) {
        this.groupCoordinator = groupCoordinator;
    }

    @Override
    public CompletableFuture<AutomqUpdateGroupResponseData> updateGroup(
        RequestContext context,
        AutomqUpdateGroupRequestData request,
        BufferSupplier bufferSupplier) {
        return FutureUtils.failedFuture(new UnsupportedOperationException(ApiKeys.AUTOMQ_UPDATE_GROUP.name() + " is not supported"));
    }

    @Override
    public CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(RequestContext context,
        ConsumerGroupHeartbeatRequestData request) {
        return this.groupCoordinator.consumerGroupHeartbeat(context, request);
    }

    @Override
    public CompletableFuture<ShareGroupHeartbeatResponseData> shareGroupHeartbeat(RequestContext context,
        ShareGroupHeartbeatRequestData request) {
        return this.groupCoordinator.shareGroupHeartbeat(context, request);
    }

    @Override
    public CompletableFuture<JoinGroupResponseData> joinGroup(RequestContext context, JoinGroupRequestData request,
        BufferSupplier bufferSupplier) {
        return this.groupCoordinator.joinGroup(context, request, bufferSupplier);
    }

    @Override
    public CompletableFuture<SyncGroupResponseData> syncGroup(RequestContext context, SyncGroupRequestData request,
        BufferSupplier bufferSupplier) {
        return this.groupCoordinator.syncGroup(context, request, bufferSupplier);
    }

    @Override
    public CompletableFuture<HeartbeatResponseData> heartbeat(RequestContext context, HeartbeatRequestData request) {
        return this.groupCoordinator.heartbeat(context, request);
    }

    @Override
    public CompletableFuture<LeaveGroupResponseData> leaveGroup(RequestContext context, LeaveGroupRequestData request) {
        return this.groupCoordinator.leaveGroup(context, request);
    }

    @Override
    public CompletableFuture<ListGroupsResponseData> listGroups(RequestContext context, ListGroupsRequestData request) {
        return this.groupCoordinator.listGroups(context, request);
    }

    @Override
    public CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> describeGroups(RequestContext context,
        List<String> groupIds) {
        return this.groupCoordinator.describeGroups(context, groupIds);
    }

    @Override
    public CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> consumerGroupDescribe(
        RequestContext context, List<String> groupIds) {
        return this.groupCoordinator.consumerGroupDescribe(context, groupIds);
    }

    @Override
    public CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> shareGroupDescribe(
        RequestContext context, List<String> groupIds) {
        return this.groupCoordinator.shareGroupDescribe(context, groupIds);
    }

    @Override
    public CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> deleteGroups(
        RequestContext context, List<String> groupIds, BufferSupplier bufferSupplier) {
        return this.groupCoordinator.deleteGroups(context, groupIds, bufferSupplier);
    }

    @Override
    public CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchOffsets(RequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request, boolean requireStable) {
        return this.groupCoordinator.fetchOffsets(context, request, requireStable);
    }

    @Override
    public CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchAllOffsets(RequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request, boolean requireStable) {
        return this.groupCoordinator.fetchAllOffsets(context, request, requireStable);
    }

    @Override
    public CompletableFuture<OffsetCommitResponseData> commitOffsets(RequestContext context,
        OffsetCommitRequestData request, BufferSupplier bufferSupplier) {
        return this.groupCoordinator.commitOffsets(context, request, bufferSupplier);
    }

    @Override
    public CompletableFuture<TxnOffsetCommitResponseData> commitTransactionalOffsets(RequestContext context,
        TxnOffsetCommitRequestData request, BufferSupplier bufferSupplier) {
        return this.groupCoordinator.commitTransactionalOffsets(context, request, bufferSupplier);
    }

    @Override
    public CompletableFuture<OffsetDeleteResponseData> deleteOffsets(RequestContext context,
        OffsetDeleteRequestData request, BufferSupplier bufferSupplier) {
        return this.groupCoordinator.deleteOffsets(context, request, bufferSupplier);
    }

    @Override
    public CompletableFuture<Void> completeTransaction(TopicPartition tp, long producerId, short producerEpoch,
        int coordinatorEpoch, TransactionResult result, Duration timeout) {
        return this.groupCoordinator.completeTransaction(tp, producerId, producerEpoch, coordinatorEpoch, result, timeout);
    }

    @Override
    public int partitionFor(String groupId) {
        return this.groupCoordinator.partitionFor(groupId);
    }

    @Override
    public void onTransactionCompleted(long producerId, Iterable<TopicPartition> partitions,
        TransactionResult transactionResult) {
        this.groupCoordinator.onTransactionCompleted(producerId, partitions, transactionResult);
    }

    @Override
    public void onPartitionsDeleted(List<TopicPartition> topicPartitions,
        BufferSupplier bufferSupplier) throws ExecutionException, InterruptedException {
        this.groupCoordinator.onPartitionsDeleted(topicPartitions, bufferSupplier);
    }

    @Override
    public void onElection(int groupMetadataPartitionIndex, int groupMetadataPartitionLeaderEpoch) {
        this.groupCoordinator.onElection(groupMetadataPartitionIndex, groupMetadataPartitionLeaderEpoch);
    }

    @Override
    public void onResignation(int groupMetadataPartitionIndex, OptionalInt groupMetadataPartitionLeaderEpoch) {
        this.groupCoordinator.onResignation(groupMetadataPartitionIndex, groupMetadataPartitionLeaderEpoch);
    }

    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        this.groupCoordinator.onNewMetadataImage(newImage, delta);
    }

    @Override
    public Properties groupMetadataTopicConfigs() {
        return this.groupCoordinator.groupMetadataTopicConfigs();
    }

    @Override
    public void startup(IntSupplier groupMetadataTopicPartitionCount) {
        this.groupCoordinator.startup(groupMetadataTopicPartitionCount);
    }

    @Override
    public void shutdown() {
        this.groupCoordinator.shutdown();
    }
}
