/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.controller;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CloseStreamsRequestData;
import org.apache.kafka.common.message.CloseStreamsResponseData;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData;
import org.apache.kafka.common.message.CommitStreamSetObjectResponseData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateStreamsRequestData;
import org.apache.kafka.common.message.CreateStreamsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.DeleteKVsResponseData;
import org.apache.kafka.common.message.DeleteStreamsRequestData;
import org.apache.kafka.common.message.DeleteStreamsResponseData;
import org.apache.kafka.common.message.DescribeStreamsRequestData;
import org.apache.kafka.common.message.DescribeStreamsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.GetKVsRequestData;
import org.apache.kafka.common.message.GetKVsResponseData;
import org.apache.kafka.common.message.GetNextNodeIdRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.OpenStreamsRequestData;
import org.apache.kafka.common.message.OpenStreamsResponseData;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.message.TrimStreamsRequestData;
import org.apache.kafka.common.message.TrimStreamsResponseData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;

/**
 * ControllerTimeoutDecorator intends to add the timeouts for the low priority events
 * in order to prevent overloading of the main Controller.
 * Because during an overload, Controller mistakenly consider all Brokers as Fenced,
 * triggering a massive re-partitioning.
 */
public class ControllerTimeoutDecorator implements Controller {

    private final Controller controller;

    private final int defaultTimeoutInMs;

    public ControllerTimeoutDecorator(Controller controller) {
        this.controller = controller;
        this.defaultTimeoutInMs = 3000;
    }

    public ControllerTimeoutDecorator(Controller controller, int defaultTimeoutInMs) {
        this.controller = controller;
        this.defaultTimeoutInMs = defaultTimeoutInMs;
    }

    @Override
    public CompletableFuture<AlterPartitionResponseData> alterPartition(ControllerRequestContext context, AlterPartitionRequestData request) {
        return controller.alterPartition(context, request);
    }

    @Override
    public CompletableFuture<AlterUserScramCredentialsResponseData> alterUserScramCredentials(ControllerRequestContext context, AlterUserScramCredentialsRequestData request) {
        return controller.alterUserScramCredentials(context, request);
    }

    @Override
    public CompletableFuture<CreateDelegationTokenResponseData> createDelegationToken(ControllerRequestContext context, CreateDelegationTokenRequestData request) {
        return controller.createDelegationToken(context, request);
    }

    @Override
    public CompletableFuture<RenewDelegationTokenResponseData> renewDelegationToken(ControllerRequestContext context, RenewDelegationTokenRequestData request) {
        return controller.renewDelegationToken(context, request);
    }

    @Override
    public CompletableFuture<ExpireDelegationTokenResponseData> expireDelegationToken(ControllerRequestContext context, ExpireDelegationTokenRequestData request) {
        return controller.expireDelegationToken(context, request);
    }

    @Override
    public CompletableFuture<CreateTopicsResponseData> createTopics(ControllerRequestContext context, CreateTopicsRequestData request, Set<String> describable) {
        return controller.createTopics(context, request, describable);
    }

    @Override
    public CompletableFuture<Void> unregisterBroker(ControllerRequestContext context, int brokerId) {
        return controller.unregisterBroker(context, brokerId);
    }

    @Override
    public CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIds(ControllerRequestContext context, Collection<String> topicNames) {
        return controller.findTopicIds(context, topicNames);
    }

    @Override
    public CompletableFuture<Map<String, Uuid>> findAllTopicIds(ControllerRequestContext context) {
        return controller.findAllTopicIds(context);
    }

    @Override
    public CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNames(ControllerRequestContext context, Collection<Uuid> topicIds) {
        return controller.findTopicNames(context, topicIds);
    }

    @Override
    public CompletableFuture<Map<Uuid, ApiError>> deleteTopics(ControllerRequestContext context, Collection<Uuid> topicIds) {
        return controller.deleteTopics(context, topicIds);
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ResultOrError<Map<String, String>>>> describeConfigs(ControllerRequestContext context, Map<ConfigResource, Collection<String>> resources) {
        return controller.describeConfigs(context, resources);
    }

    @Override
    public CompletableFuture<ElectLeadersResponseData> electLeaders(ControllerRequestContext context, ElectLeadersRequestData request) {
        return controller.electLeaders(context, request);
    }

    @Override
    public CompletableFuture<FinalizedControllerFeatures> finalizedFeatures(ControllerRequestContext context) {
        return controller.finalizedFeatures(context);
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> incrementalAlterConfigs(ControllerRequestContext context, Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
                                                                                    boolean validateOnly) {
        return controller.incrementalAlterConfigs(context, configChanges, validateOnly);
    }

    @Override
    public CompletableFuture<AlterPartitionReassignmentsResponseData> alterPartitionReassignments(ControllerRequestContext context, AlterPartitionReassignmentsRequestData request) {
        return controller.alterPartitionReassignments(context, request);
    }

    @Override
    public CompletableFuture<ListPartitionReassignmentsResponseData> listPartitionReassignments(ControllerRequestContext context, ListPartitionReassignmentsRequestData request) {
        return controller.listPartitionReassignments(context, request);
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> legacyAlterConfigs(ControllerRequestContext context, Map<ConfigResource, Map<String, String>> newConfigs, boolean validateOnly) {
        return controller.legacyAlterConfigs(context, newConfigs, validateOnly);
    }

    @Override
    public CompletableFuture<BrokerHeartbeatReply> processBrokerHeartbeat(ControllerRequestContext context, BrokerHeartbeatRequestData request) {
        return controller.processBrokerHeartbeat(context, request);
    }

    @Override
    public CompletableFuture<BrokerRegistrationReply> registerBroker(ControllerRequestContext context, BrokerRegistrationRequestData request) {
        return controller.registerBroker(context, request);
    }

    @Override
    public CompletableFuture<Void> waitForReadyBrokers(int minBrokers) {
        return controller.waitForReadyBrokers(minBrokers);
    }

    @Override
    public CompletableFuture<Map<ClientQuotaEntity, ApiError>> alterClientQuotas(ControllerRequestContext context, Collection<ClientQuotaAlteration> quotaAlterations, boolean validateOnly) {
        return controller.alterClientQuotas(context, quotaAlterations, validateOnly);
    }

    @Override
    public CompletableFuture<AllocateProducerIdsResponseData> allocateProducerIds(ControllerRequestContext context, AllocateProducerIdsRequestData request) {
        return controller.allocateProducerIds(context, request);
    }

    @Override
    public CompletableFuture<UpdateFeaturesResponseData> updateFeatures(ControllerRequestContext context, UpdateFeaturesRequestData request) {
        return controller.updateFeatures(context, request);
    }

    @Override
    public CompletableFuture<List<CreatePartitionsTopicResult>> createPartitions(ControllerRequestContext context, List<CreatePartitionsTopic> topics, boolean validateOnly) {
        return controller.createPartitions(context, topics, validateOnly);
    }

    @Override
    public CompletableFuture<Void> registerController(ControllerRequestContext context, ControllerRegistrationRequestData request) {
        return controller.registerController(context, request);
    }

    @Override
    public CompletableFuture<AssignReplicasToDirsResponseData> assignReplicasToDirs(ControllerRequestContext context, AssignReplicasToDirsRequestData request) {
        return controller.assignReplicasToDirs(context, request);
    }

    @Override
    public void beginShutdown() {
        controller.beginShutdown();
    }

    @Override
    public int curClaimEpoch() {
        return controller.curClaimEpoch();
    }

    @Override
    public void close() throws InterruptedException {
        controller.close();
    }

    @Override
    public CompletableFuture<List<AclCreateResult>> createAcls(ControllerRequestContext context, List<AclBinding> aclBindings) {
        return controller.createAcls(context, aclBindings);
    }

    @Override
    public CompletableFuture<List<AclDeleteResult>> deleteAcls(ControllerRequestContext context, List<AclBindingFilter> aclBindingFilters) {
        return controller.deleteAcls(context, aclBindingFilters);
    }

    // AutoMQ for Kafka inject start

    @Override
    public CompletableFuture<Integer> getNextNodeId(
            ControllerRequestContext context,
            GetNextNodeIdRequestData request
    ) {
        return controller.getNextNodeId(context, request);
    }

    @Override
    public CompletableFuture<Void> checkS3ObjectsLifecycle(ControllerRequestContext context) {
        return controller.checkS3ObjectsLifecycle(context).orTimeout(defaultTimeoutInMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> notifyS3ObjectDeleted(
            ControllerRequestContext context,
            List<Long/*objectId*/> deletedObjectIds
    ) {
        return controller.notifyS3ObjectDeleted(context, deletedObjectIds);
    }

    @Override
    public CompletableFuture<CreateStreamsResponseData> createStreams(
            ControllerRequestContext context,
            CreateStreamsRequestData request
    ) {
        return controller.createStreams(context, request);
    }

    @Override
    public CompletableFuture<OpenStreamsResponseData> openStreams(
            ControllerRequestContext context,
            OpenStreamsRequestData request
    ) {
        return controller.openStreams(context, request);
    }

    @Override
    public CompletableFuture<CloseStreamsResponseData> closeStreams(
            ControllerRequestContext context,
            CloseStreamsRequestData request
    ) {
        return controller.closeStreams(context, request);
    }

    @Override
    public CompletableFuture<TrimStreamsResponseData> trimStreams(
            ControllerRequestContext context,
            TrimStreamsRequestData request
    ) {
        return controller.trimStreams(context, request);
    }

    @Override
    public CompletableFuture<DeleteStreamsResponseData> deleteStreams(
            ControllerRequestContext context,
            DeleteStreamsRequestData request
    ) {
        return controller.deleteStreams(context, request).orTimeout(defaultTimeoutInMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<PrepareS3ObjectResponseData> prepareObject(
            ControllerRequestContext context,
            PrepareS3ObjectRequestData request
    ) {
        return controller.prepareObject(context, request);
    }

    @Override
    public CompletableFuture<CommitStreamSetObjectResponseData> commitStreamSetObject(
            ControllerRequestContext context,
            CommitStreamSetObjectRequestData request
    ) {
        return controller.commitStreamSetObject(context, request);
    }

    @Override
    public CompletableFuture<CommitStreamObjectResponseData> commitStreamObject(
            ControllerRequestContext context,
            CommitStreamObjectRequestData request
    ) {
        return controller.commitStreamObject(context, request);
    }

    @Override
    public CompletableFuture<DescribeStreamsResponseData> describeStreams(
            ControllerRequestContext context,
            DescribeStreamsRequestData request
    ) {
        return controller.describeStreams(context, request);
    }

    @Override
    public CompletableFuture<GetOpeningStreamsResponseData> getOpeningStreams(
            ControllerRequestContext context,
            GetOpeningStreamsRequestData request
    ) {
        return controller.getOpeningStreams(context, request);
    }

    @Override
    public CompletableFuture<GetKVsResponseData> getKVs(
            ControllerRequestContext context,
            GetKVsRequestData request
    ) {
        return controller.getKVs(context, request);
    }

    @Override
    public CompletableFuture<PutKVsResponseData> putKVs(
            ControllerRequestContext context,
            PutKVsRequestData request
    ) {
        return controller.putKVs(context, request);
    }

    @Override
    public CompletableFuture<DeleteKVsResponseData> deleteKVs(
            ControllerRequestContext context,
            DeleteKVsRequestData request
    ) {
        return controller.deleteKVs(context, request);
    }

    // AutoMQ for Kafka inject end
}
