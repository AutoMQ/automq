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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKeyCollection;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartition;
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartitionCollection;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsResponseData.AclDescription;
import org.apache.kafka.common.message.DescribeAclsResponseData.DescribeAclsResource;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState;
import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.CreateTopicsRequest.Builder;
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigType;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.apache.kafka.test.TestUtils.toBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RequestResponseTest {

    @Test
    public void testSerialization() throws Exception {
        checkRequest(createFindCoordinatorRequest(0), true);
        checkRequest(createFindCoordinatorRequest(1), true);
        checkErrorResponse(createFindCoordinatorRequest(0), new UnknownServerException(), true);
        checkErrorResponse(createFindCoordinatorRequest(1), new UnknownServerException(), true);
        checkResponse(createFindCoordinatorResponse(), 0, true);
        checkResponse(createFindCoordinatorResponse(), 1, true);
        checkRequest(createControlledShutdownRequest(), true);
        checkResponse(createControlledShutdownResponse(), 1, true);
        checkErrorResponse(createControlledShutdownRequest(), new UnknownServerException(), true);
        checkErrorResponse(createControlledShutdownRequest(0), new UnknownServerException(), true);
        checkRequest(createFetchRequest(4), true);
        checkResponse(createFetchResponse(), 4, true);
        List<TopicPartition> toForgetTopics = new ArrayList<>();
        toForgetTopics.add(new TopicPartition("foo", 0));
        toForgetTopics.add(new TopicPartition("foo", 2));
        toForgetTopics.add(new TopicPartition("bar", 0));
        checkRequest(createFetchRequest(7, new FetchMetadata(123, 456), toForgetTopics), true);
        checkResponse(createFetchResponse(123), 7, true);
        checkResponse(createFetchResponse(Errors.FETCH_SESSION_ID_NOT_FOUND, 123), 7, true);
        checkErrorResponse(createFetchRequest(4), new UnknownServerException(), true);
        checkRequest(createHeartBeatRequest(), true);
        checkErrorResponse(createHeartBeatRequest(), new UnknownServerException(), true);
        checkResponse(createHeartBeatResponse(), 0, true);

        for (int v = ApiKeys.JOIN_GROUP.oldestVersion(); v <= ApiKeys.JOIN_GROUP.latestVersion(); v++) {
            checkRequest(createJoinGroupRequest(v), true);
            checkErrorResponse(createJoinGroupRequest(v), new UnknownServerException(), true);
            checkResponse(createJoinGroupResponse(v), v, true);
        }

        for (int v = ApiKeys.SYNC_GROUP.oldestVersion(); v <= ApiKeys.SYNC_GROUP.latestVersion(); v++) {
            checkRequest(createSyncGroupRequest(v), true);
            checkErrorResponse(createSyncGroupRequest(v), new UnknownServerException(), true);
            checkResponse(createSyncGroupResponse(v), v, true);
        }

        checkRequest(createLeaveGroupRequest(), true);
        checkErrorResponse(createLeaveGroupRequest(), new UnknownServerException(), true);
        checkResponse(createLeaveGroupResponse(), 0, true);

        for (short v = ApiKeys.LIST_GROUPS.oldestVersion(); v <= ApiKeys.LIST_GROUPS.latestVersion(); v++) {
            checkRequest(createListGroupsRequest(v), false);
            checkErrorResponse(createListGroupsRequest(v), new UnknownServerException(), true);
            checkResponse(createListGroupsResponse(v), v, true);
        }

        checkRequest(createDescribeGroupRequest(), true);
        checkErrorResponse(createDescribeGroupRequest(), new UnknownServerException(), true);
        checkResponse(createDescribeGroupResponse(), 0, true);
        checkRequest(createDeleteGroupsRequest(), true);
        checkErrorResponse(createDeleteGroupsRequest(), new UnknownServerException(), true);
        checkResponse(createDeleteGroupsResponse(), 0, true);
        for (int i = 0; i < ApiKeys.LIST_OFFSETS.latestVersion(); i++) {
            checkRequest(createListOffsetRequest(i), true);
            checkErrorResponse(createListOffsetRequest(i), new UnknownServerException(), true);
            checkResponse(createListOffsetResponse(i), i, true);
        }
        checkRequest(MetadataRequest.Builder.allTopics().build((short) 2), true);
        checkRequest(createMetadataRequest(1, Collections.singletonList("topic1")), true);
        checkErrorResponse(createMetadataRequest(1, Collections.singletonList("topic1")), new UnknownServerException(), true);
        checkResponse(createMetadataResponse(), 2, true);
        checkErrorResponse(createMetadataRequest(2, Collections.singletonList("topic1")), new UnknownServerException(), true);
        checkResponse(createMetadataResponse(), 3, true);
        checkErrorResponse(createMetadataRequest(3, Collections.singletonList("topic1")), new UnknownServerException(), true);
        checkResponse(createMetadataResponse(), 4, true);
        checkErrorResponse(createMetadataRequest(4, Collections.singletonList("topic1")), new UnknownServerException(), true);
        checkRequest(createOffsetFetchRequestForAllPartition("group1", false), true);
        checkRequest(createOffsetFetchRequestForAllPartition("group1", true), true);
        checkErrorResponse(createOffsetFetchRequestForAllPartition("group1", false), new NotCoordinatorException("Not Coordinator"), true);
        checkErrorResponse(createOffsetFetchRequestForAllPartition("group1", true), new NotCoordinatorException("Not Coordinator"), true);
        checkRequest(createOffsetFetchRequest(0, false), true);
        checkRequest(createOffsetFetchRequest(1, false), true);
        checkRequest(createOffsetFetchRequest(2, false), true);
        checkRequest(createOffsetFetchRequest(7, true), true);
        checkRequest(createOffsetFetchRequestForAllPartition("group1", false), true);
        checkRequest(createOffsetFetchRequestForAllPartition("group1", true), true);
        checkErrorResponse(createOffsetFetchRequest(0, false), new UnknownServerException(), true);
        checkErrorResponse(createOffsetFetchRequest(1, false), new UnknownServerException(), true);
        checkErrorResponse(createOffsetFetchRequest(2, false), new UnknownServerException(), true);
        checkErrorResponse(createOffsetFetchRequest(7, true), new UnknownServerException(), true);
        checkResponse(createOffsetFetchResponse(), 0, true);
        checkRequest(createProduceRequest(2), true);
        checkErrorResponse(createProduceRequest(2), new UnknownServerException(), true);
        checkRequest(createProduceRequest(3), true);
        checkErrorResponse(createProduceRequest(3), new UnknownServerException(), true);
        checkResponse(createProduceResponse(), 2, true);
        checkResponse(createProduceResponseWithErrorMessage(), 8, true);

        for (int v = ApiKeys.STOP_REPLICA.oldestVersion(); v <= ApiKeys.STOP_REPLICA.latestVersion(); v++) {
            checkRequest(createStopReplicaRequest(v, true), true);
            checkRequest(createStopReplicaRequest(v, false), true);
            checkErrorResponse(createStopReplicaRequest(v, true), new UnknownServerException(), true);
            checkErrorResponse(createStopReplicaRequest(v, false), new UnknownServerException(), true);
            checkResponse(createStopReplicaResponse(), v, true);
        }

        checkRequest(createLeaderAndIsrRequest(0), true);
        checkErrorResponse(createLeaderAndIsrRequest(0), new UnknownServerException(), false);
        checkRequest(createLeaderAndIsrRequest(1), true);
        checkErrorResponse(createLeaderAndIsrRequest(1), new UnknownServerException(), false);
        checkRequest(createLeaderAndIsrRequest(2), true);
        checkErrorResponse(createLeaderAndIsrRequest(2), new UnknownServerException(), false);
        checkResponse(createLeaderAndIsrResponse(), 0, true);
        checkRequest(createSaslHandshakeRequest(), true);
        checkErrorResponse(createSaslHandshakeRequest(), new UnknownServerException(), true);
        checkResponse(createSaslHandshakeResponse(), 0, true);
        checkRequest(createSaslAuthenticateRequest(), true);
        checkErrorResponse(createSaslAuthenticateRequest(), new UnknownServerException(), true);
        checkResponse(createSaslAuthenticateResponse(), 0, true);
        checkResponse(createSaslAuthenticateResponse(), 1, true);
        checkRequest(createApiVersionRequest(), true);
        checkErrorResponse(createApiVersionRequest(), new UnknownServerException(), true);
        checkErrorResponse(createApiVersionRequest(), new UnsupportedVersionException("Not Supported"), true);
        checkResponse(createApiVersionResponse(), 0, true);
        checkResponse(createApiVersionResponse(), 1, true);
        checkResponse(createApiVersionResponse(), 2, true);
        checkResponse(createApiVersionResponse(), 3, true);
        checkResponse(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE, 0, true);
        checkResponse(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE, 1, true);
        checkResponse(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE, 2, true);
        checkResponse(ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE, 3, true);

        checkRequest(createCreateTopicRequest(0), true);
        checkErrorResponse(createCreateTopicRequest(0), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 0, true);
        checkRequest(createCreateTopicRequest(1), true);
        checkErrorResponse(createCreateTopicRequest(1), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 1, true);
        checkRequest(createCreateTopicRequest(2), true);
        checkErrorResponse(createCreateTopicRequest(2), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 2, true);
        checkRequest(createCreateTopicRequest(3), true);
        checkErrorResponse(createCreateTopicRequest(3), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 3, true);
        checkRequest(createCreateTopicRequest(4), true);
        checkErrorResponse(createCreateTopicRequest(4), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 4, true);
        checkRequest(createCreateTopicRequest(5), true);
        checkErrorResponse(createCreateTopicRequest(5), new UnknownServerException(), true);
        checkResponse(createCreateTopicResponse(), 5, true);

        checkRequest(createDeleteTopicsRequest(), true);
        checkErrorResponse(createDeleteTopicsRequest(), new UnknownServerException(), true);
        checkResponse(createDeleteTopicsResponse(), 0, true);

        checkRequest(createInitPidRequest(), true);
        checkErrorResponse(createInitPidRequest(), new UnknownServerException(), true);
        checkResponse(createInitPidResponse(), 0, true);

        checkRequest(createAddPartitionsToTxnRequest(), true);
        checkResponse(createAddPartitionsToTxnResponse(), 0, true);
        checkErrorResponse(createAddPartitionsToTxnRequest(), new UnknownServerException(), true);
        checkRequest(createAddOffsetsToTxnRequest(), true);
        checkResponse(createAddOffsetsToTxnResponse(), 0, true);
        checkErrorResponse(createAddOffsetsToTxnRequest(), new UnknownServerException(), true);
        checkRequest(createEndTxnRequest(), true);
        checkResponse(createEndTxnResponse(), 0, true);
        checkErrorResponse(createEndTxnRequest(), new UnknownServerException(), true);
        checkRequest(createWriteTxnMarkersRequest(), true);
        checkResponse(createWriteTxnMarkersResponse(), 0, true);
        checkErrorResponse(createWriteTxnMarkersRequest(), new UnknownServerException(), true);

        checkOlderFetchVersions();
        checkResponse(createMetadataResponse(), 0, true);
        checkResponse(createMetadataResponse(), 1, true);
        checkErrorResponse(createMetadataRequest(1, Collections.singletonList("topic1")), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(0), true);
        checkErrorResponse(createOffsetCommitRequest(0), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(1), true);
        checkErrorResponse(createOffsetCommitRequest(1), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(2), true);
        checkErrorResponse(createOffsetCommitRequest(2), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(3), true);
        checkErrorResponse(createOffsetCommitRequest(3), new UnknownServerException(), true);
        checkRequest(createOffsetCommitRequest(4), true);
        checkErrorResponse(createOffsetCommitRequest(4), new UnknownServerException(), true);
        checkResponse(createOffsetCommitResponse(), 4, true);
        checkRequest(createOffsetCommitRequest(5), true);
        checkErrorResponse(createOffsetCommitRequest(5), new UnknownServerException(), true);
        checkResponse(createOffsetCommitResponse(), 5, true);
        checkRequest(createJoinGroupRequest(0), true);
        checkRequest(createUpdateMetadataRequest(0, null), false);
        checkErrorResponse(createUpdateMetadataRequest(0, null), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(1, null), false);
        checkRequest(createUpdateMetadataRequest(1, "rack1"), false);
        checkErrorResponse(createUpdateMetadataRequest(1, null), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(2, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(2, null), false);
        checkErrorResponse(createUpdateMetadataRequest(2, "rack1"), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(3, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(3, null), false);
        checkErrorResponse(createUpdateMetadataRequest(3, "rack1"), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(4, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(4, null), false);
        checkErrorResponse(createUpdateMetadataRequest(4, "rack1"), new UnknownServerException(), true);
        checkRequest(createUpdateMetadataRequest(5, "rack1"), false);
        checkRequest(createUpdateMetadataRequest(5, null), false);
        checkErrorResponse(createUpdateMetadataRequest(5, "rack1"), new UnknownServerException(), true);
        checkResponse(createUpdateMetadataResponse(), 0, true);
        checkRequest(createListOffsetRequest(0), true);
        checkErrorResponse(createListOffsetRequest(0), new UnknownServerException(), true);
        checkResponse(createListOffsetResponse(0), 0, true);
        checkRequest(createLeaderEpochRequestForReplica(0, 1), true);
        checkRequest(createLeaderEpochRequestForConsumer(), true);
        checkResponse(createLeaderEpochResponse(), 0, true);
        checkErrorResponse(createLeaderEpochRequestForConsumer(), new UnknownServerException(), true);
        checkRequest(createAddPartitionsToTxnRequest(), true);
        checkErrorResponse(createAddPartitionsToTxnRequest(), new UnknownServerException(), true);
        checkResponse(createAddPartitionsToTxnResponse(), 0, true);
        checkRequest(createAddOffsetsToTxnRequest(), true);
        checkErrorResponse(createAddOffsetsToTxnRequest(), new UnknownServerException(), true);
        checkResponse(createAddOffsetsToTxnResponse(), 0, true);
        checkRequest(createEndTxnRequest(), true);
        checkErrorResponse(createEndTxnRequest(), new UnknownServerException(), true);
        checkResponse(createEndTxnResponse(), 0, true);
        checkRequest(createWriteTxnMarkersRequest(), true);
        checkErrorResponse(createWriteTxnMarkersRequest(), new UnknownServerException(), true);
        checkResponse(createWriteTxnMarkersResponse(), 0, true);
        checkRequest(createTxnOffsetCommitRequest(0), true);
        checkRequest(createTxnOffsetCommitRequest(3), true);
        checkRequest(createTxnOffsetCommitRequestWithAutoDowngrade(2), true);
        checkErrorResponse(createTxnOffsetCommitRequest(0), new UnknownServerException(), true);
        checkErrorResponse(createTxnOffsetCommitRequest(3), new UnknownServerException(), true);
        checkErrorResponse(createTxnOffsetCommitRequestWithAutoDowngrade(2), new UnknownServerException(), true);
        checkResponse(createTxnOffsetCommitResponse(), 0, true);
        checkRequest(createDescribeAclsRequest(), true);
        checkErrorResponse(createDescribeAclsRequest(), new SecurityDisabledException("Security is not enabled."), true);
        checkResponse(createDescribeAclsResponse(), ApiKeys.DESCRIBE_ACLS.latestVersion(), true);
        checkRequest(createCreateAclsRequest(), true);
        checkErrorResponse(createCreateAclsRequest(), new SecurityDisabledException("Security is not enabled."), true);
        checkResponse(createCreateAclsResponse(), ApiKeys.CREATE_ACLS.latestVersion(), true);
        checkRequest(createDeleteAclsRequest(), true);
        checkErrorResponse(createDeleteAclsRequest(), new SecurityDisabledException("Security is not enabled."), true);
        checkResponse(createDeleteAclsResponse(), ApiKeys.DELETE_ACLS.latestVersion(), true);
        checkRequest(createAlterConfigsRequest(), false);
        checkErrorResponse(createAlterConfigsRequest(), new UnknownServerException(), true);
        checkResponse(createAlterConfigsResponse(), 0, false);
        checkRequest(createDescribeConfigsRequest(0), true);
        checkRequest(createDescribeConfigsRequestWithConfigEntries(0), false);
        checkErrorResponse(createDescribeConfigsRequest(0), new UnknownServerException(), true);
        checkResponse(createDescribeConfigsResponse(), 0, false);
        checkRequest(createDescribeConfigsRequest(1), true);
        checkRequest(createDescribeConfigsRequestWithConfigEntries(1), false);
        checkRequest(createDescribeConfigsRequestWithDocumentation(1), false);
        checkRequest(createDescribeConfigsRequestWithDocumentation(2), false);
        checkRequest(createDescribeConfigsRequestWithDocumentation(3), false);
        checkErrorResponse(createDescribeConfigsRequest(1), new UnknownServerException(), true);
        checkResponse(createDescribeConfigsResponse(), 1, false);
        checkDescribeConfigsResponseVersions();
        checkRequest(createCreatePartitionsRequest(), true);
        checkRequest(createCreatePartitionsRequestWithAssignments(), false);
        checkErrorResponse(createCreatePartitionsRequest(), new InvalidTopicException(), true);
        checkResponse(createCreatePartitionsResponse(), 0, true);
        checkRequest(createCreateTokenRequest(), true);
        checkErrorResponse(createCreateTokenRequest(), new UnknownServerException(), true);
        checkResponse(createCreateTokenResponse(), 0, true);
        checkRequest(createDescribeTokenRequest(), true);
        checkErrorResponse(createDescribeTokenRequest(), new UnknownServerException(), true);
        checkResponse(createDescribeTokenResponse(), 0, true);
        checkRequest(createExpireTokenRequest(), true);
        checkErrorResponse(createExpireTokenRequest(), new UnknownServerException(), true);
        checkResponse(createExpireTokenResponse(), 0, true);
        checkRequest(createRenewTokenRequest(), true);
        checkErrorResponse(createRenewTokenRequest(), new UnknownServerException(), true);
        checkResponse(createRenewTokenResponse(), 0, true);
        checkRequest(createElectLeadersRequest(), true);
        checkRequest(createElectLeadersRequestNullPartitions(), true);
        checkErrorResponse(createElectLeadersRequest(), new UnknownServerException(), true);
        checkResponse(createElectLeadersResponse(), 1, true);
        checkRequest(createIncrementalAlterConfigsRequest(), true);
        checkErrorResponse(createIncrementalAlterConfigsRequest(), new UnknownServerException(), true);
        checkResponse(createIncrementalAlterConfigsResponse(), 0, true);
        checkRequest(createAlterPartitionReassignmentsRequest(), true);
        checkErrorResponse(createAlterPartitionReassignmentsRequest(), new UnknownServerException(), true);
        checkResponse(createAlterPartitionReassignmentsResponse(), 0, true);
        checkRequest(createListPartitionReassignmentsRequest(), true);
        checkErrorResponse(createListPartitionReassignmentsRequest(), new UnknownServerException(), true);
        checkResponse(createListPartitionReassignmentsResponse(), 0, true);
        checkRequest(createOffsetDeleteRequest(), true);
        checkErrorResponse(createOffsetDeleteRequest(), new UnknownServerException(), true);
        checkResponse(createOffsetDeleteResponse(), 0, true);
        checkRequest(createAlterReplicaLogDirsRequest(), true);
        checkErrorResponse(createAlterReplicaLogDirsRequest(), new UnknownServerException(), true);
        checkResponse(createAlterReplicaLogDirsResponse(), 0, true);
    }

    @Test
    public void testResponseHeader() {
        ResponseHeader header = createResponseHeader((short) 1);
        ByteBuffer buffer = toBuffer(header.toStruct());
        ResponseHeader deserialized = ResponseHeader.parse(buffer, header.headerVersion());
        assertEquals(header.correlationId(), deserialized.correlationId());
    }

    private void checkOlderFetchVersions() throws Exception {
        int latestVersion = FETCH.latestVersion();
        for (int i = 0; i < latestVersion; ++i) {
            checkErrorResponse(createFetchRequest(i), new UnknownServerException(), true);
            checkRequest(createFetchRequest(i), true);
            checkResponse(createFetchResponse(), i, true);
        }
    }

    private void verifyDescribeConfigsResponse(DescribeConfigsResponse expected, DescribeConfigsResponse actual, int version) throws Exception {
        for (ConfigResource resource : expected.configs().keySet()) {
            Collection<DescribeConfigsResponse.ConfigEntry> deserializedEntries1 = actual.config(resource).entries();
            Iterator<DescribeConfigsResponse.ConfigEntry> expectedEntries = expected.config(resource).entries().iterator();
            for (DescribeConfigsResponse.ConfigEntry entry : deserializedEntries1) {
                DescribeConfigsResponse.ConfigEntry expectedEntry = expectedEntries.next();
                assertEquals(expectedEntry.name(), entry.name());
                assertEquals(expectedEntry.value(), entry.value());
                assertEquals(expectedEntry.isReadOnly(), entry.isReadOnly());
                assertEquals(expectedEntry.isSensitive(), entry.isSensitive());
                if (version < 3) {
                    assertEquals(ConfigType.UNKNOWN, entry.type());
                } else {
                    assertEquals(expectedEntry.type(), entry.type());
                }
                if (version == 1 || version == 3 || (expectedEntry.source() != DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_CONFIG &&
                        expectedEntry.source() != DescribeConfigsResponse.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))
                    assertEquals(expectedEntry.source(), entry.source());
                else
                    assertEquals(DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG, entry.source());
            }
        }
    }

    private void checkDescribeConfigsResponseVersions() throws Exception {
        DescribeConfigsResponse response = createDescribeConfigsResponse();
        DescribeConfigsResponse deserialized0 = (DescribeConfigsResponse) deserialize(response,
                response.toStruct((short) 0), (short) 0);
        verifyDescribeConfigsResponse(response, deserialized0, 0);

        DescribeConfigsResponse deserialized1 = (DescribeConfigsResponse) deserialize(response,
                response.toStruct((short) 1), (short) 1);
        verifyDescribeConfigsResponse(response, deserialized1, 1);

        DescribeConfigsResponse deserialized3 = (DescribeConfigsResponse) deserialize(response,
            response.toStruct((short) 3), (short) 3);
        verifyDescribeConfigsResponse(response, deserialized3, 3);
    }

    private void checkErrorResponse(AbstractRequest req, Throwable e, boolean checkEqualityAndHashCode) {
        checkResponse(req.getErrorResponse(e), req.version(), checkEqualityAndHashCode);
    }

    private void checkRequest(AbstractRequest req, boolean checkEqualityAndHashCode) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality and hashCode of the Struct only if indicated (it is likely to fail if any of the fields
        // in the request is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            Struct struct = req.toStruct();
            AbstractRequest deserialized = AbstractRequest.parseRequest(req.api, req.version(), struct);
            Struct struct2 = deserialized.toStruct();
            if (checkEqualityAndHashCode) {
                assertEquals(struct, struct2);
                assertEquals(struct.hashCode(), struct2.hashCode());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize request " + req + " with type " + req.getClass(), e);
        }
    }

    private void checkResponse(AbstractResponse response, int version, boolean checkEqualityAndHashCode) {
        // Check that we can serialize, deserialize and serialize again
        // Check for equality and hashCode of the Struct only if indicated (it is likely to fail if any of the fields
        // in the response is a HashMap with multiple elements since ordering of the elements may vary)
        try {
            Struct struct = response.toStruct((short) version);
            AbstractResponse deserialized = (AbstractResponse) deserialize(response, struct, (short) version);
            Struct struct2 = deserialized.toStruct((short) version);
            if (checkEqualityAndHashCode) {
                assertEquals(struct, struct2);
                assertEquals(struct.hashCode(), struct2.hashCode());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize response " + response + " with type " + response.getClass(), e);
        }
    }

    private AbstractRequestResponse deserialize(AbstractRequestResponse req, Struct struct, short version) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ByteBuffer buffer = toBuffer(struct);
        Method deserializer = req.getClass().getDeclaredMethod("parse", ByteBuffer.class, Short.TYPE);
        return (AbstractRequestResponse) deserializer.invoke(null, buffer, version);
    }

    @Test(expected = UnsupportedVersionException.class)
    public void cannotUseFindCoordinatorV0ToFindTransactionCoordinator() {
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKeyType(CoordinatorType.TRANSACTION.id)
                    .setKey("foobar"));
        builder.build((short) 0);
    }

    @Test
    public void produceRequestToStringTest() {
        ProduceRequest request = createProduceRequest(ApiKeys.PRODUCE.latestVersion());
        assertEquals(1, request.partitionRecordsOrFail().size());
        assertFalse(request.toString(false).contains("partitionSizes"));
        assertTrue(request.toString(false).contains("numPartitions=1"));
        assertTrue(request.toString(true).contains("partitionSizes"));
        assertFalse(request.toString(true).contains("numPartitions"));

        request.clearPartitionRecords();
        try {
            request.partitionRecordsOrFail();
            fail("partitionRecordsOrFail should fail after clearPartitionRecords()");
        } catch (IllegalStateException e) {
            // OK
        }

        // `toString` should behave the same after `clearPartitionRecords`
        assertFalse(request.toString(false).contains("partitionSizes"));
        assertTrue(request.toString(false).contains("numPartitions=1"));
        assertTrue(request.toString(true).contains("partitionSizes"));
        assertFalse(request.toString(true).contains("numPartitions"));
    }

    @Test
    public void produceRequestGetErrorResponseTest() {
        ProduceRequest request = createProduceRequest(ApiKeys.PRODUCE.latestVersion());
        Set<TopicPartition> partitions = new HashSet<>(request.partitionRecordsOrFail().keySet());

        ProduceResponse errorResponse = (ProduceResponse) request.getErrorResponse(new NotEnoughReplicasException());
        assertEquals(partitions, errorResponse.responses().keySet());
        ProduceResponse.PartitionResponse partitionResponse = errorResponse.responses().values().iterator().next();
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, partitionResponse.error);
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionResponse.baseOffset);
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionResponse.logAppendTime);

        request.clearPartitionRecords();

        // `getErrorResponse` should behave the same after `clearPartitionRecords`
        errorResponse = (ProduceResponse) request.getErrorResponse(new NotEnoughReplicasException());
        assertEquals(partitions, errorResponse.responses().keySet());
        partitionResponse = errorResponse.responses().values().iterator().next();
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, partitionResponse.error);
        assertEquals(ProduceResponse.INVALID_OFFSET, partitionResponse.baseOffset);
        assertEquals(RecordBatch.NO_TIMESTAMP, partitionResponse.logAppendTime);
    }

    @Test
    public void fetchResponseVersionTest() {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();

        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData<>(
                Errors.NONE, 1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET,
                0L, Optional.empty(), null, records));

        FetchResponse<MemoryRecords> v0Response = new FetchResponse<>(Errors.NONE, responseData, 0, INVALID_SESSION_ID);
        FetchResponse<MemoryRecords> v1Response = new FetchResponse<>(Errors.NONE, responseData, 10, INVALID_SESSION_ID);
        assertEquals("Throttle time must be zero", 0, v0Response.throttleTimeMs());
        assertEquals("Throttle time must be 10", 10, v1Response.throttleTimeMs());
        assertEquals("Should use schema version 0", FETCH.responseSchema((short) 0),
                v0Response.toStruct((short) 0).schema());
        assertEquals("Should use schema version 1", FETCH.responseSchema((short) 1),
                v1Response.toStruct((short) 1).schema());
        assertEquals("Response data does not match", responseData, v0Response.responseData());
        assertEquals("Response data does not match", responseData, v1Response.responseData());
    }

    @Test
    public void testFetchResponseV4() {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.allocate(10));

        List<FetchResponse.AbortedTransaction> abortedTransactions = asList(
                new FetchResponse.AbortedTransaction(10, 100),
                new FetchResponse.AbortedTransaction(15, 50)
        );
        responseData.put(new TopicPartition("bar", 0), new FetchResponse.PartitionData<>(Errors.NONE, 100000,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), abortedTransactions, records));
        responseData.put(new TopicPartition("bar", 1), new FetchResponse.PartitionData<>(Errors.NONE, 900000,
                5, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), null, records));
        responseData.put(new TopicPartition("foo", 0), new FetchResponse.PartitionData<>(Errors.NONE, 70000,
                6, FetchResponse.INVALID_LOG_START_OFFSET, Optional.empty(), emptyList(), records));

        FetchResponse<MemoryRecords> response = new FetchResponse<>(Errors.NONE, responseData, 10, INVALID_SESSION_ID);
        FetchResponse<MemoryRecords> deserialized = FetchResponse.parse(toBuffer(response.toStruct((short) 4)), (short) 4);
        assertEquals(responseData, deserialized.responseData());
    }

    @Test
    public void verifyFetchResponseFullWrites() throws Exception {
        verifyFetchResponseFullWrite(FETCH.latestVersion(), createFetchResponse(123));
        verifyFetchResponseFullWrite(FETCH.latestVersion(),
            createFetchResponse(Errors.FETCH_SESSION_ID_NOT_FOUND, 123));
        for (short version = 0; version <= FETCH.latestVersion(); version++) {
            verifyFetchResponseFullWrite(version, createFetchResponse());
        }
    }

    private void verifyFetchResponseFullWrite(short apiVersion, FetchResponse<MemoryRecords> fetchResponse) throws Exception {
        int correlationId = 15;

        short responseHeaderVersion = FETCH.responseHeaderVersion(apiVersion);
        Send send = fetchResponse.toSend("1", new ResponseHeader(correlationId, responseHeaderVersion), apiVersion);
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        send.writeTo(channel);
        channel.close();

        ByteBuffer buf = channel.buffer();

        // read the size
        int size = buf.getInt();
        assertTrue(size > 0);

        // read the header
        ResponseHeader responseHeader = ResponseHeader.parse(channel.buffer(), responseHeaderVersion);
        assertEquals(correlationId, responseHeader.correlationId());

        // read the body
        Struct responseBody = FETCH.responseSchema(apiVersion).read(buf);
        assertEquals(fetchResponse.toStruct(apiVersion), responseBody);

        assertEquals(size, responseHeader.sizeOf() + responseBody.sizeOf());
    }

    @Test
    public void testControlledShutdownResponse() {
        ControlledShutdownResponse response = createControlledShutdownResponse();
        short version = ApiKeys.CONTROLLED_SHUTDOWN.latestVersion();
        Struct struct = response.toStruct(version);
        ByteBuffer buffer = toBuffer(struct);
        ControlledShutdownResponse deserialized = ControlledShutdownResponse.parse(buffer, version);
        assertEquals(response.error(), deserialized.error());
        assertEquals(response.data().remainingPartitions(), deserialized.data().remainingPartitions());
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testCreateTopicRequestV0FailsIfValidateOnly() {
        createCreateTopicRequest(0, true);
    }

    @Test
    public void testCreateTopicRequestV3FailsIfNoPartitionsOrReplicas() {
        final UnsupportedVersionException exception = assertThrows(
            UnsupportedVersionException.class, () -> {
                CreateTopicsRequestData data = new CreateTopicsRequestData()
                    .setTimeoutMs(123)
                    .setValidateOnly(false);
                data.topics().add(new CreatableTopic().
                    setName("foo").
                    setNumPartitions(CreateTopicsRequest.NO_NUM_PARTITIONS).
                    setReplicationFactor((short) 1));
                data.topics().add(new CreatableTopic().
                    setName("bar").
                    setNumPartitions(1).
                    setReplicationFactor(CreateTopicsRequest.NO_REPLICATION_FACTOR));

                new Builder(data).build((short) 3);
            });
        assertTrue(exception.getMessage().contains("supported in CreateTopicRequest version 4+"));
        assertTrue(exception.getMessage().contains("[foo, bar]"));
    }

    @Test
    public void testFetchRequestMaxBytesOldVersions() throws Exception {
        final short version = 1;
        FetchRequest fr = createFetchRequest(version);
        FetchRequest fr2 = new FetchRequest(fr.toStruct(), version);
        assertEquals(fr2.maxBytes(), fr.maxBytes());
    }

    @Test
    public void testFetchRequestIsolationLevel() throws Exception {
        FetchRequest request = createFetchRequest(4, IsolationLevel.READ_COMMITTED);
        Struct struct = request.toStruct();
        FetchRequest deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());

        request = createFetchRequest(4, IsolationLevel.READ_UNCOMMITTED);
        struct = request.toStruct();
        deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());
    }

    @Test
    public void testFetchRequestWithMetadata() throws Exception {
        FetchRequest request = createFetchRequest(4, IsolationLevel.READ_COMMITTED);
        Struct struct = request.toStruct();
        FetchRequest deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());

        request = createFetchRequest(4, IsolationLevel.READ_UNCOMMITTED);
        struct = request.toStruct();
        deserialized = (FetchRequest) deserialize(request, struct, request.version());
        assertEquals(request.isolationLevel(), deserialized.isolationLevel());
    }

    @Test
    public void testJoinGroupRequestVersion0RebalanceTimeout() {
        final short version = 0;
        JoinGroupRequest jgr = createJoinGroupRequest(version);
        JoinGroupRequest jgr2 = new JoinGroupRequest(jgr.toStruct(), version);
        assertEquals(jgr2.data().rebalanceTimeoutMs(), jgr.data().rebalanceTimeoutMs());
    }

    @Test
    public void testOffsetFetchRequestBuilderToString() {
        List<Boolean> stableFlags = Arrays.asList(true, false);
        for (Boolean requireStable : stableFlags) {
            String allTopicPartitionsString = new OffsetFetchRequest.Builder("someGroup", requireStable, null, false).toString();

            assertTrue(allTopicPartitionsString.contains("groupId='someGroup', topics=null, requireStable="
                                                             + requireStable.toString()));
            String string = new OffsetFetchRequest.Builder("group1",
                requireStable, Collections.singletonList(new TopicPartition("test11", 1)), false).toString();
            assertTrue(string.contains("test11"));
            assertTrue(string.contains("group1"));
            assertTrue(string.contains("requireStable=" + requireStable.toString()));
        }
    }

    @Test
    public void testApiVersionsRequestBeforeV3Validation() {
        for (short version = 0; version < 3; version++) {
            ApiVersionsRequest request = new ApiVersionsRequest(new ApiVersionsRequestData(), version);
            assertTrue(request.isValid());
        }
    }

    @Test
    public void testValidApiVersionsRequest() {
        ApiVersionsRequest request;

        request = new ApiVersionsRequest.Builder().build();
        assertTrue(request.isValid());

        request = new ApiVersionsRequest(new ApiVersionsRequestData()
            .setClientSoftwareName("apache-kafka.java")
            .setClientSoftwareVersion("0.0.0-SNAPSHOT"),
            ApiKeys.API_VERSIONS.latestVersion()
        );
        assertTrue(request.isValid());
    }

    @Test(expected = UnsupportedVersionException.class)
    public void testListGroupRequestV3FailsWithStates() {
        ListGroupsRequestData data = new ListGroupsRequestData()
                .setStatesFilter(asList(ConsumerGroupState.STABLE.name()));
        new ListGroupsRequest.Builder(data).build((short) 3);
    }

    @Test
    public void testInvalidApiVersionsRequest() {
        testInvalidCase("java@apache_kafka", "0.0.0-SNAPSHOT");
        testInvalidCase("apache-kafka-java", "0.0.0@java");
        testInvalidCase("-apache-kafka-java", "0.0.0");
        testInvalidCase("apache-kafka-java.", "0.0.0");
    }

    private void testInvalidCase(String name, String version) {
        ApiVersionsRequest request = new ApiVersionsRequest(new ApiVersionsRequestData()
            .setClientSoftwareName(name)
            .setClientSoftwareVersion(version),
            ApiKeys.API_VERSIONS.latestVersion()
        );
        assertFalse(request.isValid());
    }

    @Test
    public void testApiVersionResponseWithUnsupportedError() {
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        ApiVersionsResponse response = request.getErrorResponse(0, Errors.UNSUPPORTED_VERSION.exception());

        assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.data.errorCode());

        ApiVersionsResponseKey apiVersion = response.data.apiKeys().find(ApiKeys.API_VERSIONS.id);
        assertNotNull(apiVersion);
        assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey());
        assertEquals(ApiKeys.API_VERSIONS.oldestVersion(), apiVersion.minVersion());
        assertEquals(ApiKeys.API_VERSIONS.latestVersion(), apiVersion.maxVersion());
    }

    @Test
    public void testApiVersionResponseWithNotUnsupportedError() {
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
        ApiVersionsResponse response = request.getErrorResponse(0, Errors.INVALID_REQUEST.exception());

        assertEquals(response.data.errorCode(), Errors.INVALID_REQUEST.code());
        assertTrue(response.data.apiKeys().isEmpty());
    }

    @Test
    public void testApiVersionResponseStructParsingFallback() {
        Struct struct = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.toStruct((short) 0);
        ApiVersionsResponse response = ApiVersionsResponse.
            fromStruct(struct, ApiKeys.API_VERSIONS.latestVersion());

        assertEquals(Errors.NONE.code(), response.data.errorCode());
    }

    @Test(expected = SchemaException.class)
    public void testApiVersionResponseStructParsingFallbackException() {
        short version = 0;
        ApiVersionsResponse.fromStruct(new Struct(ApiKeys.API_VERSIONS.requestSchema(version)), version);
    }

    @Test
    public void testApiVersionResponseStructParsing() {
        Struct struct = ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.toStruct(ApiKeys.API_VERSIONS.latestVersion());
        ApiVersionsResponse response = ApiVersionsResponse.
            fromStruct(struct, ApiKeys.API_VERSIONS.latestVersion());

        assertEquals(Errors.NONE.code(), response.data.errorCode());
    }

    @Test
    public void testInitProducerIdRequestVersions() {
        InitProducerIdRequest.Builder bld = new InitProducerIdRequest.Builder(
            new InitProducerIdRequestData().setTransactionTimeoutMs(1000).
                setTransactionalId("abracadabra").
                setProducerId(123));
        final UnsupportedVersionException exception = assertThrows(
            UnsupportedVersionException.class, () -> bld.build((short) 2).toStruct());
        assertTrue(exception.getMessage().contains("Attempted to write a non-default producerId at version 2"));
        bld.build((short) 3);
    }

    private ResponseHeader createResponseHeader(short headerVersion) {
        return new ResponseHeader(10, headerVersion);
    }

    private FindCoordinatorRequest createFindCoordinatorRequest(int version) {
        return new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKeyType(CoordinatorType.GROUP.id())
                    .setKey("test-group"))
                .build((short) version);
    }

    private FindCoordinatorResponse createFindCoordinatorResponse() {
        Node node = new Node(10, "host1", 2014);
        return FindCoordinatorResponse.prepareResponse(Errors.NONE, node);
    }

    private FetchRequest createFetchRequest(int version, FetchMetadata metadata, List<TopicPartition> toForget) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 0L,
                1000000, Optional.of(15)));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 0L,
                1000000, Optional.of(25)));
        return FetchRequest.Builder.forConsumer(100, 100000, fetchData).
            metadata(metadata).setMaxBytes(1000).toForget(toForget).build((short) version);
    }

    private FetchRequest createFetchRequest(int version, IsolationLevel isolationLevel) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 0L,
                1000000, Optional.of(15)));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 0L,
                1000000, Optional.of(25)));
        return FetchRequest.Builder.forConsumer(100, 100000, fetchData).
            isolationLevel(isolationLevel).setMaxBytes(1000).build((short) version);
    }

    private FetchRequest createFetchRequest(int version) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetchData = new LinkedHashMap<>();
        fetchData.put(new TopicPartition("test1", 0), new FetchRequest.PartitionData(100, 0L,
                1000000, Optional.of(15)));
        fetchData.put(new TopicPartition("test2", 0), new FetchRequest.PartitionData(200, 0L,
                1000000, Optional.of(25)));
        return FetchRequest.Builder.forConsumer(100, 100000, fetchData).setMaxBytes(1000).build((short) version);
    }

    private FetchResponse<MemoryRecords> createFetchResponse(Errors error, int sessionId) {
        return new FetchResponse<>(error, new LinkedHashMap<>(), 25, sessionId);
    }

    private FetchResponse<MemoryRecords> createFetchResponse(int sessionId) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData<>(Errors.NONE,
            1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), null, records));
        List<FetchResponse.AbortedTransaction> abortedTransactions = Collections.singletonList(
            new FetchResponse.AbortedTransaction(234L, 999L));
        responseData.put(new TopicPartition("test", 1), new FetchResponse.PartitionData<>(Errors.NONE,
            1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), abortedTransactions, MemoryRecords.EMPTY));
        return new FetchResponse<>(Errors.NONE, responseData, 25, sessionId);
    }

    private FetchResponse<MemoryRecords> createFetchResponse() {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("blah".getBytes()));
        responseData.put(new TopicPartition("test", 0), new FetchResponse.PartitionData<>(Errors.NONE,
                1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), null, records));

        List<FetchResponse.AbortedTransaction> abortedTransactions = Collections.singletonList(
                new FetchResponse.AbortedTransaction(234L, 999L));
        responseData.put(new TopicPartition("test", 1), new FetchResponse.PartitionData<>(Errors.NONE,
                1000000, FetchResponse.INVALID_LAST_STABLE_OFFSET, 0L, Optional.empty(), abortedTransactions, MemoryRecords.EMPTY));

        return new FetchResponse<>(Errors.NONE, responseData, 25, INVALID_SESSION_ID);
    }

    private HeartbeatRequest createHeartBeatRequest() {
        return new HeartbeatRequest.Builder(new HeartbeatRequestData()
                .setGroupId("group1")
                .setGenerationId(1)
                .setMemberId("consumer1")).build();
    }

    private HeartbeatResponse createHeartBeatResponse() {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(Errors.NONE.code()));
    }

    private JoinGroupRequest createJoinGroupRequest(int version) {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singleton(
                new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName("consumer-range")
                        .setMetadata(new byte[0])).iterator()
        );

        JoinGroupRequestData data = new JoinGroupRequestData()
            .setGroupId("group1")
            .setSessionTimeoutMs(30000)
            .setMemberId("consumer1")
            .setProtocolType("consumer")
            .setProtocols(protocols);

        // v1 and above contains rebalance timeout
        if (version >= 1)
            data.setRebalanceTimeoutMs(60000);

        // v5 and above could set group instance id
        if (version >= 5)
            data.setGroupInstanceId("groupInstanceId");

        return new JoinGroupRequest.Builder(data).build((short) version);
    }

    private JoinGroupResponse createJoinGroupResponse(int version) {
        List<JoinGroupResponseData.JoinGroupResponseMember> members = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            JoinGroupResponseMember member = new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("consumer" + i)
                .setMetadata(new byte[0]);

            if (version >= 5)
                member.setGroupInstanceId("instance" + i);

            members.add(member);
        }

        JoinGroupResponseData data = new JoinGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setGenerationId(1)
            .setProtocolType("consumer") // Added in v7 but ignorable
            .setProtocolName("range")
            .setLeader("leader")
            .setMemberId("consumer1")
            .setMembers(members);

        // v1 and above could set throttle time
        if (version >= 1)
            data.setThrottleTimeMs(1000);

        return new JoinGroupResponse(data);
    }

    private SyncGroupRequest createSyncGroupRequest(int version) {
        List<SyncGroupRequestAssignment> assignments = Collections.singletonList(
            new SyncGroupRequestAssignment()
                .setMemberId("member")
                .setAssignment(new byte[0])
        );

        SyncGroupRequestData data = new SyncGroupRequestData()
            .setGroupId("group1")
            .setGenerationId(1)
            .setMemberId("member")
            .setProtocolType("consumer") // Added in v5 but ignorable
            .setProtocolName("range")    // Added in v5 but ignorable
            .setAssignments(assignments);

        // v3 and above could set group instance id
        if (version >= 3)
            data.setGroupInstanceId("groupInstanceId");

        return new SyncGroupRequest.Builder(data).build((short) version);
    }

    private SyncGroupResponse createSyncGroupResponse(int version) {
        SyncGroupResponseData data = new SyncGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setProtocolType("consumer") // Added in v5 but ignorable
            .setProtocolName("range")    // Added in v5 but ignorable
            .setAssignment(new byte[0]);

        // v1 and above could set throttle time
        if (version >= 1)
            data.setThrottleTimeMs(1000);

        return new SyncGroupResponse(data);
    }

    private ListGroupsRequest createListGroupsRequest(short version) {
        ListGroupsRequestData data = new ListGroupsRequestData();
        if (version >= 4)
            data.setStatesFilter(Arrays.asList("Stable"));
        return new ListGroupsRequest.Builder(data).build(version);
    }

    private ListGroupsResponse createListGroupsResponse(int version) {
        ListGroupsResponseData.ListedGroup group = new ListGroupsResponseData.ListedGroup()
                .setGroupId("test-group")
                .setProtocolType("consumer");
        if (version >= 4)
            group.setGroupState("Stable");
        ListGroupsResponseData data = new ListGroupsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setGroups(Collections.singletonList(group));
        return new ListGroupsResponse(data);
    }

    private DescribeGroupsRequest createDescribeGroupRequest() {
        return new DescribeGroupsRequest.Builder(
            new DescribeGroupsRequestData().
                setGroups(Collections.singletonList("test-group"))).build();
    }

    private DescribeGroupsResponse createDescribeGroupResponse() {
        String clientId = "consumer-1";
        String clientHost = "localhost";
        DescribeGroupsResponseData describeGroupsResponseData = new DescribeGroupsResponseData();
        DescribeGroupsResponseData.DescribedGroupMember member = DescribeGroupsResponse.groupMember("memberId", null,
                clientId, clientHost, new byte[0], new byte[0]);
        DescribedGroup metadata = DescribeGroupsResponse.groupMetadata("test-group",
                Errors.NONE,
                "STABLE",
                "consumer",
                "roundrobin",
                Collections.singletonList(member),
                DescribeGroupsResponse.AUTHORIZED_OPERATIONS_OMITTED);
        describeGroupsResponseData.groups().add(metadata);
        return new DescribeGroupsResponse(describeGroupsResponseData);
    }

    private LeaveGroupRequest createLeaveGroupRequest() {
        return new LeaveGroupRequest.Builder(
            "group1", Collections.singletonList(new MemberIdentity()
                                                    .setMemberId("consumer1"))
            ).build();
    }

    private LeaveGroupResponse createLeaveGroupResponse() {
        return new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()));
    }

    private DeleteGroupsRequest createDeleteGroupsRequest() {
        return new DeleteGroupsRequest.Builder(
            new DeleteGroupsRequestData()
                .setGroupsNames(Collections.singletonList("test-group"))
        ).build();
    }

    private DeleteGroupsResponse createDeleteGroupsResponse() {
        DeletableGroupResultCollection result = new DeletableGroupResultCollection();
        result.add(new DeletableGroupResult()
                       .setGroupId("test-group")
                       .setErrorCode(Errors.NONE.code()));
        return new DeleteGroupsResponse(
            new DeleteGroupsResponseData()
                .setResults(result)
        );
    }

    private ListOffsetRequest createListOffsetRequest(int version) {
        if (version == 0) {
            Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0),
                    new ListOffsetRequest.PartitionData(1000000L, 10));
            return ListOffsetRequest.Builder
                    .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
                    .setTargetTimes(offsetData)
                    .build((short) version);
        } else if (version == 1) {
            Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0),
                    new ListOffsetRequest.PartitionData(1000000L, Optional.empty()));
            return ListOffsetRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                    .setTargetTimes(offsetData)
                    .build((short) version);
        } else if (version >= 2 && version <= 5) {
            Map<TopicPartition, ListOffsetRequest.PartitionData> offsetData = Collections.singletonMap(
                    new TopicPartition("test", 0),
                    new ListOffsetRequest.PartitionData(1000000L, Optional.of(5)));
            return ListOffsetRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_COMMITTED)
                    .setTargetTimes(offsetData)
                    .build((short) version);
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetRequest version " + version);
        }
    }

    private ListOffsetResponse createListOffsetResponse(int version) {
        if (version == 0) {
            Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(new TopicPartition("test", 0),
                    new ListOffsetResponse.PartitionData(Errors.NONE, asList(100L)));
            return new ListOffsetResponse(responseData);
        } else if (version >= 1 && version <= 5) {
            Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(new TopicPartition("test", 0),
                    new ListOffsetResponse.PartitionData(Errors.NONE, 10000L, 100L, Optional.of(27)));
            return new ListOffsetResponse(responseData);
        } else {
            throw new IllegalArgumentException("Illegal ListOffsetResponse version " + version);
        }
    }

    private MetadataRequest createMetadataRequest(int version, List<String> topics) {
        return new MetadataRequest.Builder(topics, true).build((short) version);
    }

    private MetadataResponse createMetadataResponse() {
        Node node = new Node(1, "host1", 1001);
        List<Integer> replicas = singletonList(node.id());
        List<Integer> isr = singletonList(node.id());
        List<Integer> offlineReplicas = emptyList();

        List<MetadataResponse.TopicMetadata> allTopicMetadata = new ArrayList<>();
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "__consumer_offsets", true,
                asList(new MetadataResponse.PartitionMetadata(Errors.NONE,
                        new TopicPartition("__consumer_offsets", 1),
                        Optional.of(node.id()), Optional.of(5), replicas, isr, offlineReplicas))));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, "topic2", false,
                emptyList()));
        allTopicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, "topic3", false,
            asList(new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE,
                    new TopicPartition("topic3", 0), Optional.empty(),
                    Optional.empty(), replicas, isr, offlineReplicas))));

        return MetadataResponse.prepareResponse(asList(node), null, MetadataResponse.NO_CONTROLLER_ID, allTopicMetadata);
    }

    private OffsetCommitRequest createOffsetCommitRequest(int version) {
        return new OffsetCommitRequest.Builder(new OffsetCommitRequestData()
                .setGroupId("group1")
                .setMemberId("consumer1")
                .setGroupInstanceId(null)
                .setGenerationId(100)
                .setTopics(Collections.singletonList(
                        new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                .setName("test")
                                .setPartitions(Arrays.asList(
                                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                                .setPartitionIndex(0)
                                                .setCommittedOffset(100)
                                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                .setCommittedMetadata(""),
                                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                                .setPartitionIndex(1)
                                                .setCommittedOffset(200)
                                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                .setCommittedMetadata(null)
                                ))
                ))
        ).build((short) version);
    }

    private OffsetCommitResponse createOffsetCommitResponse() {
        return new OffsetCommitResponse(new OffsetCommitResponseData()
                .setTopics(Collections.singletonList(
                        new OffsetCommitResponseData.OffsetCommitResponseTopic()
                                .setName("test")
                                .setPartitions(Collections.singletonList(
                                        new OffsetCommitResponseData.OffsetCommitResponsePartition()
                                                .setPartitionIndex(0)
                                                .setErrorCode(Errors.NONE.code())
                                ))
                ))
        );
    }

    private OffsetFetchRequest createOffsetFetchRequest(int version, boolean requireStable) {
        return new OffsetFetchRequest.Builder("group1", requireStable, Collections.singletonList(new TopicPartition("test11", 1)), false)
                .build((short) version);
    }

    private OffsetFetchRequest createOffsetFetchRequestForAllPartition(String groupId, boolean requireStable) {
        return new OffsetFetchRequest.Builder(groupId, requireStable, null, false).build();
    }

    private OffsetFetchResponse createOffsetFetchResponse() {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new OffsetFetchResponse.PartitionData(
                100L, Optional.empty(), "", Errors.NONE));
        responseData.put(new TopicPartition("test", 1), new OffsetFetchResponse.PartitionData(
                100L, Optional.of(10), null, Errors.NONE));
        return new OffsetFetchResponse(Errors.NONE, responseData);
    }

    private ProduceRequest createProduceRequest(int version) {
        if (version < 2)
            throw new IllegalArgumentException("Produce request version 2 is not supported");

        byte magic = version == 2 ? RecordBatch.MAGIC_VALUE_V1 : RecordBatch.MAGIC_VALUE_V2;
        MemoryRecords records = MemoryRecords.withRecords(magic, CompressionType.NONE, new SimpleRecord("woot".getBytes()));
        Map<TopicPartition, MemoryRecords> produceData = Collections.singletonMap(new TopicPartition("test", 0), records);
        return ProduceRequest.Builder.forMagic(magic, (short) 1, 5000, produceData, "transactionalId")
                .build((short) version);
    }

    private ProduceResponse createProduceResponse() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100));
        return new ProduceResponse(responseData, 0);
    }

    private ProduceResponse createProduceResponseWithErrorMessage() {
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseData = new HashMap<>();
        responseData.put(new TopicPartition("test", 0), new ProduceResponse.PartitionResponse(Errors.NONE,
                10000, RecordBatch.NO_TIMESTAMP, 100, Collections.singletonList(new ProduceResponse.RecordError(0, "error message")),
                "global error message"));
        return new ProduceResponse(responseData, 0);
    }

    private StopReplicaRequest createStopReplicaRequest(int version, boolean deletePartitions) {
        List<StopReplicaTopicState> topicStates = new ArrayList<>();
        StopReplicaTopicState topic1 = new StopReplicaTopicState()
            .setTopicName("topic1")
            .setPartitionStates(Collections.singletonList(new StopReplicaPartitionState()
                .setPartitionIndex(0)
                .setLeaderEpoch(1)
                .setDeletePartition(deletePartitions)));
        topicStates.add(topic1);
        StopReplicaTopicState topic2 = new StopReplicaTopicState()
            .setTopicName("topic2")
            .setPartitionStates(Collections.singletonList(new StopReplicaPartitionState()
                .setPartitionIndex(1)
                .setLeaderEpoch(2)
                .setDeletePartition(deletePartitions)));
        topicStates.add(topic2);

        return new StopReplicaRequest.Builder((short) version, 0, 1, 0,
            deletePartitions, topicStates).build((short) version);
    }

    private StopReplicaResponse createStopReplicaResponse() {
        List<StopReplicaResponseData.StopReplicaPartitionError> partitions = new ArrayList<>();
        partitions.add(new StopReplicaResponseData.StopReplicaPartitionError()
            .setTopicName("test")
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code()));
        return new StopReplicaResponse(new StopReplicaResponseData()
            .setErrorCode(Errors.NONE.code())
            .setPartitionErrors(partitions));
    }

    private ControlledShutdownRequest createControlledShutdownRequest() {
        ControlledShutdownRequestData data = new ControlledShutdownRequestData()
                .setBrokerId(10)
                .setBrokerEpoch(0L);
        return new ControlledShutdownRequest.Builder(
                data,
                ApiKeys.CONTROLLED_SHUTDOWN.latestVersion()).build();
    }

    private ControlledShutdownRequest createControlledShutdownRequest(int version) {
        ControlledShutdownRequestData data = new ControlledShutdownRequestData()
                .setBrokerId(10)
                .setBrokerEpoch(0L);
        return new ControlledShutdownRequest.Builder(
                data,
                ApiKeys.CONTROLLED_SHUTDOWN.latestVersion()).build((short) version);
    }

    private ControlledShutdownResponse createControlledShutdownResponse() {
        RemainingPartition p1 = new RemainingPartition()
                .setTopicName("test2")
                .setPartitionIndex(5);
        RemainingPartition p2 = new RemainingPartition()
                .setTopicName("test1")
                .setPartitionIndex(10);
        RemainingPartitionCollection pSet = new RemainingPartitionCollection();
        pSet.add(p1);
        pSet.add(p2);
        ControlledShutdownResponseData data = new ControlledShutdownResponseData()
                .setErrorCode(Errors.NONE.code())
                .setRemainingPartitions(pSet);
        return new ControlledShutdownResponse(data);
    }

    private LeaderAndIsrRequest createLeaderAndIsrRequest(int version) {
        List<LeaderAndIsrPartitionState> partitionStates = new ArrayList<>();
        List<Integer> isr = asList(1, 2);
        List<Integer> replicas = asList(1, 2, 3, 4);
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("topic5")
            .setPartitionIndex(105)
            .setControllerEpoch(0)
            .setLeader(2)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setIsNew(false));
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("topic5")
            .setPartitionIndex(1)
            .setControllerEpoch(1)
            .setLeader(1)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setIsNew(false));
        partitionStates.add(new LeaderAndIsrPartitionState()
            .setTopicName("topic20")
            .setPartitionIndex(1)
            .setControllerEpoch(1)
            .setLeader(0)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setIsNew(false));

        Set<Node> leaders = Utils.mkSet(
                new Node(0, "test0", 1223),
                new Node(1, "test1", 1223)
        );
        return new LeaderAndIsrRequest.Builder((short) version, 1, 10, 0, partitionStates, leaders).build();
    }

    private LeaderAndIsrResponse createLeaderAndIsrResponse() {
        List<LeaderAndIsrResponseData.LeaderAndIsrPartitionError> partitions = new ArrayList<>();
        partitions.add(new LeaderAndIsrResponseData.LeaderAndIsrPartitionError()
            .setTopicName("test")
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code()));
        return new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
            .setErrorCode(Errors.NONE.code())
            .setPartitionErrors(partitions));
    }

    private UpdateMetadataRequest createUpdateMetadataRequest(int version, String rack) {
        List<UpdateMetadataPartitionState> partitionStates = new ArrayList<>();
        List<Integer> isr = asList(1, 2);
        List<Integer> replicas = asList(1, 2, 3, 4);
        List<Integer> offlineReplicas = asList();
        partitionStates.add(new UpdateMetadataPartitionState()
            .setTopicName("topic5")
            .setPartitionIndex(105)
            .setControllerEpoch(0)
            .setLeader(2)
            .setLeaderEpoch(1)
            .setIsr(isr)
            .setZkVersion(2)
            .setReplicas(replicas)
            .setOfflineReplicas(offlineReplicas));
        partitionStates.add(new UpdateMetadataPartitionState()
                .setTopicName("topic5")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(1)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setZkVersion(2)
                .setReplicas(replicas)
                .setOfflineReplicas(offlineReplicas));
        partitionStates.add(new UpdateMetadataPartitionState()
                .setTopicName("topic20")
                .setPartitionIndex(1)
                .setControllerEpoch(1)
                .setLeader(0)
                .setLeaderEpoch(1)
                .setIsr(isr)
                .setZkVersion(2)
                .setReplicas(replicas)
                .setOfflineReplicas(offlineReplicas));

        SecurityProtocol plaintext = SecurityProtocol.PLAINTEXT;
        List<UpdateMetadataEndpoint> endpoints1 = new ArrayList<>();
        endpoints1.add(new UpdateMetadataEndpoint()
            .setHost("host1")
            .setPort(1223)
            .setSecurityProtocol(plaintext.id)
            .setListener(ListenerName.forSecurityProtocol(plaintext).value()));

        List<UpdateMetadataEndpoint> endpoints2 = new ArrayList<>();
        endpoints2.add(new UpdateMetadataEndpoint()
            .setHost("host1")
            .setPort(1244)
            .setSecurityProtocol(plaintext.id)
            .setListener(ListenerName.forSecurityProtocol(plaintext).value()));
        if (version > 0) {
            SecurityProtocol ssl = SecurityProtocol.SSL;
            endpoints2.add(new UpdateMetadataEndpoint()
                .setHost("host2")
                .setPort(1234)
                .setSecurityProtocol(ssl.id)
                .setListener(ListenerName.forSecurityProtocol(ssl).value()));
            endpoints2.add(new UpdateMetadataEndpoint()
                .setHost("host2")
                .setPort(1334)
                .setSecurityProtocol(ssl.id));
            if (version >= 3)
                endpoints2.get(1).setListener("CLIENT");
        }

        List<UpdateMetadataBroker> liveBrokers = Arrays.asList(
            new UpdateMetadataBroker()
                .setId(0)
                .setEndpoints(endpoints1)
                .setRack(rack),
            new UpdateMetadataBroker()
                .setId(1)
                .setEndpoints(endpoints2)
                .setRack(rack)
        );
        return new UpdateMetadataRequest.Builder((short) version, 1, 10, 0, partitionStates,
            liveBrokers).build();
    }

    private UpdateMetadataResponse createUpdateMetadataResponse() {
        return new UpdateMetadataResponse(new UpdateMetadataResponseData().setErrorCode(Errors.NONE.code()));
    }

    private SaslHandshakeRequest createSaslHandshakeRequest() {
        return new SaslHandshakeRequest.Builder(
                new SaslHandshakeRequestData().setMechanism("PLAIN")).build();
    }

    private SaslHandshakeResponse createSaslHandshakeResponse() {
        return new SaslHandshakeResponse(
                new SaslHandshakeResponseData()
                .setErrorCode(Errors.NONE.code()).setMechanisms(Collections.singletonList("GSSAPI")));
    }

    private SaslAuthenticateRequest createSaslAuthenticateRequest() {
        SaslAuthenticateRequestData data = new SaslAuthenticateRequestData().setAuthBytes(new byte[0]);
        return new SaslAuthenticateRequest(data);
    }

    private SaslAuthenticateResponse createSaslAuthenticateResponse() {
        SaslAuthenticateResponseData data = new SaslAuthenticateResponseData()
                .setErrorCode(Errors.NONE.code())
                .setAuthBytes(new byte[0])
                .setSessionLifetimeMs(Long.MAX_VALUE);
        return new SaslAuthenticateResponse(data);
    }

    private ApiVersionsRequest createApiVersionRequest() {
        return new ApiVersionsRequest.Builder().build();
    }

    private ApiVersionsResponse createApiVersionResponse() {
        ApiVersionsResponseKeyCollection apiVersions = new ApiVersionsResponseKeyCollection();
        apiVersions.add(new ApiVersionsResponseKey()
            .setApiKey((short) 0)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 2));

        return new ApiVersionsResponse(new ApiVersionsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0)
            .setApiKeys(apiVersions));
    }

    private CreateTopicsRequest createCreateTopicRequest(int version) {
        return createCreateTopicRequest(version, version >= 1);
    }

    private CreateTopicsRequest createCreateTopicRequest(int version, boolean validateOnly) {
        CreateTopicsRequestData data = new CreateTopicsRequestData().
            setTimeoutMs(123).
            setValidateOnly(validateOnly);
        data.topics().add(new CreatableTopic().
            setNumPartitions(3).
            setReplicationFactor((short) 5));

        CreatableTopic topic2 = new CreatableTopic();
        data.topics().add(topic2);
        topic2.assignments().add(new CreatableReplicaAssignment().
            setPartitionIndex(0).
            setBrokerIds(Arrays.asList(1, 2, 3)));
        topic2.assignments().add(new CreatableReplicaAssignment().
            setPartitionIndex(1).
            setBrokerIds(Arrays.asList(2, 3, 4)));
        topic2.configs().add(new CreateableTopicConfig().
            setName("config1").setValue("value1"));

        return new CreateTopicsRequest.Builder(data).build((short) version);
    }

    private CreateTopicsResponse createCreateTopicResponse() {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreatableTopicResult()
            .setName("t1")
            .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
            .setErrorMessage(null));
        data.topics().add(new CreatableTopicResult()
            .setName("t2")
            .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
            .setErrorMessage("Leader with id 5 is not available."));
        data.topics().add(new CreatableTopicResult()
            .setName("t3")
            .setErrorCode(Errors.NONE.code())
            .setNumPartitions(1)
            .setReplicationFactor((short) 2)
            .setConfigs(Collections.singletonList(new CreatableTopicConfigs()
                .setName("min.insync.replicas")
                .setValue("2"))));
        return new CreateTopicsResponse(data);
    }

    private DeleteTopicsRequest createDeleteTopicsRequest() {
        return new DeleteTopicsRequest.Builder(
                new DeleteTopicsRequestData()
                .setTopicNames(Arrays.asList("my_t1", "my_t2"))
                .setTimeoutMs(1000)).build();
    }

    private DeleteTopicsResponse createDeleteTopicsResponse() {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
                .setName("t1")
                .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code()));
        data.responses().add(new DeletableTopicResult()
                .setName("t2")
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()));
        return new DeleteTopicsResponse(data);
    }

    private InitProducerIdRequest createInitPidRequest() {
        InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                .setTransactionalId(null)
                .setTransactionTimeoutMs(100);
        return new InitProducerIdRequest.Builder(requestData).build();
    }

    private InitProducerIdResponse createInitPidResponse() {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(Errors.NONE.code())
                .setProducerEpoch((short) 3)
                .setProducerId(3332)
                .setThrottleTimeMs(0);
        return new InitProducerIdResponse(responseData);
    }

    private Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> createOffsetForLeaderEpochPartitionData() {
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> epochs = new HashMap<>();
        epochs.put(new TopicPartition("topic1", 0),
                new OffsetsForLeaderEpochRequest.PartitionData(Optional.of(0), 1));
        epochs.put(new TopicPartition("topic1", 1),
                new OffsetsForLeaderEpochRequest.PartitionData(Optional.of(0), 1));
        epochs.put(new TopicPartition("topic2", 2),
                new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), 3));
        return epochs;
    }

    private OffsetsForLeaderEpochRequest createLeaderEpochRequestForConsumer() {
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> epochs = createOffsetForLeaderEpochPartitionData();
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build();
    }

    private OffsetsForLeaderEpochRequest createLeaderEpochRequestForReplica(int version, int replicaId) {
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> epochs = createOffsetForLeaderEpochPartitionData();
        return OffsetsForLeaderEpochRequest.Builder.forFollower((short) version, epochs, replicaId).build();
    }

    private OffsetsForLeaderEpochResponse createLeaderEpochResponse() {
        Map<TopicPartition, EpochEndOffset> epochs = new HashMap<>();

        epochs.put(new TopicPartition("topic1", 0), new EpochEndOffset(Errors.NONE, 1, 0));
        epochs.put(new TopicPartition("topic1", 1), new EpochEndOffset(Errors.NONE, 1, 1));
        epochs.put(new TopicPartition("topic2", 2), new EpochEndOffset(Errors.NONE, 1, 2));

        return new OffsetsForLeaderEpochResponse(0, epochs);
    }

    private AddPartitionsToTxnRequest createAddPartitionsToTxnRequest() {
        return new AddPartitionsToTxnRequest.Builder("tid", 21L, (short) 42,
            Collections.singletonList(new TopicPartition("topic", 73))).build();
    }

    private AddPartitionsToTxnResponse createAddPartitionsToTxnResponse() {
        return new AddPartitionsToTxnResponse(0, Collections.singletonMap(new TopicPartition("t", 0), Errors.NONE));
    }

    private AddOffsetsToTxnRequest createAddOffsetsToTxnRequest() {
        return new AddOffsetsToTxnRequest.Builder(
            new AddOffsetsToTxnRequestData()
                .setTransactionalId("tid")
                .setProducerId(21L)
                .setProducerEpoch((short) 42)
                .setGroupId("gid")
        ).build();
    }

    private AddOffsetsToTxnResponse createAddOffsetsToTxnResponse() {
        return new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
                                               .setErrorCode(Errors.NONE.code())
                                               .setThrottleTimeMs(0));
    }

    private EndTxnRequest createEndTxnRequest() {
        return new EndTxnRequest.Builder(
            new EndTxnRequestData()
                .setTransactionalId("tid")
                .setProducerId(21L)
                .setProducerEpoch((short) 42)
                .setCommitted(TransactionResult.COMMIT.id)
            ).build();
    }

    private EndTxnResponse createEndTxnResponse() {
        return new EndTxnResponse(
            new EndTxnResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(0)
        );
    }

    private WriteTxnMarkersRequest createWriteTxnMarkersRequest() {
        return new WriteTxnMarkersRequest.Builder(
            Collections.singletonList(new WriteTxnMarkersRequest.TxnMarkerEntry(21L, (short) 42, 73, TransactionResult.ABORT,
                                                                                Collections.singletonList(new TopicPartition("topic", 73))))).build();
    }

    private WriteTxnMarkersResponse createWriteTxnMarkersResponse() {
        final Map<TopicPartition, Errors> errorPerPartitions = new HashMap<>();
        errorPerPartitions.put(new TopicPartition("topic", 73), Errors.NONE);
        final Map<Long, Map<TopicPartition, Errors>> response = new HashMap<>();
        response.put(21L, errorPerPartitions);
        return new WriteTxnMarkersResponse(response);
    }

    private TxnOffsetCommitRequest createTxnOffsetCommitRequest(int version) {
        final Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic", 73),
                    new TxnOffsetCommitRequest.CommittedOffset(100, null, Optional.empty()));
        offsets.put(new TopicPartition("topic", 74),
                new TxnOffsetCommitRequest.CommittedOffset(100, "blah", Optional.of(27)));

        if (version < 3) {
            return new TxnOffsetCommitRequest.Builder("transactionalId",
                "groupId",
                21L,
                (short) 42,
                offsets,
                false).build();
        } else {
            return new TxnOffsetCommitRequest.Builder("transactionalId",
                "groupId",
                21L,
                (short) 42,
                offsets,
                "member",
                2,
                Optional.of("instance"),
                false).build();
        }
    }

    private TxnOffsetCommitRequest createTxnOffsetCommitRequestWithAutoDowngrade(int version) {
        final Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic", 73),
            new TxnOffsetCommitRequest.CommittedOffset(100, null, Optional.empty()));
        offsets.put(new TopicPartition("topic", 74),
            new TxnOffsetCommitRequest.CommittedOffset(100, "blah", Optional.of(27)));

        return new TxnOffsetCommitRequest.Builder("transactionalId",
            "groupId",
            21L,
            (short) 42,
            offsets,
            "member",
            2,
            Optional.of("instance"),
            true).build();
    }

    private TxnOffsetCommitResponse createTxnOffsetCommitResponse() {
        final Map<TopicPartition, Errors> errorPerPartitions = new HashMap<>();
        errorPerPartitions.put(new TopicPartition("topic", 73), Errors.NONE);
        return new TxnOffsetCommitResponse(0, errorPerPartitions);
    }

    private DescribeAclsRequest createDescribeAclsRequest() {
        return new DescribeAclsRequest.Builder(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
                new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY))).build();
    }

    private DescribeAclsResponse createDescribeAclsResponse() {
        DescribeAclsResponseData data = new DescribeAclsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(Errors.NONE.message())
                .setThrottleTimeMs(0)
                .setResources(Collections.singletonList(new DescribeAclsResource()
                        .setResourceType(ResourceType.TOPIC.code())
                        .setResourceName("mytopic")
                        .setPatternType(PatternType.LITERAL.code())
                        .setAcls(Collections.singletonList(new AclDescription()
                                .setHost("*")
                                .setOperation(AclOperation.WRITE.code())
                                .setPermissionType(AclPermissionType.ALLOW.code())
                                .setPrincipal("User:ANONYMOUS")))));
        return new DescribeAclsResponse(data);
    }

    private CreateAclsRequest createCreateAclsRequest() {
        List<CreateAclsRequestData.AclCreation> creations = new ArrayList<>();
        creations.add(CreateAclsRequest.aclCreation(new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "mytopic", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.ALLOW))));
        creations.add(CreateAclsRequest.aclCreation(new AclBinding(
            new ResourcePattern(ResourceType.GROUP, "mygroup", PatternType.LITERAL),
            new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.DENY))));
        CreateAclsRequestData data = new CreateAclsRequestData().setCreations(creations);
        return new CreateAclsRequest.Builder(data).build();
    }

    private CreateAclsResponse createCreateAclsResponse() {
        return new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
            new CreateAclsResponseData.AclCreationResult(),
            new CreateAclsResponseData.AclCreationResult()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("Foo bar"))));
    }

    private DeleteAclsRequest createDeleteAclsRequest() {
        DeleteAclsRequestData data = new DeleteAclsRequestData().setFilters(asList(
            new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.ANY.code())
                .setResourceNameFilter(null)
                .setPatternTypeFilter(PatternType.LITERAL.code())
                .setPrincipalFilter("User:ANONYMOUS")
                .setHostFilter(null)
                .setOperation(AclOperation.ANY.code())
                .setPermissionType(AclPermissionType.ANY.code()),
            new DeleteAclsRequestData.DeleteAclsFilter()
                .setResourceTypeFilter(ResourceType.ANY.code())
                .setResourceNameFilter(null)
                .setPatternTypeFilter(PatternType.LITERAL.code())
                .setPrincipalFilter("User:bob")
                .setHostFilter(null)
                .setOperation(AclOperation.ANY.code())
                .setPermissionType(AclPermissionType.ANY.code())
        ));
        return new DeleteAclsRequest.Builder(data).build();
    }

    private DeleteAclsResponse createDeleteAclsResponse() {
        List<DeleteAclsResponseData.DeleteAclsFilterResult> filterResults = new ArrayList<>();
        filterResults.add(new DeleteAclsResponseData.DeleteAclsFilterResult().setMatchingAcls(asList(
                new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                    .setResourceType(ResourceType.TOPIC.code())
                    .setResourceName("mytopic3")
                    .setPatternType(PatternType.LITERAL.code())
                    .setPrincipal("User:ANONYMOUS")
                    .setHost("*")
                    .setOperation(AclOperation.DESCRIBE.code())
                    .setPermissionType(AclPermissionType.ALLOW.code()),
                new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                    .setResourceType(ResourceType.TOPIC.code())
                    .setResourceName("mytopic4")
                    .setPatternType(PatternType.LITERAL.code())
                    .setPrincipal("User:ANONYMOUS")
                    .setHost("*")
                    .setOperation(AclOperation.DESCRIBE.code())
                    .setPermissionType(AclPermissionType.DENY.code()))));
        filterResults.add(new DeleteAclsResponseData.DeleteAclsFilterResult()
            .setErrorCode(Errors.SECURITY_DISABLED.code())
            .setErrorMessage("No security"));
        return new DeleteAclsResponse(new DeleteAclsResponseData()
            .setThrottleTimeMs(0)
            .setFilterResults(filterResults));
    }

    private DescribeConfigsRequest createDescribeConfigsRequest(int version) {
        return new DescribeConfigsRequest.Builder(asList(
                new ConfigResource(ConfigResource.Type.BROKER, "0"),
                new ConfigResource(ConfigResource.Type.TOPIC, "topic")))
                .build((short) version);
    }

    private DescribeConfigsRequest createDescribeConfigsRequestWithConfigEntries(int version) {
        Map<ConfigResource, Collection<String>> resources = new HashMap<>();
        resources.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), asList("foo", "bar"));
        resources.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"), null);
        resources.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic a"), emptyList());
        return new DescribeConfigsRequest.Builder(resources).build((short) version);
    }

    private DescribeConfigsRequest createDescribeConfigsRequestWithDocumentation(int version) {
        Map<ConfigResource, Collection<String>> resources = new HashMap<>();
        resources.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), asList("foo", "bar"));
        return new DescribeConfigsRequest.Builder(resources).includeDocumentation(true).build((short) version);
    }

    private DescribeConfigsResponse createDescribeConfigsResponse() {
        Map<ConfigResource, DescribeConfigsResponse.Config> configs = new HashMap<>();
        List<DescribeConfigsResponse.ConfigSynonym> synonyms = emptyList();
        List<DescribeConfigsResponse.ConfigEntry> configEntries = asList(
                new DescribeConfigsResponse.ConfigEntry("config_name", "config_value",
                        DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_CONFIG, true, false, synonyms),
                new DescribeConfigsResponse.ConfigEntry("another_name", "another value",
                        DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG, false, true, synonyms),
                new DescribeConfigsResponse.ConfigEntry("yet_another_name", "yet another value",
                        DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG, false, true, synonyms,
                            ConfigType.BOOLEAN, "some description")
        );
        configs.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), new DescribeConfigsResponse.Config(
                ApiError.NONE, configEntries));
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"), new DescribeConfigsResponse.Config(
                ApiError.NONE, Collections.<DescribeConfigsResponse.ConfigEntry>emptyList()));
        return new DescribeConfigsResponse(200, configs);
    }

    private AlterConfigsRequest createAlterConfigsRequest() {
        Map<ConfigResource, AlterConfigsRequest.Config> configs = new HashMap<>();
        List<AlterConfigsRequest.ConfigEntry> configEntries = asList(
                new AlterConfigsRequest.ConfigEntry("config_name", "config_value"),
                new AlterConfigsRequest.ConfigEntry("another_name", "another value")
        );
        configs.put(new ConfigResource(ConfigResource.Type.BROKER, "0"), new AlterConfigsRequest.Config(configEntries));
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, "topic"),
                new AlterConfigsRequest.Config(Collections.<AlterConfigsRequest.ConfigEntry>emptyList()));
        return new AlterConfigsRequest.Builder(configs, false).build((short) 0);
    }

    private AlterConfigsResponse createAlterConfigsResponse() {
        AlterConfigsResponseData data = new AlterConfigsResponseData()
                .setThrottleTimeMs(20);
        data.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(null)
                .setResourceName("0")
                .setResourceType(ConfigResource.Type.BROKER.id()));
        data.responses().add(new AlterConfigsResponseData.AlterConfigsResourceResponse()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("This request is invalid")
                .setResourceName("topic")
                .setResourceType(ConfigResource.Type.TOPIC.id()));
        return new AlterConfigsResponse(data);
    }

    private CreatePartitionsRequest createCreatePartitionsRequest() {
        List<CreatePartitionsTopic> topics = new LinkedList<>();
        topics.add(new CreatePartitionsTopic()
                .setName("my_topic")
                .setCount(3)
        );
        topics.add(new CreatePartitionsTopic()
                .setName("my_other_topic")
                .setCount(3)
        );

        CreatePartitionsRequestData data = new CreatePartitionsRequestData()
                .setTimeoutMs(0)
                .setValidateOnly(false)
                .setTopics(topics);
        return new CreatePartitionsRequest(data, (short) 0);
    }

    private CreatePartitionsRequest createCreatePartitionsRequestWithAssignments() {
        List<CreatePartitionsTopic> topics = new LinkedList<>();
        CreatePartitionsAssignment myTopicAssignment = new CreatePartitionsAssignment()
                .setBrokerIds(Collections.singletonList(2));
        topics.add(new CreatePartitionsTopic()
                .setName("my_topic")
                .setCount(3)
                .setAssignments(Collections.singletonList(myTopicAssignment))
        );

        topics.add(new CreatePartitionsTopic()
                .setName("my_other_topic")
                .setCount(3)
                .setAssignments(asList(
                    new CreatePartitionsAssignment().setBrokerIds(asList(2, 3)),
                    new CreatePartitionsAssignment().setBrokerIds(asList(3, 1))
                ))
        );

        CreatePartitionsRequestData data = new CreatePartitionsRequestData()
                .setTimeoutMs(0)
                .setValidateOnly(false)
                .setTopics(topics);
        return new CreatePartitionsRequest(data, (short) 0);
    }

    private CreatePartitionsResponse createCreatePartitionsResponse() {
        List<CreatePartitionsTopicResult> results = new LinkedList<>();
        results.add(new CreatePartitionsTopicResult()
                .setName("my_topic")
                .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code()));
        results.add(new CreatePartitionsTopicResult()
                .setName("my_topic")
                .setErrorCode(Errors.NONE.code()));
        CreatePartitionsResponseData data = new CreatePartitionsResponseData()
                .setThrottleTimeMs(42)
                .setResults(results);
        return new CreatePartitionsResponse(data);
    }

    private CreateDelegationTokenRequest createCreateTokenRequest() {
        List<CreatableRenewers> renewers = new ArrayList<>();
        renewers.add(new CreatableRenewers()
                .setPrincipalType("User")
                .setPrincipalName("user1"));
        renewers.add(new CreatableRenewers()
                .setPrincipalType("User")
                .setPrincipalName("user2"));
        return new CreateDelegationTokenRequest.Builder(new CreateDelegationTokenRequestData()
                .setRenewers(renewers)
                .setMaxLifetimeMs(System.currentTimeMillis())).build();
    }

    private CreateDelegationTokenResponse createCreateTokenResponse() {
        CreateDelegationTokenResponseData data = new CreateDelegationTokenResponseData()
                .setThrottleTimeMs(20)
                .setErrorCode(Errors.NONE.code())
                .setPrincipalType("User")
                .setPrincipalName("user1")
                .setIssueTimestampMs(System.currentTimeMillis())
                .setExpiryTimestampMs(System.currentTimeMillis())
                .setMaxTimestampMs(System.currentTimeMillis())
                .setTokenId("token1")
                .setHmac("test".getBytes());
        return new CreateDelegationTokenResponse(data);
    }

    private RenewDelegationTokenRequest createRenewTokenRequest() {
        RenewDelegationTokenRequestData data = new RenewDelegationTokenRequestData()
                .setHmac("test".getBytes())
                .setRenewPeriodMs(System.currentTimeMillis());
        return new RenewDelegationTokenRequest.Builder(data).build();
    }

    private RenewDelegationTokenResponse createRenewTokenResponse() {
        RenewDelegationTokenResponseData data = new RenewDelegationTokenResponseData()
                .setThrottleTimeMs(20)
                .setErrorCode(Errors.NONE.code())
                .setExpiryTimestampMs(System.currentTimeMillis());
        return new RenewDelegationTokenResponse(data);
    }

    private ExpireDelegationTokenRequest createExpireTokenRequest() {
        ExpireDelegationTokenRequestData data = new ExpireDelegationTokenRequestData()
                .setHmac("test".getBytes())
                .setExpiryTimePeriodMs(System.currentTimeMillis());
        return new ExpireDelegationTokenRequest.Builder(data).build();
    }

    private ExpireDelegationTokenResponse createExpireTokenResponse() {
        ExpireDelegationTokenResponseData data = new ExpireDelegationTokenResponseData()
                .setThrottleTimeMs(20)
                .setErrorCode(Errors.NONE.code())
                .setExpiryTimestampMs(System.currentTimeMillis());
        return new ExpireDelegationTokenResponse(data);
    }

    private DescribeDelegationTokenRequest createDescribeTokenRequest() {
        List<KafkaPrincipal> owners = new ArrayList<>();
        owners.add(SecurityUtils.parseKafkaPrincipal("User:user1"));
        owners.add(SecurityUtils.parseKafkaPrincipal("User:user2"));
        return new DescribeDelegationTokenRequest.Builder(owners).build();
    }

    private DescribeDelegationTokenResponse createDescribeTokenResponse() {
        List<KafkaPrincipal> renewers = new ArrayList<>();
        renewers.add(SecurityUtils.parseKafkaPrincipal("User:user1"));
        renewers.add(SecurityUtils.parseKafkaPrincipal("User:user2"));

        List<DelegationToken> tokenList = new LinkedList<>();

        TokenInformation tokenInfo1 = new TokenInformation("1", SecurityUtils.parseKafkaPrincipal("User:owner"), renewers,
            System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis());

        TokenInformation tokenInfo2 = new TokenInformation("2", SecurityUtils.parseKafkaPrincipal("User:owner1"), renewers,
            System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis());

        tokenList.add(new DelegationToken(tokenInfo1, "test".getBytes()));
        tokenList.add(new DelegationToken(tokenInfo2, "test".getBytes()));

        return new DescribeDelegationTokenResponse(20, Errors.NONE, tokenList);
    }

    private ElectLeadersRequest createElectLeadersRequestNullPartitions() {
        return new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 100).build((short) 1);
    }

    private ElectLeadersRequest createElectLeadersRequest() {
        List<TopicPartition> partitions = asList(new TopicPartition("data", 1), new TopicPartition("data", 2));

        return new ElectLeadersRequest.Builder(ElectionType.PREFERRED, partitions, 100).build((short) 1);
    }

    private ElectLeadersResponse createElectLeadersResponse() {
        String topic = "myTopic";
        List<ReplicaElectionResult> electionResults = new ArrayList<>();
        ReplicaElectionResult electionResult = new ReplicaElectionResult();
        electionResult.setTopic(topic);
        // Add partition 1 result
        PartitionResult partitionResult = new PartitionResult();
        partitionResult.setPartitionId(0);
        partitionResult.setErrorCode(ApiError.NONE.error().code());
        partitionResult.setErrorMessage(ApiError.NONE.message());
        electionResult.partitionResult().add(partitionResult);

        // Add partition 2 result
        partitionResult = new PartitionResult();
        partitionResult.setPartitionId(1);
        partitionResult.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        partitionResult.setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        electionResult.partitionResult().add(partitionResult);

        return new ElectLeadersResponse(200, Errors.NONE.code(), electionResults);
    }

    private IncrementalAlterConfigsRequest createIncrementalAlterConfigsRequest() {
        IncrementalAlterConfigsRequestData data = new IncrementalAlterConfigsRequestData();
        AlterableConfig alterableConfig = new AlterableConfig()
                .setName("retention.ms")
                .setConfigOperation((byte) 0)
                .setValue("100");
        IncrementalAlterConfigsRequestData.AlterableConfigCollection alterableConfigs = new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
        alterableConfigs.add(alterableConfig);

        data.resources().add(new AlterConfigsResource()
                .setResourceName("testtopic")
                .setResourceType(ResourceType.TOPIC.code())
                .setConfigs(alterableConfigs));
        return new IncrementalAlterConfigsRequest.Builder(data).build((short) 0);
    }

    private IncrementalAlterConfigsResponse createIncrementalAlterConfigsResponse() {
        IncrementalAlterConfigsResponseData data = new IncrementalAlterConfigsResponseData();

        data.responses().add(new AlterConfigsResourceResponse()
                .setResourceName("testtopic")
                .setResourceType(ResourceType.TOPIC.code())
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("Duplicate Keys"));
        return new IncrementalAlterConfigsResponse(data);
    }

    private AlterPartitionReassignmentsRequest createAlterPartitionReassignmentsRequest() {
        AlterPartitionReassignmentsRequestData data = new AlterPartitionReassignmentsRequestData();
        data.topics().add(
                new AlterPartitionReassignmentsRequestData.ReassignableTopic().setName("topic").setPartitions(
                        Collections.singletonList(
                                new AlterPartitionReassignmentsRequestData.ReassignablePartition().setPartitionIndex(0).setReplicas(null)
                        )
                )
        );
        return new AlterPartitionReassignmentsRequest.Builder(data).build((short) 0);
    }

    private AlterPartitionReassignmentsResponse createAlterPartitionReassignmentsResponse() {
        AlterPartitionReassignmentsResponseData data = new AlterPartitionReassignmentsResponseData();
        data.responses().add(
                new AlterPartitionReassignmentsResponseData.ReassignableTopicResponse()
                        .setName("topic")
                        .setPartitions(Collections.singletonList(
                                new AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse()
                                        .setPartitionIndex(0)
                                        .setErrorCode(Errors.NO_REASSIGNMENT_IN_PROGRESS.code())
                                        .setErrorMessage("No reassignment is in progress for topic topic partition 0")
                                )
                        )
        );
        return new AlterPartitionReassignmentsResponse(data);
    }

    private ListPartitionReassignmentsRequest createListPartitionReassignmentsRequest() {
        ListPartitionReassignmentsRequestData data = new ListPartitionReassignmentsRequestData();
        data.setTopics(
            Collections.singletonList(
                new ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics()
                    .setName("topic")
                    .setPartitionIndexes(Collections.singletonList(1))
            )
        );
        return new ListPartitionReassignmentsRequest.Builder(data).build((short) 0);
    }

    private ListPartitionReassignmentsResponse createListPartitionReassignmentsResponse() {
        ListPartitionReassignmentsResponseData data = new ListPartitionReassignmentsResponseData();
        data.setTopics(Collections.singletonList(
            new ListPartitionReassignmentsResponseData.OngoingTopicReassignment()
                        .setName("topic")
                        .setPartitions(Collections.singletonList(
                                new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
                                        .setPartitionIndex(0)
                                        .setReplicas(Arrays.asList(1, 2))
                                        .setAddingReplicas(Collections.singletonList(2))
                                        .setRemovingReplicas(Collections.singletonList(1))
                                )
                        )
        ));
        return new ListPartitionReassignmentsResponse(data);
    }

    private OffsetDeleteRequest createOffsetDeleteRequest() {
        OffsetDeleteRequestTopicCollection topics = new OffsetDeleteRequestTopicCollection();
        topics.add(new OffsetDeleteRequestTopic()
            .setName("topic1")
            .setPartitions(Collections.singletonList(
                new OffsetDeleteRequestPartition()
                    .setPartitionIndex(0)
                )
            )
        );

        OffsetDeleteRequestData data = new OffsetDeleteRequestData();
        data.setGroupId("group1");
        data.setTopics(topics);

        return new OffsetDeleteRequest.Builder(data).build((short) 0);
    }

    private OffsetDeleteResponse createOffsetDeleteResponse() {
        OffsetDeleteResponsePartitionCollection partitions = new OffsetDeleteResponsePartitionCollection();
        partitions.add(new OffsetDeleteResponsePartition()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code())
        );

        OffsetDeleteResponseTopicCollection topics = new OffsetDeleteResponseTopicCollection();
        topics.add(new OffsetDeleteResponseTopic()
            .setName("topic1")
            .setPartitions(partitions)
        );

        OffsetDeleteResponseData data = new OffsetDeleteResponseData();
        data.setErrorCode(Errors.NONE.code());
        data.setTopics(topics);

        return new OffsetDeleteResponse(data);
    }

    private AlterReplicaLogDirsRequest createAlterReplicaLogDirsRequest() {
        AlterReplicaLogDirsRequestData data = new AlterReplicaLogDirsRequestData();
        data.dirs().add(
                new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
                        .setPath("/data0")
                        .setTopics(new AlterReplicaLogDirTopicCollection(Collections.singletonList(
                                new AlterReplicaLogDirTopic()
                                        .setPartitions(singletonList(0))
                                        .setName("topic")
                        ).iterator())
                )
        );
        return new AlterReplicaLogDirsRequest.Builder(data).build((short) 0);
    }

    private AlterReplicaLogDirsResponse createAlterReplicaLogDirsResponse() {
        AlterReplicaLogDirsResponseData data = new AlterReplicaLogDirsResponseData();
        data.results().add(
                new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
                        .setTopicName("topic")
                        .setPartitions(Collections.singletonList(
                                new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                                        .setPartitionIndex(0)
                                        .setErrorCode(Errors.LOG_DIR_NOT_FOUND.code())
                                )
                        )
        );
        return new AlterReplicaLogDirsResponse(data);
    }

}
