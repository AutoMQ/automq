/*
 * Copyright 2026, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.kafka.controller;

import org.apache.kafka.common.message.CreateStreamsRequestData;
import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.OpenStreamsRequestData;
import org.apache.kafka.common.message.OpenStreamsRequestData.OpenStreamRequest;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.ArchivePublish;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.StreamArchiveOperation;
import org.apache.kafka.common.message.UpdateStreamArchiveResponseData;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.StreamArchiveOperationType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.controller.stream.StreamClient;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.MemoryObjectStorage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Qualifies the QuorumController batch contract for Stream Archive updates.
 */
@Tag("S3Unit")
public class QuorumControllerArchiveTest {
    private static final int BROKER_ID = 1;
    private static final long BROKER_EPOCH = 0L;
    private static final long STREAM_ID = 0L;
    private static final long STREAM_EPOCH = 0L;

    /**
     * Given duplicate Stream IDs with mixed business outcomes, verify positional independent results and top errors.
     */
    @Test
    public void testArchiveUpdatesPreservePositionAndSeparateFrameworkErrors() throws Exception {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).build();
             QuorumControllerTestEnv controllerEnv = controllerEnv(logEnv, AutoMQVersion.V6)) {
            QuorumController controller = controllerEnv.activeController(true);
            createOwnedStream(controller);
            UpdateStreamArchiveRequestData request = new UpdateStreamArchiveRequestData()
                .setNodeId(BROKER_ID)
                .setNodeEpoch(BROKER_EPOCH)
                .setOperations(List.of(idleState(), idleState(-1L),
                    idleState()));

            UpdateStreamArchiveResponseData response =
                controller.updateStreamArchive(ANONYMOUS_CONTEXT, request).get();

            assertEquals(Errors.NONE.code(), response.errorCode());
            assertEquals(List.of(Errors.NONE.code(), Errors.INVALID_REQUEST.code(), Errors.NONE.code()),
                response.updateStreamResponses().stream().map(result -> result.errorCode()).toList());
            assertEquals(0L, controller.streamControlManager().getStreamArchiveMetadata(STREAM_ID)
                .archiveEndOffset());

            ControllerRequestContext expiredContext = new ControllerRequestContext(
                new RequestHeaderData(), KafkaPrincipal.ANONYMOUS, OptionalLong.of(0L));
            UpdateStreamArchiveResponseData frameworkError =
                controller.updateStreamArchive(expiredContext, request).get();
            assertEquals(Errors.REQUEST_TIMED_OUT.code(), frameworkError.errorCode());
            assertEquals(List.of(), frameworkError.updateStreamResponses());
        }
    }

    /**
     * Given pre-Archive finalized metadata, verify the batch is rejected without materializing Archive state.
     */
    @Test
    public void testArchiveFeatureGateRejectsWholeBatchWithoutState() throws Exception {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).build();
             QuorumControllerTestEnv controllerEnv = controllerEnv(logEnv, AutoMQVersion.V5)) {
            QuorumController controller = controllerEnv.activeController(true);
            UpdateStreamArchiveRequestData request = new UpdateStreamArchiveRequestData()
                .setNodeId(BROKER_ID)
                .setNodeEpoch(BROKER_EPOCH)
                .setOperations(List.of(idleState(), idleState()));

            UpdateStreamArchiveResponseData response =
                controller.updateStreamArchive(ANONYMOUS_CONTEXT, request).get();

            assertEquals(List.of(Errors.UNSUPPORTED_VERSION.code(), Errors.UNSUPPORTED_VERSION.code()),
                response.updateStreamResponses().stream().map(result -> result.errorCode()).toList());
            assertNull(controller.streamControlManager().getStreamArchiveMetadata(STREAM_ID));
        }
    }

    private static QuorumControllerTestEnv controllerEnv(LocalLogManagerTestEnv logEnv, AutoMQVersion version)
        throws Exception {
        BootstrapMetadata bootstrap = BootstrapMetadata.fromRecords(List.of(
            new ApiMessageAndVersion(new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(MetadataVersion.latestTesting().featureLevel()), (short) 0),
            new ApiMessageAndVersion(new FeatureLevelRecord()
                .setName(AutoMQVersion.FEATURE_NAME)
                .setFeatureLevel(version.featureLevel()), (short) 0)), "Archive batch test");
        return new QuorumControllerTestEnv.Builder(logEnv)
            .setBootstrapMetadata(bootstrap)
            .setControllerBuilderInitializer(builder -> builder.setStreamClient(StreamClient.builder()
                .streamConfig(new Config()).objectStorage(new MemoryObjectStorage()).build()))
            .build();
    }

    private static void createOwnedStream(QuorumController controller) throws Exception {
        controller.getOpeningStreams(ANONYMOUS_CONTEXT, new GetOpeningStreamsRequestData()
            .setNodeId(BROKER_ID).setNodeEpoch(-1L)).get();
        controller.createStreams(ANONYMOUS_CONTEXT, new CreateStreamsRequestData()
            .setNodeId(BROKER_ID).setNodeEpoch(BROKER_EPOCH)
            .setCreateStreamRequests(List.of(new CreateStreamRequest().setNodeId(BROKER_ID)))).get();
        controller.openStreams(ANONYMOUS_CONTEXT, new OpenStreamsRequestData()
            .setNodeId(BROKER_ID).setNodeEpoch(BROKER_EPOCH)
            .setOpenStreamRequests(List.of(new OpenStreamRequest()
                .setStreamId(STREAM_ID).setStreamEpoch(STREAM_EPOCH)))).get();
    }

    private static StreamArchiveOperation idleState() {
        return idleState(0L);
    }

    private static StreamArchiveOperation idleState(long endOffset) {
        return new StreamArchiveOperation()
            .setStreamId(STREAM_ID)
            .setStreamEpoch(STREAM_EPOCH)
            .setOperation(StreamArchiveOperationType.ARCHIVE_PUBLISH.value())
            .setArchivePublish(new ArchivePublish()
                .setExpectedArchiveEndOffset(0L).setArchiveEndOffset(endOffset).setArchiveSize(0L));
    }
}
