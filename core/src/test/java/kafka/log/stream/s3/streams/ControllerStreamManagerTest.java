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

package kafka.log.stream.s3.streams;

import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.request.BatchRequest;

import org.apache.kafka.common.message.CloseStreamsRequestData.CloseStreamRequest;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.CloseStreamsRequest;
import org.apache.kafka.common.requests.s3.UpdateStreamArchiveRequest;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.streams.StreamArchiveOperation;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Verifies that broker close requests expose only fields supported by the finalized AutoMQ version.
 */
@Tag("S3Unit")
public class ControllerStreamManagerTest {

    /**
     * Given the same fast-close boundary, V5 emits legacy metadata while V6 emits the tagged end offset.
     */
    @Test
    public void testEndOffsetEmissionIsGatedByFinalizedVersion() {
        assertEquals(-1L, captureCloseRequest(AutoMQVersion.V5, 10L).endOffset());
        assertEquals(10L, captureCloseRequest(AutoMQVersion.V6, 10L).endOffset());
    }

    /**
     * Given finalized V5 and V6 cluster versions, fast close capability follows the finalized version gate.
     */
    @Test
    public void testFastCloseCapabilityIsGatedByFinalizedVersion() {
        assertFalse(new ControllerStreamManager(null, null, 1, 2L,
            () -> AutoMQVersion.V5, false).isFastCloseSupported());
        assertTrue(new ControllerStreamManager(null, null, 1, 2L,
            () -> AutoMQVersion.V6, false).isFastCloseSupported());
    }

    /**
     * Given an Archive update, verify the Broker emits one typed v0 operation with a 1,000-entry cap.
     */
    @Test
    public void testArchiveUpdateUsesBoundedBatchRequest() {
        ControllerRequestSender sender = mock(ControllerRequestSender.class);
        ControllerStreamManager manager = new ControllerStreamManager(null, sender, 1, 2L,
            () -> AutoMQVersion.V6, false);
        StreamArchiveOperation.ArchivePublish update = new StreamArchiveOperation.ArchivePublish(
            3L, 4L, 0L, 0L, 0L);

        manager.updateStreamArchive(update);

        ArgumentCaptor<ControllerRequestSender.RequestTask> captor =
            ArgumentCaptor.forClass(ControllerRequestSender.RequestTask.class);
        verify(sender).send(captor.capture());
        BatchRequest batch = (BatchRequest) captor.getValue().request();
        UpdateStreamArchiveRequest request =
            (UpdateStreamArchiveRequest) batch.toRequestBuilder().build((short) 0);
        assertEquals(1_000, batch.maxBatchSize());
        assertEquals(1, request.data().nodeId());
        assertEquals(2L, request.data().nodeEpoch());
        assertEquals(update.streamId(), request.data().operations().get(0).streamId());
        assertEquals(update.streamEpoch(), request.data().operations().get(0).streamEpoch());
    }

    /** Given Archive business failures, verify the client completes each operation exceptionally without retrying. */
    @Test
    public void testArchiveBusinessFailuresCompleteExceptionally() {
        ControllerStreamManager manager = new ControllerStreamManager(null, null, 1, 2L,
            () -> AutoMQVersion.V6, false);
        CompletableFuture<Void> result = new CompletableFuture<>();

        manager.handleArchiveResponse(Errors.INVALID_REQUEST, result);
        assertTrue(result.isCompletedExceptionally());

        CompletableFuture<Void> conflictResult = new CompletableFuture<>();
        manager.handleArchiveResponse(Errors.STREAM_ARCHIVE_STATE_CONFLICT, conflictResult);
        assertTrue(conflictResult.isCompletedExceptionally());
    }

    private CloseStreamRequest captureCloseRequest(AutoMQVersion version, long endOffset) {
        ControllerRequestSender sender = mock(ControllerRequestSender.class);
        ControllerStreamManager manager = new ControllerStreamManager(null, sender, 1, 2L,
            () -> version, false);

        manager.closeStream0(3L, 4L, 1, 2L, endOffset);

        ArgumentCaptor<ControllerRequestSender.RequestTask> captor =
            ArgumentCaptor.forClass(ControllerRequestSender.RequestTask.class);
        verify(sender).send(captor.capture());
        CloseStreamsRequest request = (CloseStreamsRequest) captor.getValue().request()
            .toRequestBuilder().build((short) 0);
        return request.data().closeStreamRequests().get(0);
    }

}
