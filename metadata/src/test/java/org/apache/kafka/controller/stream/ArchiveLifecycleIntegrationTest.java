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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.CreateStreamsResponseData.CreateStreamResponse;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.OpenStreamsRequestData.OpenStreamRequest;
import org.apache.kafka.common.message.OpenStreamsResponseData.OpenStreamResponse;
import org.apache.kafka.common.message.TrimStreamsRequestData.TrimStreamRequest;
import org.apache.kafka.common.message.TrimStreamsResponseData.TrimStreamResponse;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.StreamArchiveUpdate;
import org.apache.kafka.common.message.UpdateStreamArchiveResponseData.UpdateStreamResponse;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.S3StreamArchiveRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.FeatureControlManager;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamArchiveMetadata;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.automq.stream.api.Stream;
import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.CompositeObjectWriter;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.compact.StreamObjectArchiveCleanupTask;
import com.automq.stream.s3.compact.StreamObjectArchiveTask;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Qualifies the complete Archive lifecycle across the Broker, Controller, KRaft Image, and object-storage seams.
 */
@Tag("S3Unit")
public class ArchiveLifecycleIntegrationTest {
    private static final int BROKER_ID = 0;
    private static final long BROKER_EPOCH = 0L;
    private static final long STREAM_ID = 0L;
    private static final long STREAM_EPOCH = 0L;
    private static final long OBJECT_ID = 10L;
    private static final long END_OFFSET = 10L;
    private static final long LOGICAL_SIZE = 512L * 1024 * 1024;

    private StreamControlManager controller;
    private S3ObjectControlManager objectControlManager;
    private MemoryObjectStorage objectStorage;

    @BeforeEach
    public void setUp() {
        LogContext logContext = new LogContext();
        QuorumController quorumController = mock(QuorumController.class);
        objectControlManager = mock(S3ObjectControlManager.class);
        FeatureControlManager featureControlManager = mock(FeatureControlManager.class);
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.LATEST);
        when(quorumController.isActive()).thenReturn(false);
        objectStorage = new MemoryObjectStorage();
        controller = new StreamControlManager(quorumController, new SnapshotRegistry(logContext), logContext,
            objectControlManager, mock(ClusterControlManager.class), featureControlManager,
            mock(ReplicationControlManager.class), objectStorage, new MockTime());
    }

    /**
     * Given one owned Stream with a terminal Composite, verify prepare, copy, publish, replay, trim, and cleanup.
     */
    @Test
    public void testCompleteArchiveLifecycleAcrossBrokerControllerImageAndObjectStorage() throws Exception {
        ControllerResult<?> registration = controller.getOpeningStreams(new GetOpeningStreamsRequestData()
            .setNodeId(BROKER_ID).setNodeEpoch(-1L));
        replayController(registration.records());
        ControllerResult<CreateStreamResponse> create = controller.createStream(BROKER_ID, BROKER_EPOCH,
            new CreateStreamRequest().setNodeId(BROKER_ID));
        replayController(create.records());
        ControllerResult<OpenStreamResponse> open = controller.openStream(BROKER_ID, BROKER_EPOCH,
            new OpenStreamRequest().setStreamId(STREAM_ID).setStreamEpoch(STREAM_EPOCH));
        replayController(open.records());

        AtomicReference<MetadataImage> image = new AtomicReference<>(MetadataImage.EMPTY);
        replayImage(image, create.records());
        replayImage(image, open.records());
        S3ObjectMetadata source = writeComposite();
        S3StreamObjectRecord streamObject = new S3StreamObjectRecord().setStreamId(STREAM_ID)
            .setObjectId(OBJECT_ID).setStartOffset(0L).setEndOffset(END_OFFSET);
        controller.replay(streamObject);
        when(objectControlManager.getObject(OBJECT_ID)).thenReturn(new S3Object(OBJECT_ID, source.objectSize(), 0L,
            S3ObjectState.COMMITTED,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes()));
        replayImage(image, List.of(new ApiMessageAndVersion(streamObject, (short) 0)));

        List<StreamArchiveState> desiredStates = new ArrayList<>();
        StreamManager broker = archiveStreamManager(image, desiredStates);
        ObjectManager imageBackedObjects = mock(ObjectManager.class);
        when(imageBackedObjects.getStreamObjects(STREAM_ID, 0L, 100L, Integer.MAX_VALUE)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(image.get().streamsMetadata()
                .getStreamObjects(STREAM_ID, 0L, 100L, Integer.MAX_VALUE)
                .stream().map(object -> source).toList()));
        AtomicLong streamStartOffset = new AtomicLong(0L);
        Stream stream = stream(streamStartOffset);

        StreamObjectArchiveTask.builder().objectManager(imageBackedObjects).streamManager(broker)
            .objectStorage(objectStorage).stream(stream).build().archive();

        assertEquals(2, desiredStates.size());
        assertEquals(END_OFFSET, desiredStates.get(0).archivePreparedEndOffset());
        assertEquals(0L, desiredStates.get(0).archiveEndOffset());
        assertEquals(END_OFFSET, desiredStates.get(1).archiveEndOffset());
        assertEquals(LOGICAL_SIZE, desiredStates.get(1).archiveSize());
        assertArchiveStateReplayed(image);
        String archiveKey = "archive/0/0000000000000000010-0000000000000000000-10-536870912";
        assertTrue(objectStorage.contains(ObjectUtils.genKey(0, OBJECT_ID)));
        assertTrue(objectStorage.contains(archiveKey));

        ControllerResult<TrimStreamResponse> trim = controller.trimStream(BROKER_ID, BROKER_EPOCH,
            new TrimStreamRequest().setStreamId(STREAM_ID).setStreamEpoch(STREAM_EPOCH)
                .setNewStartOffset(END_OFFSET));
        assertEquals(Errors.NONE.code(), trim.response().errorCode());
        replayController(trim.records());
        replayImage(image, trim.records());
        streamStartOffset.set(END_OFFSET);

        assertTrue(StreamObjectArchiveCleanupTask.builder().streamManager(broker).objectStorage(objectStorage)
            .stream(stream).cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache()).build().cleanup());

        assertFalse(objectStorage.contains(archiveKey));
        assertTrue(objectStorage.contains(ObjectUtils.genKey(0, OBJECT_ID)));
        assertEquals(END_OFFSET, controller.getStreamArchiveMetadata(STREAM_ID).archiveStartOffset());
        assertEquals(0L, controller.getStreamArchiveMetadata(STREAM_ID).archiveSize());
        assertArchiveStateReplayed(image);
    }

    private StreamManager archiveStreamManager(AtomicReference<MetadataImage> image,
        List<StreamArchiveState> desiredStates) {
        StreamManager broker = mock(StreamManager.class);
        when(broker.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored -> {
            S3StreamArchiveMetadata archive = image.get().streamsMetadata().getStreamArchiveMetadata(STREAM_ID);
            return CompletableFuture.completedFuture(new StreamArchiveState(STREAM_ID, STREAM_EPOCH,
                archive.archiveStartOffset(), archive.archiveMetadataEndOffset(), archive.archiveEndOffset(),
                archive.archivePreparedEndOffset(), archive.archiveSize(), archive.archiveCleanupEndOffset(),
                archive.archiveCleanupSize(), List.of()));
        });
        when(broker.updateStreamArchive(any())).thenAnswer(invocation -> {
            StreamArchiveState desired = invocation.getArgument(0);
            desiredStates.add(desired);
            ControllerResult<UpdateStreamResponse> result = controller.updateStreamArchive(
                BROKER_ID, BROKER_EPOCH, toRequest(desired));
            Errors error = Errors.forCode(result.response().errorCode());
            if (error != Errors.NONE) {
                return CompletableFuture.failedFuture(error.exception());
            }
            replayController(result.records());
            replayImage(image, result.records());
            return CompletableFuture.completedFuture(null);
        });
        return broker;
    }

    private static StreamArchiveUpdate toRequest(StreamArchiveState desired) {
        return new StreamArchiveUpdate()
            .setStreamId(desired.streamId())
            .setStreamEpoch(desired.streamEpoch())
            .setArchiveStartOffset(desired.archiveStartOffset())
            .setArchiveMetadataEndOffset(desired.archiveMetadataEndOffset())
            .setArchiveEndOffset(desired.archiveEndOffset())
            .setArchivePreparedEndOffset(desired.archivePreparedEndOffset())
            .setArchiveSize(desired.archiveSize())
            .setArchiveCleanupEndOffset(desired.archiveCleanupEndOffset())
            .setArchiveCleanupSize(desired.archiveCleanupSize())
            .setArchiveObjectIds(desired.archiveObjectIds());
    }

    private S3ObjectMetadata writeComposite() throws Exception {
        S3ObjectMetadata linked = new S3ObjectMetadata(OBJECT_ID + 1_000,
            ObjectAttributes.builder().bucket(objectStorage.bucketId()).build().attributes());
        DataBlockIndex index = new DataBlockIndex(STREAM_ID, 0L, Math.toIntExact(END_OFFSET),
            1, 0L, Math.toIntExact(LOGICAL_SIZE));
        CompositeObjectWriter writer = CompositeObject.writer(objectStorage.writer(new ObjectStorage.WriteOptions(),
            ObjectUtils.genKey(0, OBJECT_ID)));
        writer.addComponent(linked, List.of(index));
        writer.close().get();
        return new S3ObjectMetadata(OBJECT_ID, S3ObjectType.COMPOSITE,
            List.of(new StreamOffsetRange(STREAM_ID, 0L, END_OFFSET)), 0L, 0L, writer.size(), OBJECT_ID,
            ObjectAttributes.builder().bucket(objectStorage.bucketId()).type(ObjectAttributes.Type.Composite).build()
                .attributes());
    }

    private static Stream stream(AtomicLong startOffset) {
        Stream stream = mock(Stream.class);
        when(stream.streamId()).thenReturn(STREAM_ID);
        when(stream.streamEpoch()).thenReturn(STREAM_EPOCH);
        when(stream.startOffset()).thenAnswer(ignored -> startOffset.get());
        when(stream.confirmOffset()).thenReturn(100L);
        return stream;
    }

    private void assertArchiveStateReplayed(AtomicReference<MetadataImage> image) {
        assertEquals(controller.getStreamArchiveMetadata(STREAM_ID),
            image.get().streamsMetadata().getStreamArchiveMetadata(STREAM_ID));
    }

    private void replayController(List<ApiMessageAndVersion> records) {
        for (ApiMessageAndVersion record : records) {
            ApiMessage message = record.message();
            if (message instanceof AssignedStreamIdRecord assignedStreamId) {
                controller.replay(assignedStreamId);
            } else if (message instanceof NodeWALMetadataRecord node) {
                controller.replay(node);
            } else if (message instanceof S3StreamRecord stream) {
                controller.replay(stream);
            } else if (message instanceof RangeRecord range) {
                controller.replay(range);
            } else if (message instanceof S3StreamArchiveRecord archive) {
                controller.replay(archive);
            }
        }
    }

    private static void replayImage(AtomicReference<MetadataImage> image,
        List<ApiMessageAndVersion> records) {
        MetadataDelta delta = new MetadataDelta(image.get());
        records.forEach(record -> delta.replay(record.message()));
        image.set(delta.apply(MetadataProvenance.EMPTY));
    }
}
