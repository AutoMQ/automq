/*
 * Copyright 2025, AutoMQ HK Limited.
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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CloseStreamsRequestData.CloseStreamRequest;
import org.apache.kafka.common.message.CloseStreamsResponseData.CloseStreamResponse;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData.ObjectStreamRange;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData.StreamObject;
import org.apache.kafka.common.message.CommitStreamSetObjectResponseData;
import org.apache.kafka.common.message.CreateStreamsRequestData;
import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.CreateStreamsResponseData.CreateStreamResponse;
import org.apache.kafka.common.message.DeleteStreamsRequestData.DeleteStreamRequest;
import org.apache.kafka.common.message.DeleteStreamsResponseData.DeleteStreamResponse;
import org.apache.kafka.common.message.DescribeStreamsRequestData;
import org.apache.kafka.common.message.DescribeStreamsResponseData;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.message.OpenStreamsRequestData.OpenStreamRequest;
import org.apache.kafka.common.message.OpenStreamsResponseData.OpenStreamResponse;
import org.apache.kafka.common.message.TrimStreamsRequestData.TrimStreamRequest;
import org.apache.kafka.common.message.TrimStreamsResponseData.TrimStreamResponse;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.ArchivePrepare;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.StreamArchiveOperation;
import org.apache.kafka.common.message.UpdateStreamArchiveResponseData.UpdateStreamResponse;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.NodeWALUncommittedOffsetsRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamArchiveRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.metadata.S3StreamArchiveRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.StreamArchiveOperationType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.FeatureControlManager;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.metadata.stream.NodeWALUncommittedOffset;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamArchiveMetadata;
import org.apache.kafka.metadata.stream.S3StreamEndOffsetsCodec;
import org.apache.kafka.metadata.stream.StreamEndOffset;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.automq.stream.s3.CompositeObjectWriter;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 40)
@Tag("S3Unit")
@SuppressWarnings("checkstyle:JavaNCSS")
public class StreamControlManagerTest {

    private static final long STREAM0 = 0;
    private static final long STREAM1 = 1;
    private static final long STREAM2 = 2;

    private static final int BROKER0 = 0;
    private static final int BROKER1 = 1;
    private static final int BROKER2 = 2;

    private static final long EPOCH0 = 0;
    private static final long EPOCH1 = 1;
    private static final long EPOCH2 = 2;

    private static final long BROKER_EPOCH0 = 0;

    private static final String TOPIC = "test";
    private static final Uuid TOPIC_ID = Uuid.ONE_UUID;
    private static final int PARTITION = 0;

    private QuorumController quorumController;
    private StreamControlManager manager;
    private S3ObjectControlManager objectControlManager;
    private ClusterControlManager clusterControlManager;
    private FeatureControlManager featureControlManager;
    private ReplicationControlManager replicationControlManager;
    private MemoryObjectStorage objectStorage;
    private MockTime time;

    @BeforeEach
    public void setUp() {
        LogContext context = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(context);
        quorumController = mock(QuorumController.class);

        objectControlManager = mock(S3ObjectControlManager.class);
        clusterControlManager = mock(ClusterControlManager.class);
        featureControlManager = mock(FeatureControlManager.class);
        replicationControlManager = mock(ReplicationControlManager.class);
        objectStorage = new MemoryObjectStorage();
        time = new MockTime();
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.LATEST);
        when(replicationControlManager.getTopicId(TOPIC)).thenReturn(TOPIC_ID);
        when(replicationControlManager.getTopic(TOPIC_ID)).thenReturn(new ReplicationControlManager.TopicControlInfo(TOPIC, new SnapshotRegistry(new LogContext()), TOPIC_ID));

        manager = new StreamControlManager(quorumController, registry, context, objectControlManager,
            clusterControlManager, featureControlManager, replicationControlManager, objectStorage, time);
        doAnswer(args -> {
            QuorumController.ControllerWriteOperation<?> op = args.getArgument(2);
            ControllerResult<?> rst = op.generateRecordsAndResult();
            replay(manager, rst.records());
            return CompletableFuture.completedFuture(rst.response());
        }).when(quorumController).appendWriteEvent(anyString(), any(), any());
    }

    @Test
    public void testBasicCreateStream() {
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);

        // 1. create stream_0 success
        CreateStreamRequest request0 = new CreateStreamRequest()
            .setNodeId(BROKER0);
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        List<ApiMessageAndVersion> records0 = result0.records();
        CreateStreamResponse response0 = result0.response();
        assertEquals(Errors.NONE.code(), response0.errorCode());
        assertEquals(STREAM0, response0.streamId());
        assertEquals(2, records0.size());
        ApiMessageAndVersion record0 = records0.get(0);
        assertInstanceOf(AssignedStreamIdRecord.class, record0.message());
        AssignedStreamIdRecord assignedRecord = (AssignedStreamIdRecord) record0.message();
        assertEquals(STREAM0, assignedRecord.assignedStreamId());
        ApiMessageAndVersion record1 = records0.get(1);
        assertInstanceOf(S3StreamRecord.class, record1.message());
        S3StreamRecord streamRecord0 = (S3StreamRecord) record1.message();
        assertEquals(STREAM0, streamRecord0.streamId());
        assertEquals(S3StreamConstant.INIT_EPOCH, streamRecord0.epoch());
        assertEquals(S3StreamConstant.INIT_RANGE_INDEX, streamRecord0.rangeIndex());
        assertEquals(S3StreamConstant.INIT_START_OFFSET, streamRecord0.startOffset());

        // replay
        manager.replay(assignedRecord);
        manager.replay(streamRecord0);
        // verify the stream_0 is created
        Map<Long, StreamRuntimeMetadata> streamsMetadata =
            manager.streamsMetadata();
        assertEquals(1, streamsMetadata.size());
        verifyInitializedStreamMetadata(streamsMetadata.get(STREAM0));
        assertEquals(1, manager.nextAssignedStreamId());

        // 2. create stream_1
        CreateStreamRequest request1 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result1 = manager.createStream(BROKER0, BROKER_EPOCH0, request1);
        List<ApiMessageAndVersion> records1 = result1.records();
        CreateStreamResponse response1 = result1.response();
        assertEquals(Errors.NONE.code(), response1.errorCode());
        assertEquals(STREAM1, response1.streamId());
        assertEquals(2, records1.size());
        record0 = records1.get(0);
        assertInstanceOf(AssignedStreamIdRecord.class, record0.message());
        assignedRecord = (AssignedStreamIdRecord) record0.message();
        assertEquals(STREAM1, assignedRecord.assignedStreamId());
        record1 = records1.get(1);
        assertInstanceOf(S3StreamRecord.class, record1.message());
        S3StreamRecord streamRecord1 = (S3StreamRecord) record1.message();
        assertEquals(STREAM1, streamRecord1.streamId());
        assertEquals(S3StreamConstant.INIT_EPOCH, streamRecord1.epoch());
        assertEquals(S3StreamConstant.INIT_RANGE_INDEX, streamRecord1.rangeIndex());
        assertEquals(S3StreamConstant.INIT_START_OFFSET, streamRecord1.startOffset());

        // replay records_1
        manager.replay(assignedRecord);
        manager.replay(streamRecord1);
        // verify the stream_2 is created
        streamsMetadata =
            manager.streamsMetadata();
        assertEquals(2, streamsMetadata.size());
        verifyInitializedStreamMetadata(streamsMetadata.get(STREAM1));
        assertEquals(2, manager.nextAssignedStreamId());
    }

    @Test
    public void testBasicOpenCloseStream() {
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);

        // 1. create stream_0 and stream_1
        CreateStreamRequest request0 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, result0.records());
        CreateStreamRequest request1 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result1 = manager.createStream(BROKER0, BROKER_EPOCH0, request1);
        replay(manager, result1.records());

        // verify the streams are created
        Map<Long, StreamRuntimeMetadata> streamsMetadata = manager.streamsMetadata();
        assertEquals(2, streamsMetadata.size());
        verifyInitializedStreamMetadata(streamsMetadata.get(STREAM0));
        verifyInitializedStreamMetadata(streamsMetadata.get(STREAM1));
        assertEquals(2, manager.nextAssignedStreamId());

        // 2. node_0 open stream_0 and stream_1 with epoch0
        ControllerResult<OpenStreamResponse> result2 = manager.openStream(BROKER0, EPOCH0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        ControllerResult<OpenStreamResponse> result3 = manager.openStream(BROKER0, EPOCH0,
            new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH0));
        assertEquals(Errors.NONE.code(), result2.response().errorCode());
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        assertEquals(0L, result2.response().startOffset());
        assertEquals(0L, result3.response().startOffset());
        assertEquals(0L, result2.response().nextOffset());
        assertEquals(0L, result3.response().nextOffset());
        verifyFirstTimeOpenStreamResult(result2, EPOCH0, BROKER0);
        verifyFirstTimeOpenStreamResult(result3, EPOCH0, BROKER0);
        S3StreamRecord streamRecord = (S3StreamRecord) result2.records().get(0).message();
        manager.replay(streamRecord);
        RangeRecord rangeRecord = (RangeRecord) result2.records().get(1).message();
        manager.replay(rangeRecord);
        streamRecord = (S3StreamRecord) result3.records().get(0).message();
        manager.replay(streamRecord);
        rangeRecord = (RangeRecord) result3.records().get(1).message();
        manager.replay(rangeRecord);

        // verify the stream_0 and stream_1 metadata are updated, and the range_0 is created
        StreamRuntimeMetadata streamMetadata0 = manager.streamsMetadata().get(STREAM0);
        verifyFirstRange(manager.streamsMetadata().get(STREAM0), EPOCH0, BROKER0);
        verifyFirstRange(manager.streamsMetadata().get(STREAM1), EPOCH0, BROKER0);

        // TODO: support write range record, then roll the range and verify
        // 3. node_1 try to open stream_0 with epoch0
        ControllerResult<OpenStreamResponse> result4 = manager.openStream(BROKER1, 0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        assertEquals(Errors.STREAM_FENCED.code(), result4.response().errorCode());
        assertEquals(0, result4.records().size());

        // 4. node_0 try to open stream_1 with epoch0
        ControllerResult<OpenStreamResponse> result6 = manager.openStream(BROKER0, 0,
            new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH0));
        assertEquals(Errors.NONE.code(), result6.response().errorCode());
        assertEquals(0L, result6.response().startOffset());
        assertEquals(0L, result6.response().nextOffset());
        assertEquals(0, result6.records().size());

        // 5. node_0 try to open stream_1 with epoch1
        ControllerResult<OpenStreamResponse> result7 = manager.openStream(BROKER0, 0,
            new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH1));
        assertEquals(Errors.STREAM_NOT_CLOSED.code(), result7.response().errorCode());

        // 6. node_1 try to open stream_1 with epoch0
        ControllerResult<OpenStreamResponse> result8 = manager.openStream(BROKER1, 0,
            new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH0));
        assertEquals(Errors.STREAM_FENCED.code(), result8.response().errorCode());
        assertEquals(0, result8.records().size());

        // 7. node_1 try to open stream_1 with epoch1
        ControllerResult<OpenStreamResponse> result9 = manager.openStream(BROKER1, 0,
            new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH1));
        assertEquals(Errors.STREAM_NOT_CLOSED.code(), result9.response().errorCode());

        // 8. node_1 try to close stream_1 with epoch0
        ControllerResult<CloseStreamResponse> result10 = manager.closeStream(BROKER1, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH0));
        assertEquals(Errors.STREAM_FENCED.code(), result10.response().errorCode());

        // 9. node_0 try to close stream_1 with epoch1
        ControllerResult<CloseStreamResponse> result11 = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH1));
        assertEquals(Errors.STREAM_INNER_ERROR.code(), result11.response().errorCode());

        // 10. node_0 try to close stream_1 with epoch0
        ControllerResult<CloseStreamResponse> result12 = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH0));
        assertEquals(Errors.NONE.code(), result12.response().errorCode());
        replay(manager, result12.records());

        // 11. node_0 try to close stream_1 with epoch0 again
        ControllerResult<CloseStreamResponse> result13 = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH0));
        assertEquals(Errors.NONE.code(), result13.response().errorCode());

        // 12. node_1 try to open stream_1 with epoch1
        ControllerResult<OpenStreamResponse> result14 = manager.openStream(BROKER1, 0,
            new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(EPOCH1));
        assertEquals(Errors.NONE.code(), result14.response().errorCode());
        replay(manager, result14.records());

        // 13. verify the stream_1 metadata are updated, and the range_1 is created
        StreamRuntimeMetadata streamMetadata1 = manager.streamsMetadata().get(STREAM1);
        assertEquals(EPOCH1, streamMetadata1.currentEpoch());
        RangeMetadata range = streamMetadata1.ranges().get(streamMetadata1.currentRangeIndex());
        assertEquals(EPOCH1, range.epoch());
        assertEquals(BROKER1, range.nodeId());
    }

    /**
     * Given a V5 cluster, a fast close is rejected while a legacy close retains the existing record contract.
     */
    @Test
    public void testV5RejectsFastCloseAndAcceptsLegacyClose() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V5);
        createAndOpenStream0();

        ControllerResult<CloseStreamResponse> fastClose = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(10L));
        assertEquals(Errors.UNSUPPORTED_VERSION.code(), fastClose.response().errorCode());
        assertEquals(0, fastClose.records().size());

        ControllerResult<CloseStreamResponse> legacyClose = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        assertEquals(Errors.NONE.code(), legacyClose.response().errorCode());
        assertEquals(1, legacyClose.records().size());
        assertInstanceOf(S3StreamRecord.class, legacyClose.records().get(0).message());
    }

    /**
     * Given a finalized V6 cluster, a non-negative close end offset is accepted.
     */
    @Test
    public void testV6AcceptsFastClose() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        createAndOpenStream0();

        ControllerResult<CloseStreamResponse> result = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(10L));

        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(3, result.records().size());
    }

    /**
     * Given a V6 stream with a logical end, verify fast close rejects regression, avoids an empty
     * responsibility entry at the boundary, and atomically records a later handoff boundary.
     */
    @Test
    public void testV6FastCloseRecordsNodeWALResponsibility() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        createAndOpenStream0();
        manager.replay(new S3StreamEndOffsetsRecord().setEndOffsets(
            S3StreamEndOffsetsCodec.encode(List.of(new StreamEndOffset(STREAM0, 10L)))));

        ControllerResult<CloseStreamResponse> belowEnd = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(9L));
        assertEquals(Errors.OFFSET_NOT_MATCHED.code(), belowEnd.response().errorCode());
        assertEquals(0, belowEnd.records().size());

        ControllerResult<CloseStreamResponse> atEnd = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(10L));
        assertEquals(Errors.NONE.code(), atEnd.response().errorCode());
        assertEquals(2, atEnd.records().size());
        assertInstanceOf(S3StreamRecord.class, atEnd.records().get(0).message());
        assertInstanceOf(RangeRecord.class, atEnd.records().get(1).message());

        ControllerResult<CloseStreamResponse> afterEnd = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(20L));
        assertEquals(Errors.NONE.code(), afterEnd.response().errorCode());
        assertEquals(3, afterEnd.records().size());
        assertInstanceOf(S3StreamRecord.class, afterEnd.records().get(0).message());
        assertInstanceOf(RangeRecord.class, afterEnd.records().get(1).message());
        NodeWALUncommittedOffsetsRecord entryRecord = assertInstanceOf(
            NodeWALUncommittedOffsetsRecord.class, afterEnd.records().get(2).message());
        assertEquals(BROKER0, entryRecord.nodeId());
        assertEquals(List.of(new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
            .setStreamId(STREAM0).setStartOffset(10L).setEndOffset(20L)), entryRecord.entries());

        replay(manager, afterEnd.records());
        assertEquals(20L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals(20L, manager.streamsMetadata().get(STREAM0).currentRangeMetadata().endOffset());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 10L, 20L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
    }

    /**
     * Given a fast close exactly at logical end, verify replay seals the range and closes without
     * recording historical WAL responsibility.
     */
    @Test
    public void testV6FastCloseAtLogicalEndSealsRangeWithoutEntry() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        createAndOpenStream0();
        manager.replay(new S3StreamEndOffsetsRecord().setEndOffsets(
            S3StreamEndOffsetsCodec.encode(List.of(new StreamEndOffset(STREAM0, 10L)))));

        ControllerResult<CloseStreamResponse> result = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(10L));
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(2, result.records().size());
        assertTrue(result.records().stream()
            .noneMatch(record -> record.message() instanceof NodeWALUncommittedOffsetsRecord));

        replay(manager, result.records());
        StreamRuntimeMetadata stream = manager.streamsMetadata().get(STREAM0);
        assertEquals(StreamState.CLOSED, stream.currentState());
        assertEquals(10L, stream.endOffset());
        assertEquals(10L, stream.currentRangeMetadata().endOffset());
        assertTrue(manager.nodesMetadata().get(BROKER0).uncommittedOffsets().isEmpty());
    }

    /**
     * Given a completed fast close, verify an exact retry is record-free, a conflicting retry is
     * rejected, and an old owner cannot retry after a new owner opens the stream.
     */
    @Test
    public void testV6FastCloseRetryIsIdempotentAndFencedAfterReopen() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);

        ControllerResult<CloseStreamResponse> close = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L));
        replay(manager, close.records());

        ControllerResult<CloseStreamResponse> exactRetry = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L));
        assertEquals(Errors.NONE.code(), exactRetry.response().errorCode());
        assertTrue(exactRetry.records().isEmpty());

        ControllerResult<CloseStreamResponse> conflictingRetry = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(101L));
        assertEquals(Errors.OFFSET_NOT_MATCHED.code(), conflictingRetry.response().errorCode());
        assertTrue(conflictingRetry.records().isEmpty());

        openStream(BROKER1, EPOCH1, STREAM0);
        ControllerResult<CloseStreamResponse> staleRetry = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L));
        assertEquals(Errors.STREAM_FENCED.code(), staleRetry.response().errorCode());
        assertTrue(staleRetry.records().isEmpty());
    }

    /**
     * Given historical commits have advanced a fast-close entry, verify an exact close retry does
     * not recreate the original responsibility interval.
     */
    @Test
    public void testV6FastCloseRetryDoesNotRecreateAdvancedEntry() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);

        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        ControllerResult<CommitStreamSetObjectResponseData> commit = commitRange(
            BROKER0, BROKER_EPOCH0, 1L, 0L, 40L);
        assertEquals(Errors.NONE.code(), commit.response().errorCode());
        replay(manager, commit.records());

        ControllerResult<CloseStreamResponse> retry = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L));
        assertEquals(Errors.NONE.code(), retry.response().errorCode());
        assertTrue(retry.records().isEmpty());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 40L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
    }

    /**
     * Given a node retains active historical WAL responsibility, verify both normal open and a
     * same-epoch retry open are rejected until that responsibility becomes inactive.
     */
    @Test
    public void testOpenRejectsActiveHistoricalEntryForTargetNode() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());

        ControllerResult<OpenStreamResponse> retryOpen = manager.openStream(BROKER0, BROKER_EPOCH0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        assertEquals(Errors.STREAM_NOT_CLOSED.code(), retryOpen.response().errorCode());
        assertTrue(retryOpen.records().isEmpty());

        openStream(BROKER1, EPOCH1, STREAM0);
        replay(manager, manager.closeStream(BROKER1, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH1)).records());
        ControllerResult<OpenStreamResponse> normalOpen = manager.openStream(BROKER0, BROKER_EPOCH0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(2L));
        assertEquals(Errors.STREAM_NOT_CLOSED.code(), normalOpen.response().errorCode());
        assertTrue(normalOpen.records().isEmpty());
    }

    /**
     * Given trim makes a raw historical entry inactive, verify open succeeds at the logical end
     * without physically deleting that entry.
     */
    @Test
    public void testOpenPermitsAndRetainsInactiveHistoricalEntry() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);
        replay(manager, manager.trimStream(BROKER1, BROKER_EPOCH0, new TrimStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH1).setNewStartOffset(100L)).records());
        replay(manager, manager.closeStream(BROKER1, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH1)).records());

        NodeWALUncommittedOffset inactiveEntry =
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0);
        ControllerResult<OpenStreamResponse> result = manager.openStream(BROKER0, BROKER_EPOCH0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(2L));
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(100L, result.response().nextOffset());
        assertTrue(result.records().stream()
            .noneMatch(record -> record.message() instanceof NodeWALUncommittedOffsetsRecord));
        replay(manager, result.records());
        assertEquals(inactiveEntry,
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).currentRangeMetadata().startOffset());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).currentRangeMetadata().endOffset());
    }

    /**
     * Given a node owns both an opened stream and a trimmed historical interval, verify failover
     * returns both responsibilities with the historical range epoch and visible recovery start.
     */
    @Test
    public void testGetOpeningStreamsIncludesActiveHistoricalResponsibility() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);
        createAndOpenStream(BROKER0, EPOCH0);

        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);
        replay(manager, commitRange(BROKER0, BROKER_EPOCH0, 1L, 0L, 40L).records());
        replay(manager, manager.trimStream(BROKER1, BROKER_EPOCH0, new TrimStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH1).setNewStartOffset(60L)).records());

        ControllerResult<GetOpeningStreamsResponseData> result = manager.getOpeningStreams(
            new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true));

        assertEquals(Errors.NONE.code(), result.response().errorCode());
        Map<Long, GetOpeningStreamsResponseData.StreamMetadata> streams = result.response()
            .streamMetadataList().stream().collect(Collectors.toMap(
                GetOpeningStreamsResponseData.StreamMetadata::streamId, metadata -> metadata));
        assertEquals(2, streams.size());
        assertEquals(EPOCH0, streams.get(STREAM0).epoch());
        assertEquals(60L, streams.get(STREAM0).startOffset());
        assertEquals(60L, streams.get(STREAM0).endOffset());
        assertEquals(EPOCH0, streams.get(STREAM1).epoch());
        assertEquals(0L, streams.get(STREAM1).endOffset());

        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM1).setStreamEpoch(EPOCH0)).records());
        assertTrue(manager.hasOpeningStreams(BROKER0));
    }

    /**
     * Given historical WAL metadata no longer has one matching sealed ownership range, verify
     * failover reports corruption instead of silently omitting responsibility.
     */
    @Test
    public void testGetOpeningStreamsRejectsMissingHistoricalRange() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        manager.replay(new NodeWALUncommittedOffsetsRecord().setNodeId(BROKER0).setEntries(List.of(
            new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
                .setStreamId(STREAM0).setStartOffset(0L).setEndOffset(101L))));

        ControllerResult<GetOpeningStreamsResponseData> result = manager.getOpeningStreams(
            new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true));

        assertEquals(Errors.STREAM_INNER_ERROR.code(), result.response().errorCode());
        assertTrue(result.response().streamMetadataList().isEmpty());
        assertTrue(result.records().isEmpty());
    }

    /**
     * Given two sealed ranges both match one historical responsibility entry, verify failover
     * reports ambiguous ownership as metadata corruption.
     */
    @Test
    public void testGetOpeningStreamsRejectsAmbiguousHistoricalRange() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        manager.replay(new RangeRecord()
            .setStreamId(STREAM0)
            .setRangeIndex(1)
            .setEpoch(EPOCH1)
            .setNodeId(BROKER0)
            .setStartOffset(0L)
            .setEndOffset(100L));

        ControllerResult<GetOpeningStreamsResponseData> result = manager.getOpeningStreams(
            new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true));

        assertEquals(Errors.STREAM_INNER_ERROR.code(), result.response().errorCode());
        assertTrue(result.response().streamMetadataList().isEmpty());
        assertTrue(result.records().isEmpty());
    }

    /**
     * Given trim advances beyond historical WAL progress before failover establishes its node
     * barrier, verify normal commits are fenced while failover recovery commits from the visible
     * start returned by getOpeningStreams.
     */
    @Test
    public void testGetOpeningStreamsFailoverBarrierFencesNormalCommit() {
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);

        ControllerResult<CommitStreamSetObjectResponseData> beforeBarrier = commitRange(
            BROKER0, BROKER_EPOCH0, 1L, 0L, 40L);
        assertEquals(Errors.NONE.code(), beforeBarrier.response().errorCode());
        replay(manager, beforeBarrier.records());
        replay(manager, manager.trimStream(BROKER1, BROKER_EPOCH0, new TrimStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH1).setNewStartOffset(60L)).records());

        ControllerResult<GetOpeningStreamsResponseData> barrier = manager.getOpeningStreams(
            new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true));
        assertEquals(1, barrier.records().size());
        assertEquals(1, barrier.response().streamMetadataList().size());
        assertEquals(60L, barrier.response().streamMetadataList().get(0).endOffset());
        replay(manager, barrier.records());

        ControllerResult<CommitStreamSetObjectResponseData> normalCommit = commitRange(
            BROKER0, BROKER_EPOCH0, 2L, 60L, 100L);
        assertEquals(Errors.NODE_FENCED.code(), normalCommit.response().errorCode());
        assertTrue(normalCommit.records().isEmpty());

        ControllerResult<CommitStreamSetObjectResponseData> failoverCommit = manager.commitStreamSetObject(
            new CommitStreamSetObjectRequestData()
                .setNodeId(BROKER0)
                .setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true)
                .setObjectId(3L)
                .setOrderId(3L)
                .setObjectSize(999L)
                .setObjectStreamRanges(List.of(new ObjectStreamRange()
                    .setStreamId(STREAM0)
                    .setStreamEpoch(EPOCH0)
                    .setStartOffset(60L)
                    .setEndOffset(100L))));
        assertEquals(Errors.NONE.code(), failoverCommit.response().errorCode());
        replay(manager, failoverCommit.records());
        assertFalse(manager.hasOpeningStreams(BROKER0));
    }

    /**
     * Given nodes A and B each own one current range and one crossed historical range, verify
     * failing over A then B and B then A converges to the same openable state.
     */
    @Test
    public void testCrossedHistoricalOwnershipIsFailoverOrderIndependent() {
        assertEquals(recoverCrossedOwnership(List.of(BROKER0, BROKER1)),
            recoverCrossedOwnership(List.of(BROKER1, BROKER0)));
    }

    @Test
    public void testCommitStreamSetObjectBasic() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).then(ink -> {
            long objectId = ink.getArgument(0);
            if (objectId == 1) {
                return ControllerResult.of(Collections.emptyList(), Errors.OBJECT_NOT_EXIST);
            }
            return ControllerResult.of(Collections.emptyList(), Errors.NONE);
        });
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);

        // 1. create and open stream_0
        CreateStreamRequest request0 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, result0.records());
        ControllerResult<OpenStreamResponse> result2 = manager.openStream(BROKER0, 0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        verifyFirstTimeOpenStreamResult(result2, EPOCH0, BROKER0);
        replay(manager, result2.records());
        // 2. commit valid stream set object
        List<ObjectStreamRange> streamRanges0 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(0L)
            .setEndOffset(100L));
        CommitStreamSetObjectRequestData commitRequest0 = new CommitStreamSetObjectRequestData()
            .setObjectId(0L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges0);
        ControllerResult<CommitStreamSetObjectResponseData> result3 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        replay(manager, result3.records());
        // verify range's end offset advanced and stream set object is added
        StreamRuntimeMetadata streamMetadata0 = manager.streamsMetadata().get(STREAM0);
        assertEquals(1, streamMetadata0.ranges().size());
        RangeMetadata rangeMetadata0 = streamMetadata0.ranges().get(0);
        assertEquals(0L, rangeMetadata0.startOffset());
        assertEquals(100L, streamMetadata0.endOffset());
        assertEquals(1, manager.nodesMetadata().get(BROKER0).streamSetObjects().size());
        // 3. commit a stream set object that doesn't exist
        List<ObjectStreamRange> streamRanges1 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(100)
            .setEndOffset(200));
        CommitStreamSetObjectRequestData commitRequest1 = new CommitStreamSetObjectRequestData()
            .setObjectId(1L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges1);
        ControllerResult<CommitStreamSetObjectResponseData> result4 = manager.commitStreamSetObject(commitRequest1);
        assertEquals(Errors.OBJECT_NOT_EXIST.code(), result4.response().errorCode());
        // 4. node_0 close stream_0 with epoch_0 and node_1 open stream_0 with epoch_1
        ControllerResult<CloseStreamResponse> result7 = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        assertEquals(Errors.NONE.code(), result7.response().errorCode());
        replay(manager, result7.records());
        ControllerResult<OpenStreamResponse> result8 = manager.openStream(BROKER1, 0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH1));
        assertEquals(Errors.NONE.code(), result8.response().errorCode());
        assertEquals(0L, result8.response().startOffset());
        assertEquals(100L, result8.response().nextOffset());
        replay(manager, result8.records());
        // 5. node_1 successfully commit stream set object which contains stream_0's data
        List<ObjectStreamRange> streamRanges6 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH1)
            .setStartOffset(100)
            .setEndOffset(300));
        CommitStreamSetObjectRequestData commitRequest6 = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER1)
            .setObjectId(6L)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges6);
        ControllerResult<CommitStreamSetObjectResponseData> result10 = manager.commitStreamSetObject(commitRequest6);
        assertEquals(Errors.NONE.code(), result10.response().errorCode());
        replay(manager, result10.records());
        // verify range's end offset advanced and stream set object is added
        streamMetadata0 = manager.streamsMetadata().get(STREAM0);
        assertEquals(2, streamMetadata0.ranges().size());
        assertEquals(0L, streamMetadata0.ranges().get(0).startOffset());
        assertEquals(100L, streamMetadata0.ranges().get(0).endOffset());
        RangeMetadata rangeMetadata1 = streamMetadata0.ranges().get(1);
        assertEquals(100L, rangeMetadata1.startOffset());
        assertEquals(300L, streamMetadata0.endOffset());
        assertEquals(1, manager.nodesMetadata().get(BROKER1).streamSetObjects().size());

        // 6. get stream's offset
        GetOpeningStreamsRequestData request = new GetOpeningStreamsRequestData()
            .setNodeId(BROKER1).setNodeEpoch(0L);
        GetOpeningStreamsResponseData response = manager.getOpeningStreams(request).response();
        assertEquals(1, response.streamMetadataList().size());
        assertEquals(STREAM0, response.streamMetadataList().get(0).streamId());
        assertEquals(0L, response.streamMetadataList().get(0).startOffset());
        assertEquals(300L, response.streamMetadataList().get(0).endOffset());

        request = new GetOpeningStreamsRequestData()
            .setNodeId(BROKER0).setNodeEpoch(0L);
        assertEquals(0, manager.getOpeningStreams(request).response().streamMetadataList().size());
    }

    @Test
    public void testCommitStreamSetObject_compactWithDeletedStream() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).then(args -> {
            long objectId = args.getArgument(0);
            return ControllerResult.of(
                List.of(
                    new ApiMessageAndVersion(
                        new S3ObjectRecord().setObjectId(objectId).setObjectState(S3ObjectState.COMMITTED.toByte()),
                        (short) 0
                    )
                ),
                true);
        });
        when(objectControlManager.markDestroyObjects(anyList(), anyList())).then(args -> {
            List<Long> objectIds = args.getArgument(0);
            return ControllerResult.of(
                objectIds
                    .stream()
                    .map(id ->
                        new ApiMessageAndVersion(
                            new S3ObjectRecord().setObjectId(id).setObjectState(S3ObjectState.MARK_DESTROYED.toByte()),
                            (short) 0
                        )
                    )
                    .collect(Collectors.toList()),
                true);
        });
        when(objectControlManager.markDestroyObjects(anyList())).then(args -> {
            List<Long> objectIds = args.getArgument(0);
            return ControllerResult.of(
                objectIds
                    .stream()
                    .map(id ->
                        new ApiMessageAndVersion(
                            new S3ObjectRecord().setObjectId(id).setObjectState(S3ObjectState.MARK_DESTROYED.toByte()),
                            (short) 0
                        )
                    )
                    .collect(Collectors.toList()),
                true);
        });
        registerAlwaysSuccessEpoch(BROKER0);

        // 1. create and open stream_0
        CreateStreamRequest request0 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, result0.records());
        ControllerResult<OpenStreamResponse> result2 = manager.openStream(BROKER0, 0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        replay(manager, result2.records());

        // 2. setup compacted object
        for (int i = 0; i < 2; i++) {
            ControllerResult<CommitStreamSetObjectResponseData> rst = manager.commitStreamSetObject(new CommitStreamSetObjectRequestData()
                .setObjectId(i)
                .setNodeId(BROKER0)
                .setObjectSize(999)
                .setObjectStreamRanges(List.of(new ObjectStreamRange()
                    .setStreamId(STREAM0)
                    .setStreamEpoch(EPOCH0)
                    .setStartOffset(i)
                    .setEndOffset(i + 1))));
            replay(manager, rst.records());
        }

        // 2. compact stream set object
        List<ObjectStreamRange> streamRanges0 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(0L)
            .setEndOffset(2L));
        // STREAM1 is not exist
        List<StreamObject> streamObjects = List.of(new StreamObject().setStreamId(STREAM1).setObjectId(233).setObjectSize(111).setStartOffset(111).setEndOffset(200));

        CommitStreamSetObjectRequestData commitRequest0 = new CommitStreamSetObjectRequestData()
            .setObjectId(2L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges0)
            .setStreamObjects(streamObjects).setCompactedObjectIds(List.of(0L, 1L));
        ControllerResult<CommitStreamSetObjectResponseData> result3 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        replay(manager, result3.records());

        List<ApiMessageAndVersion> records = result3.records();
        assertEquals(7, records.size());
        assertEquals(2, ((S3ObjectRecord) records.get(0).message()).objectId());
        assertEquals(S3ObjectState.COMMITTED.toByte(), ((S3ObjectRecord) records.get(0).message()).objectState());

        assertEquals(0, ((S3ObjectRecord) records.get(1).message()).objectId());
        assertEquals(S3ObjectState.MARK_DESTROYED.toByte(), ((S3ObjectRecord) records.get(1).message()).objectState());

        assertEquals(1, ((S3ObjectRecord) records.get(2).message()).objectId());
        assertEquals(S3ObjectState.MARK_DESTROYED.toByte(), ((S3ObjectRecord) records.get(2).message()).objectState());

        assertEquals(2, ((S3StreamSetObjectRecord) records.get(3).message()).objectId());

        // STREAM1 stream object should fast delete
        assertEquals(233, ((S3ObjectRecord) records.get(4).message()).objectId());
        assertEquals(S3ObjectState.MARK_DESTROYED.toByte(), ((S3ObjectRecord) records.get(4).message()).objectState());

        assertEquals(0, ((RemoveStreamSetObjectRecord) records.get(5).message()).objectId());

        assertEquals(1, ((RemoveStreamSetObjectRecord) records.get(6).message()).objectId());
    }

    @Test
    public void testCommitStreamSetObject_theSameStreamSetObject() {
        List<Long> committed = new LinkedList<>();
        when(objectControlManager.getObject(anyLong())).thenAnswer(args -> committed.contains(args.getArgument(0))
            ? new S3Object(args.getArgument(0), 999L, 0L, S3ObjectState.COMMITTED, 0) : null);
        when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).then(args -> {
            long objectId = args.getArgument(0);
            if (committed.contains(objectId)) {
                return ControllerResult.of(Collections.emptyList(), Errors.REDUNDANT_OPERATION);
            }
            committed.add(objectId);
            return ControllerResult.of(Collections.emptyList(), Errors.NONE);
        });
        registerAlwaysSuccessEpoch(BROKER0);

        // 1. create and open stream_0
        CreateStreamRequest request0 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, result0.records());
        ControllerResult<OpenStreamResponse> result2 = manager.openStream(BROKER0, 0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        verifyFirstTimeOpenStreamResult(result2, EPOCH0, BROKER0);
        replay(manager, result2.records());

        // 2. commit valid stream set object
        List<ObjectStreamRange> streamRanges0 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(0L)
            .setEndOffset(100L));
        CommitStreamSetObjectRequestData commitRequest0 = new CommitStreamSetObjectRequestData()
            .setObjectId(0L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges0);
        ControllerResult<CommitStreamSetObjectResponseData> result3 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        replay(manager, result3.records());

        // 3. re-commit the same object
        ControllerResult<CommitStreamSetObjectResponseData> result4 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result4.response().errorCode());
        assertTrue(result4.records().isEmpty());
    }

    @Test
    public void testCommitStreamSetObject_theSameStreamObject() {
        List<Long> committed = new LinkedList<>();
        when(objectControlManager.getObject(anyLong())).thenAnswer(args -> committed.contains(args.getArgument(0))
            ? new S3Object(args.getArgument(0), 999L, 0L, S3ObjectState.COMMITTED, 0) : null);
        when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).then(args -> {
            long objectId = args.getArgument(0);
            if (objectId == NOOP_OBJECT_ID) {
                return ControllerResult.of(Collections.emptyList(), Errors.NONE);
            }
            if (committed.contains(objectId)) {
                return ControllerResult.of(Collections.emptyList(), Errors.REDUNDANT_OPERATION);
            }
            committed.add(objectId);
            return ControllerResult.of(Collections.emptyList(), Errors.NONE);
        });
        registerAlwaysSuccessEpoch(BROKER0);

        // 1. create and open stream_0
        CreateStreamRequest request0 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, result0.records());
        ControllerResult<OpenStreamResponse> result2 = manager.openStream(BROKER0, 0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        verifyFirstTimeOpenStreamResult(result2, EPOCH0, BROKER0);
        replay(manager, result2.records());

        // 2. commit valid stream set object
        List<StreamObject> streamObjects = List.of(new StreamObject()
            .setStreamId(STREAM0)
            .setObjectId(0L)
            .setObjectSize(999)
            .setStartOffset(0L)
            .setEndOffset(100L));
        CommitStreamSetObjectRequestData commitRequest0 = new CommitStreamSetObjectRequestData()
            .setObjectId(-1L)
            .setNodeId(BROKER0)
            .setObjectSize(0)
            .setStreamObjects(streamObjects);
        ControllerResult<CommitStreamSetObjectResponseData> result3 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        replay(manager, result3.records());

        // 3. re-commit the same object
        ControllerResult<CommitStreamSetObjectResponseData> result4 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result4.response().errorCode());
        assertTrue(result4.records().isEmpty());
    }

    @Test
    public void testTrimBeyondCommit() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).then(args -> {
            long objectId = args.getArgument(0);
            return ControllerResult.of(
                List.of(
                    new ApiMessageAndVersion(
                        new S3ObjectRecord().setObjectId(objectId).setObjectState(S3ObjectState.COMMITTED.toByte()),
                        (short) 0
                    )
                ),
                true);
        });
        registerAlwaysSuccessEpoch(BROKER0);
        CreateStreamRequest request0 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> createStreamRst = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, createStreamRst.records());
        ControllerResult<OpenStreamResponse> openStreamRst = manager.openStream(BROKER0, 0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        replay(manager, openStreamRst.records());

        ControllerResult<TrimStreamResponse> trimRst = manager.trimStream(BROKER0, EPOCH0, new TrimStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setNewStartOffset(100L));
        replay(manager, trimRst.records());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).startOffset());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());

        ControllerResult<CommitStreamSetObjectResponseData> commitRst = manager.commitStreamSetObject(new CommitStreamSetObjectRequestData().setNodeId(BROKER0).setNodeEpoch(EPOCH0).setObjectStreamRanges(
            List.of(
                new ObjectStreamRange().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setStartOffset(0).setEndOffset(10L)
            )
        ));
        replay(manager, commitRst.records());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals((short) 0, commitRst.response().errorCode());
    }

    /**
     * Given a trim beyond committed data, verify fully trimmed, first trim-cross, continuous,
     * repeated-overlap, and gap commits follow the logical-end contract.
     */
    @Test
    public void testCurrentOwnerCommitClassificationAfterTrim() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).then(args -> {
            long objectId = args.getArgument(0);
            return ControllerResult.of(List.of(new ApiMessageAndVersion(
                new S3ObjectRecord().setObjectId(objectId).setObjectState(S3ObjectState.COMMITTED.toByte()),
                (short) 0)), true);
        });
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);

        ControllerResult<TrimStreamResponse> trimResult = manager.trimStream(BROKER0, BROKER_EPOCH0,
            new TrimStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setNewStartOffset(100L));
        replay(manager, trimResult.records());

        ControllerResult<CommitStreamSetObjectResponseData> fullyTrimmed = commitRange(1L, 0L, 90L);
        assertEquals(Errors.NONE.code(), fullyTrimmed.response().errorCode());
        replay(manager, fullyTrimmed.records());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());

        ControllerResult<CommitStreamSetObjectResponseData> trimCross = commitRange(2L, 90L, 110L);
        assertEquals(Errors.NONE.code(), trimCross.response().errorCode());
        replay(manager, trimCross.records());
        assertEquals(110L, manager.streamsMetadata().get(STREAM0).endOffset());

        ControllerResult<CommitStreamSetObjectResponseData> repeatedOverlap = commitRange(3L, 95L, 120L);
        assertEquals(Errors.OFFSET_NOT_MATCHED.code(), repeatedOverlap.response().errorCode());
        assertTrue(repeatedOverlap.records().isEmpty());

        ControllerResult<CommitStreamSetObjectResponseData> gap = commitRange(4L, 120L, 130L);
        assertEquals(Errors.OFFSET_NOT_MATCHED.code(), gap.response().errorCode());
        assertTrue(gap.records().isEmpty());

        ControllerResult<CommitStreamSetObjectResponseData> continuous = commitRange(5L, 110L, 120L);
        assertEquals(Errors.NONE.code(), continuous.response().errorCode());
        replay(manager, continuous.records());
        assertEquals(120L, manager.streamsMetadata().get(STREAM0).endOffset());
    }

    /**
     * Given trim reaches the current owner's logical end before broker failover, verify recovery and
     * the first failover upload both start at the visible offset returned by getOpeningStreams.
     */
    @Test
    public void testCurrentOwnerFailoverCommitStartsAtTrimmedEnd() {
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.trimStream(BROKER0, BROKER_EPOCH0, new TrimStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setNewStartOffset(100L)).records());

        ControllerResult<GetOpeningStreamsResponseData> openingStreams = manager.getOpeningStreams(
            new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true));
        assertEquals(Errors.NONE.code(), openingStreams.response().errorCode());
        assertEquals(1, openingStreams.response().streamMetadataList().size());
        assertEquals(100L, openingStreams.response().streamMetadataList().get(0).endOffset());
        replay(manager, openingStreams.records());

        ControllerResult<CommitStreamSetObjectResponseData> commit = manager.commitStreamSetObject(
            new CommitStreamSetObjectRequestData()
                .setNodeId(BROKER0)
                .setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true)
                .setObjectId(6L)
                .setOrderId(6L)
                .setObjectSize(999L)
                .setObjectStreamRanges(List.of(new ObjectStreamRange()
                    .setStreamId(STREAM0)
                    .setStreamEpoch(EPOCH0)
                    .setStartOffset(100L)
                    .setEndOffset(120L))));
        assertEquals(Errors.NONE.code(), commit.response().errorCode());
        replay(manager, commit.records());
        assertEquals(120L, manager.streamsMetadata().get(STREAM0).endOffset());
    }

    /**
     * Given a fast-closed range with a new owner, verify historical and current-owner commits
     * advance only their respective logical state and historical commits stay within the seal.
     */
    @Test
    public void testHistoricalAndCurrentOwnerCommitsAreRoutedIndependently() {
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);

        ControllerResult<CloseStreamResponse> closeResult = manager.closeStream(BROKER0, BROKER_EPOCH0,
            new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L));
        replay(manager, closeResult.records());
        openStream(BROKER1, EPOCH1, STREAM0);

        ControllerResult<CommitStreamSetObjectResponseData> historical = commitRange(
            BROKER0, BROKER_EPOCH0, 10L, 0L, 40L);
        assertEquals(Errors.NONE.code(), historical.response().errorCode());
        replay(manager, historical.records());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 40L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));

        ControllerResult<CommitStreamSetObjectResponseData> currentOwner = commitRange(
            BROKER1, BROKER_EPOCH0, 11L, 100L, 120L);
        assertEquals(Errors.NONE.code(), currentOwner.response().errorCode());
        replay(manager, currentOwner.records());
        assertEquals(120L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 40L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));

        ControllerResult<CommitStreamSetObjectResponseData> pastHistoricalEnd = commitRange(
            BROKER0, BROKER_EPOCH0, 12L, 40L, 101L);
        assertEquals(Errors.OFFSET_NOT_MATCHED.code(), pastHistoricalEnd.response().errorCode());
        assertTrue(pastHistoricalEnd.records().isEmpty());

        ControllerResult<CommitStreamSetObjectResponseData> historicalComplete = commitRange(
            BROKER0, BROKER_EPOCH0, 13L, 40L, 100L);
        assertEquals(Errors.NONE.code(), historicalComplete.response().errorCode());
        replay(manager, historicalComplete.records());
        assertEquals(120L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertNull(manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
    }

    /**
     * Given trim establishes the fast-close logical end, verify an object formed before trim can
     * still commit across the visible start while the old owner retains historical responsibility.
     */
    @Test
    public void testHistoricalCommitCrossesTrimAtInitialWALProgress() {
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);

        replay(manager, manager.trimStream(BROKER0, BROKER_EPOCH0, new TrimStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setNewStartOffset(40L)).records());
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);

        assertEquals(new NodeWALUncommittedOffset(STREAM0, 40L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
        ControllerResult<CommitStreamSetObjectResponseData> trimCross = commitRange(
            BROKER0, BROKER_EPOCH0, 14L, 0L, 60L);
        assertEquals(Errors.NONE.code(), trimCross.response().errorCode());
        replay(manager, trimCross.records());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 60L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));

        ControllerResult<CommitStreamSetObjectResponseData> repeatedOverlap = commitRange(
            BROKER0, BROKER_EPOCH0, 15L, 0L, 80L);
        assertEquals(Errors.OFFSET_NOT_MATCHED.code(), repeatedOverlap.response().errorCode());
    }

    /**
     * Given trim over a historical range, verify late commits remain fenced, fully trimmed data
     * preserves metadata without logical advances, and the first trim-cross advances the entry.
     */
    @Test
    public void testHistoricalCommitClassificationAfterTrim() {
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);
        replay(manager, manager.trimStream(BROKER1, BROKER_EPOCH0, new TrimStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH1).setNewStartOffset(40L)).records());

        replay(manager, manager.getOpeningStreams(new GetOpeningStreamsRequestData()
            .setNodeId(BROKER0).setNodeEpoch(1L)).records());
        ControllerResult<CommitStreamSetObjectResponseData> fenced = commitRange(
            BROKER0, BROKER_EPOCH0, 20L, 0L, 30L);
        assertEquals(Errors.NODE_EPOCH_EXPIRED.code(), fenced.response().errorCode());
        assertTrue(fenced.records().isEmpty());

        ControllerResult<CommitStreamSetObjectResponseData> fullyTrimmed = commitRange(
            BROKER0, 1L, 21L, 0L, 30L);
        assertEquals(Errors.NONE.code(), fullyTrimmed.response().errorCode());
        assertTrue(fullyTrimmed.records().stream().anyMatch(record -> record.message() instanceof S3StreamSetObjectRecord));
        assertTrue(fullyTrimmed.records().stream().noneMatch(record -> record.message() instanceof S3StreamEndOffsetsRecord));
        replay(manager, fullyTrimmed.records());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 0L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));

        ControllerResult<CommitStreamSetObjectResponseData> trimCross = commitRange(
            BROKER0, 1L, 22L, 0L, 60L);
        assertEquals(Errors.NONE.code(), trimCross.response().errorCode());
        replay(manager, trimCross.records());
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 60L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));

        replay(manager, manager.trimStream(BROKER1, BROKER_EPOCH0, new TrimStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH1).setNewStartOffset(100L)).records());
        ControllerResult<CommitStreamSetObjectResponseData> afterFullTrim = manager.commitStreamSetObject(
            new CommitStreamSetObjectRequestData()
                .setNodeId(BROKER0)
                .setNodeEpoch(1L)
                .setObjectId(23L)
                .setObjectSize(999L)
                .setOrderId(23L)
                .setObjectStreamRanges(List.of(new ObjectStreamRange()
                    .setStreamId(STREAM1)
                    .setStreamEpoch(EPOCH0)
                    .setStartOffset(0L)
                    .setEndOffset(10L))));
        assertEquals(Errors.NONE.code(), afterFullTrim.response().errorCode());
        replay(manager, afterFullTrim.records());
        assertNull(manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
    }

    /**
     * Given a sealed historical range and a later owner, when the later owner trims within that range,
     * then trim advances its own range without expanding it beyond the existing logical end.
     */
    @Test
    public void testTrimDoesNotExpandLaterOwnershipRange() {
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);

        ControllerResult<TrimStreamResponse> result = manager.trimStream(BROKER1, BROKER_EPOCH0,
            new TrimStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH1).setNewStartOffset(40L));
        replay(manager, result.records());

        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(new RangeMetadata(STREAM0, EPOCH0, 0, 40L, 100L, BROKER0),
            manager.streamsMetadata().get(STREAM0).ranges().get(0));
        assertEquals(new RangeMetadata(STREAM0, EPOCH1, 1, 100L, 100L, BROKER1),
            manager.streamsMetadata().get(STREAM0).ranges().get(1));
    }

    /**
     * Given a shared object containing deleted and live streams, verify the deleted range is
     * terminal success, its node entry is removed, and the live range still commits.
     */
    @Test
    public void testDeletedStreamRangeDoesNotFailSharedObject() {
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        replay(manager, manager.deleteStream(new DeleteStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0)).records());
        createAndOpenStream(BROKER0, EPOCH0);

        CommitStreamSetObjectRequestData request = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER0)
            .setNodeEpoch(BROKER_EPOCH0)
            .setObjectId(40L)
            .setObjectSize(999L)
            .setObjectStreamRanges(List.of(
                new ObjectStreamRange().setStreamId(STREAM0).setStartOffset(0L).setEndOffset(100L),
                new ObjectStreamRange().setStreamId(STREAM1).setStartOffset(0L).setEndOffset(20L)));
        ControllerResult<CommitStreamSetObjectResponseData> result = manager.commitStreamSetObject(request);
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        replay(manager, result.records());

        assertNull(manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
        assertEquals(20L, manager.streamsMetadata().get(STREAM1).endOffset());
        assertTrue(manager.nodesMetadata().get(BROKER0).streamSetObjects().containsKey(40L));
    }

    /**
     * Given compaction over an active historical range, verify it does not advance logical or WAL
     * state.
     */
    @Test
    public void testCompactionDoesNotAdvanceLogicalState() {
        mockSuccessfulObjectCommits();
        when(objectControlManager.markDestroyObjects(anyList()))
            .thenReturn(ControllerResult.of(Collections.emptyList(), true));
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);
        manager.replay(new NodeWALUncommittedOffsetsRecord().setNodeId(BROKER0).setEntries(List.of(
            new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
                .setStreamId(999L).setStartOffset(0L).setEndOffset(50L))));
        manager.replay(new S3StreamSetObjectRecord().setNodeId(BROKER0).setObjectId(70L)
            .setOrderId(70L).setDataTimeInMs(1L));

        CommitStreamSetObjectRequestData request = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER0)
            .setNodeEpoch(BROKER_EPOCH0)
            .setObjectId(71L)
            .setObjectSize(999L)
            .setCompactedObjectIds(List.of(70L))
            .setObjectStreamRanges(List.of(new ObjectStreamRange()
                .setStreamId(STREAM0).setStartOffset(0L).setEndOffset(40L)));
        ControllerResult<CommitStreamSetObjectResponseData> result = manager.commitStreamSetObject(request);
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        replay(manager, result.records());

        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 0L, 100L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(STREAM0));
        assertEquals(new NodeWALUncommittedOffset(999L, 0L, 50L),
            manager.nodesMetadata().get(BROKER0).uncommittedOffsets().get(999L));
    }

    /**
     * Given out-of-order stream and range records, verify runtime logical end only advances.
     */
    @Test
    public void testLogicalEndReplayIsMonotonic() {
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setRangeIndex(0).setStartOffset(100L));
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).endOffset());

        manager.replay(new RangeRecord().setStreamId(STREAM0).setRangeIndex(0)
            .setStartOffset(100L).setEndOffset(150L).setNodeId(BROKER0));
        assertEquals(150L, manager.streamsMetadata().get(STREAM0).endOffset());

        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setRangeIndex(0).setStartOffset(80L));
        manager.replay(new RangeRecord().setStreamId(STREAM0).setRangeIndex(0)
            .setStartOffset(80L).setEndOffset(120L).setNodeId(BROKER0));
        assertEquals(100L, manager.streamsMetadata().get(STREAM0).startOffset());
        assertEquals(150L, manager.streamsMetadata().get(STREAM0).endOffset());
    }

    /**
     * Given Stream Archive records, verify Controller replay resolves defaults, replaces complete
     * state, retains Archive state after Stream deletion, and removes it only on its own record.
     */
    @Test
    public void testArchiveStateReplay() {
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(100L));
        assertEquals(S3StreamArchiveMetadata.defaultAt(STREAM0, 100L),
            manager.getStreamArchiveMetadata(STREAM0));

        manager.replay(new S3StreamArchiveRecord()
            .setStreamId(STREAM0)
            .setArchiveStartOffset(10L)
            .setArchiveMetadataEndOffset(20L)
            .setArchiveEndOffset(30L)
            .setArchivePreparedEndOffset(40L)
            .setArchiveSize(500L)
            .setArchiveCleanupEndOffset(25L)
            .setArchiveCleanupSize(100L));
        assertEquals(new S3StreamArchiveMetadata(STREAM0, 10L, 20L, 30L, 40L, 500L, 25L, 100L),
            manager.getStreamArchiveMetadata(STREAM0));

        manager.replay(new RemoveS3StreamRecord().setStreamId(STREAM0));
        assertEquals(500L, manager.getStreamArchiveMetadata(STREAM0).archiveSize());

        manager.replay(new RemoveS3StreamArchiveRecord().setStreamId(STREAM0));
        assertNull(manager.getStreamArchiveMetadata(STREAM0));
    }

    /**
     * Given a live owned Stream, verify prepare and publish persist complete Archive states and
     * equal retries are idempotent.
     */
    @Test
    public void testUpdateStreamArchivePreparePublishAndIdempotency() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        manager.replay(new RangeRecord().setStreamId(STREAM0).setRangeIndex(0)
            .setNodeId(BROKER0).setEpoch(EPOCH0).setStartOffset(0L).setEndOffset(100L));
        addCommittedNormal(10L, 0L, 50L);

        ArchiveUpdate prepare = archiveUpdate(0L, 0L, 50L, 0L, 0L, 0L)
            .setArchiveObjectIds(List.of(10L));
        ControllerResult<UpdateStreamResponse> prepareResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepare);
        assertEquals(Errors.NONE.code(), prepareResult.response().errorCode());
        replay(manager, prepareResult.records());
        assertEquals(50L, manager.getStreamArchiveMetadata(STREAM0).archivePreparedEndOffset());

        assertTrue(manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepare).records().isEmpty());

        ArchiveUpdate publish = archiveUpdate(0L, 50L, 50L, 1_000L, 0L, 0L);
        ControllerResult<UpdateStreamResponse> publishResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, publish);
        assertEquals(Errors.NONE.code(), publishResult.response().errorCode());
        replay(manager, publishResult.records());
        assertEquals(50L, manager.getStreamArchiveMetadata(STREAM0).archiveEndOffset());
        assertEquals(1_000L, manager.getStreamArchiveMetadata(STREAM0).archiveSize());
    }

    /**
     * Given Controller metadata cleanup advances between Broker operations, verify prepare, publish, and retention
     * cleanup preserve that Controller-owned progress instead of conflicting with or overwriting it.
     */
    @Test
    public void testBrokerArchiveUpdatesPreserveControllerMetadataProgress() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        manager.replay(new RangeRecord().setStreamId(STREAM0).setRangeIndex(0)
            .setNodeId(BROKER0).setEpoch(EPOCH0).setStartOffset(0L).setEndOffset(100L));
        addCommittedNormal(10L, 50L, 100L);
        replay(manager, List.of(archiveMetadata(0L, 25L, 50L, 50L, 500L, 0L, 0L).toRecord()));

        ArchiveUpdate prepare = archiveUpdate(0L, 50L, 100L, 500L, 0L, 0L)
            .setArchiveObjectIds(List.of(10L));
        ControllerResult<UpdateStreamResponse> prepareResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepare);
        assertEquals(Errors.NONE.code(), prepareResult.response().errorCode());
        replay(manager, prepareResult.records());
        assertEquals(25L, manager.getStreamArchiveMetadata(STREAM0).archiveMetadataEndOffset());

        replay(manager, List.of(archiveMetadata(0L, 50L, 50L, 100L, 500L, 0L, 0L).toRecord()));
        ArchiveUpdate publish = archiveUpdate(0L, 100L, 100L, 1_000L, 0L, 0L);
        ControllerResult<UpdateStreamResponse> publishResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, publish);
        assertEquals(Errors.NONE.code(), publishResult.response().errorCode());
        replay(manager, publishResult.records());
        assertEquals(50L, manager.getStreamArchiveMetadata(STREAM0).archiveMetadataEndOffset());

        ArchiveUpdate cleanupPrepare = archiveUpdate(0L, 100L, 100L, 1_000L, 50L, 100L);
        ControllerResult<UpdateStreamResponse> cleanupPrepareResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, cleanupPrepare);
        assertEquals(Errors.NONE.code(), cleanupPrepareResult.response().errorCode());
        replay(manager, cleanupPrepareResult.records());
        ArchiveUpdate cleanupCommit = archiveUpdate(50L, 100L, 100L, 900L, 50L, 0L);
        ControllerResult<UpdateStreamResponse> cleanupCommitResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, cleanupCommit);
        assertEquals(Errors.NONE.code(), cleanupCommitResult.response().errorCode());
        replay(manager, cleanupCommitResult.records());
        assertEquals(50L, manager.getStreamArchiveMetadata(STREAM0).archiveMetadataEndOffset());
    }

    /**
     * Given more than one cleanup round of published Composite metadata, verify one atomic round
     * removes at most 1,000 mappings, shallow-deletes their manifests, and advances only to the
     * last removed Composite boundary.
     */
    @Test
    public void testArchiveMetadataCleanupIsBoundedAtomicAndShallow() {
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        for (long objectId = 0; objectId < 1_003; objectId++) {
            manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(objectId)
                .setStartOffset(objectId).setEndOffset(objectId + 1));
        }
        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 0L, 1_002L, 1_002L, 10_020L, 0L, 0L).toRecord().message());
        when(objectControlManager.getObject(anyLong())).thenAnswer(invocation -> new S3Object(
            invocation.getArgument(0), 10L, 0L, S3ObjectState.COMMITTED,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes()));
        when(objectControlManager.markDestroyObjects(anyList(), anyList())).thenAnswer(invocation -> {
            List<Long> objectIds = invocation.getArgument(0);
            List<ApiMessageAndVersion> records = objectIds.stream()
                .map(objectId -> new ApiMessageAndVersion(
                    new S3ObjectRecord().setObjectId(objectId).setObjectSize(10L)
                        .setObjectState(S3ObjectState.MARK_DESTROYED.toByte())
                        .setAttributes(ObjectAttributes.builder()
                            .type(ObjectAttributes.Type.Composite).build().attributes()), (short) 0))
                .collect(Collectors.toList());
            return ControllerResult.atomicOf(records, true);
        });

        ControllerResult<Void> result = manager.cleanupStreamArchiveMetadata(STREAM0);

        verify(objectControlManager).markDestroyObjects(anyList(),
            eq(Collections.nCopies(1_000, CompactOperations.DELETE)));
        assertTrue(result.isAtomic());
        assertEquals(2_001, result.records().size());
        assertEquals(1_000, result.records().stream()
            .filter(record -> record.message() instanceof S3ObjectRecord).count());
        assertEquals(1_000, result.records().stream()
            .filter(record -> record.message() instanceof RemoveS3StreamObjectRecord).count());
        S3StreamArchiveRecord progress = result.records().stream()
            .map(ApiMessageAndVersion::message)
            .filter(S3StreamArchiveRecord.class::isInstance)
            .map(S3StreamArchiveRecord.class::cast)
            .findFirst()
            .orElseThrow();
        assertEquals(1_000L, progress.archiveMetadataEndOffset());
        assertEquals(1_002L, progress.archiveEndOffset());
    }

    /**
     * Given publication with more than one cleanup round, verify replay triggers cleanup and each
     * progress replay appends the remaining work after the current Controller event.
     */
    @Test
    public void testArchivePublicationTriggersMetadataCleanupUntilCaughtUp() {
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        for (long objectId = 0; objectId < 1_003; objectId++) {
            manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(objectId)
                .setStartOffset(objectId).setEndOffset(objectId + 1));
        }
        when(objectControlManager.getObject(anyLong())).thenAnswer(invocation -> new S3Object(
            invocation.getArgument(0), 10L, 0L, S3ObjectState.COMMITTED,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes()));
        when(objectControlManager.markDestroyObjects(anyList(), anyList())).thenAnswer(invocation -> {
            List<Long> objectIds = invocation.getArgument(0);
            return ControllerResult.atomicOf(objectIds.stream()
                .map(objectId -> new ApiMessageAndVersion(
                    new S3ObjectRecord().setObjectId(objectId).setObjectSize(10L)
                        .setObjectState(S3ObjectState.MARK_DESTROYED.toByte())
                        .setAttributes(ObjectAttributes.builder()
                            .type(ObjectAttributes.Type.Composite).build().attributes()), (short) 0))
                .collect(Collectors.toList()), true);
        });
        when(quorumController.isActive()).thenReturn(true);

        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 0L, 1_002L, 1_002L, 10_020L, 0L, 0L).toRecord().message());
        manager.reconcileStreamArchiveMetadataCleanup();

        assertEquals(Set.of(1_002L), manager.streamsMetadata().get(STREAM0).streamObjects().keySet());
        assertEquals(1_002L, manager.getStreamArchiveMetadata(STREAM0).archiveMetadataEndOffset());
        verify(objectControlManager, times(2)).markDestroyObjects(anyList(), anyList());
    }

    /**
     * Given publication leaves source metadata pending cleanup, verify Broker retention cleanup
     * cannot deep-delete linked objects from the authoritative Archive range.
     */
    @Test
    public void testPublishedArchiveRangeFencesDeepDeleteUntilMetadataCleanup() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        addCommittedComposite(10L, 0L, 50L);
        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 0L, 50L, 50L, 500L, 0L, 0L).toRecord().message());
        CommitStreamObjectRequestData cleanup = commitStreamObject(
            NOOP_OBJECT_ID, ObjectUtils.NOOP_OFFSET, ObjectUtils.NOOP_OFFSET, List.of(10L))
            .setOperations(List.of(CompactOperations.DEEP_DELETE.value()));

        ControllerResult<CommitStreamObjectResponseData> result = manager.commitStreamObject(cleanup);

        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), result.response().errorCode());
        assertTrue(result.records().isEmpty());
        verify(objectControlManager, never()).markDestroyObjects(anyList(), anyList());
    }

    /**
     * Given cleanup scheduling was lost before a Controller leadership change, verify periodic
     * reconciliation rediscovers the durable backlog and reclaims it.
     */
    @Test
    public void testArchiveMetadataCleanupReconciliationRediscoversBacklog() {
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(10L)
            .setStartOffset(0L).setEndOffset(50L));
        when(objectControlManager.getObject(10L)).thenReturn(new S3Object(
            10L, 500L, 0L, S3ObjectState.COMMITTED,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes()));
        when(objectControlManager.markDestroyObjects(anyList(), anyList())).thenReturn(
            ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                new S3ObjectRecord().setObjectId(10L).setObjectSize(500L)
                    .setObjectState(S3ObjectState.MARK_DESTROYED.toByte())
                    .setAttributes(ObjectAttributes.builder()
                        .type(ObjectAttributes.Type.Composite).build().attributes()), (short) 0)), true));
        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 0L, 50L, 50L, 500L, 0L, 0L).toRecord().message());
        when(quorumController.isActive()).thenReturn(true);

        manager.reconcileStreamArchiveMetadataCleanup();

        assertTrue(manager.streamsMetadata().get(STREAM0).streamObjects().isEmpty());
        assertEquals(50L, manager.getStreamArchiveMetadata(STREAM0).archiveMetadataEndOffset());
        verify(objectControlManager).markDestroyObjects(anyList(), anyList());
    }

    /**
     * Given Archive records are added, replaced, and removed, verify capacity accounting follows those records
     * independently of Stream deletion.
     */
    @Test
    public void testStreamArchiveSizeFollowsArchiveRecords() {
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        manager.replay(new S3StreamRecord().setStreamId(STREAM1).setStartOffset(0L));
        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 50L, 50L, 50L, 500L, 0L, 0L).toRecord().message());
        manager.replay(new S3StreamArchiveRecord()
            .setStreamId(STREAM1)
            .setArchiveStartOffset(0L)
            .setArchiveMetadataEndOffset(70L)
            .setArchiveEndOffset(70L)
            .setArchivePreparedEndOffset(70L)
            .setArchiveSize(700L)
            .setArchiveCleanupEndOffset(0L)
            .setArchiveCleanupSize(0L));

        assertEquals(1_200L, manager.streamArchiveSize());

        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 50L, 50L, 50L, 600L, 0L, 0L).toRecord().message());
        assertEquals(1_300L, manager.streamArchiveSize());

        manager.replay(new RemoveS3StreamRecord().setStreamId(STREAM0));
        assertEquals(1_300L, manager.streamArchiveSize());

        manager.replay(new RemoveS3StreamArchiveRecord().setStreamId(STREAM0));
        assertEquals(700L, manager.streamArchiveSize());
    }

    /**
     * Given a closed Stream with a published Archive record and an empty Archive prefix, when the
     * Stream is deleted, then visible Stream metadata disappears immediately and the Controller
     * removes the durable deletion task after the empty prefix remains quiescent for five minutes.
     */
    @Test
    public void testDeleteStreamCompletesPublishedArchiveTaskAfterQuiescence() {
        when(quorumController.isActive()).thenReturn(true);
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 50L, 50L, 50L, 500L, 0L, 0L).toRecord().message());
        when(objectControlManager.markDestroyObjects(List.of(), List.of()))
            .thenReturn(ControllerResult.atomicOf(List.of(), true));

        ControllerResult<DeleteStreamResponse> result = manager.deleteStream(new DeleteStreamRequest()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0));
        replay(manager, result.records());
        manager.reconcileStreamArchiveMetadataCleanup();
        time.sleep(TimeUnit.MINUTES.toMillis(5));
        manager.reconcileStreamArchiveMetadataCleanup();
        verify(quorumController, timeout(5_000)).appendWriteEvent(
            eq("completeDeletedStreamArchiveCleanup"), eq(OptionalLong.empty()), any());

        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertNull(manager.streamsMetadata().get(STREAM0));
        assertNull(manager.getStreamArchiveMetadata(STREAM0));
    }

    /**
     * Given more than one page of canonical Composite manifests and a malformed top-level object, when a new
     * Controller rediscovers the deleted Stream's durable task, then it repeatedly deletes bounded first pages,
     * deep-deletes known Composites, and directly deletes the malformed object.
     */
    @Test
    public void testDeletedStreamArchiveCleanupIsBoundedFormatAgnosticAndRecoveredAfterFailover()
        throws Exception {
        objectStorage = spy(new MemoryObjectStorage());
        manager = new StreamControlManager(quorumController, new SnapshotRegistry(new LogContext()),
            new LogContext(), objectControlManager, clusterControlManager, featureControlManager,
            replicationControlManager, objectStorage, time);
        List<String> manifestKeys = new ArrayList<>();
        List<String> linkedKeys = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            long linkedObjectId = 10_000L + i;
            String manifestKey = ArchiveObjectKey.manifestKey(
                STREAM0, i, i + 1L, ObjectAttributes.Type.Composite, linkedObjectId, 1L);
            String linkedKey = writeArchiveComposite(manifestKey, linkedObjectId, i);
            manifestKeys.add(manifestKey);
            linkedKeys.add(linkedKey);
        }
        String malformedKey = ArchiveObjectKey.manifestPrefix(STREAM0) + "non-canonical";
        objectStorage.write(new ObjectStorage.WriteOptions(), malformedKey,
            Unpooled.wrappedBuffer(new byte[] {1})).get();
        manifestKeys.add(malformedKey);
        S3StreamArchiveRecord archive = (S3StreamArchiveRecord) archiveMetadata(
            0L, 101L, 101L, 101L, 101L, 0L, 0L).toRecord().message();
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        manager.replay(archive);
        when(objectControlManager.markDestroyObjects(List.of(), List.of()))
            .thenReturn(ControllerResult.atomicOf(List.of(), true));
        replay(manager, manager.deleteStream(new DeleteStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0)).records());
        assertNotNull(manager.getStreamArchiveMetadata(STREAM0));

        manager = new StreamControlManager(quorumController, new SnapshotRegistry(new LogContext()),
            new LogContext(), objectControlManager, clusterControlManager, featureControlManager,
            replicationControlManager, objectStorage, time);
        when(quorumController.isActive()).thenReturn(true);
        manager.replay(archive);
        manager.reconcileStreamArchiveMetadataCleanup();

        assertNotNull(manager.getStreamArchiveMetadata(STREAM0));
        awaitCondition(() -> manifestKeys.stream().noneMatch(objectStorage::contains));
        linkedKeys.forEach(key -> assertFalse(objectStorage.contains(key)));
        time.sleep(TimeUnit.MINUTES.toMillis(5));
        manager.reconcileStreamArchiveMetadataCleanup();
        awaitCondition(() -> manager.getStreamArchiveMetadata(STREAM0) == null);
        manifestKeys.forEach(key -> assertFalse(objectStorage.contains(key)));
        org.mockito.ArgumentCaptor<ObjectStorage.ListOptions> options =
            org.mockito.ArgumentCaptor.forClass(ObjectStorage.ListOptions.class);
        verify(objectStorage, Mockito.atLeast(3)).list(options.capture());
        assertTrue(options.getAllValues().stream().allMatch(value -> value.maxKeys() == 100));
    }

    /**
     * Given unpublished prepared Archive work, verify an empty prefix must remain quiescent for five
     * minutes, an observed late write is deleted and restarts the window, and a write after durable
     * task removal is the explicitly accepted leak boundary.
     */
    @Test
    public void testDeletedPreparedArchiveRequiresQuiescenceAndRecordsAcceptedLateLeak() throws Exception {
        registerAlwaysSuccessEpoch(BROKER0);
        when(quorumController.isActive()).thenReturn(true);
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        manager.replay((S3StreamArchiveRecord) archiveMetadata(
            0L, 50L, 50L, 100L, 500L, 0L, 0L).toRecord().message());
        when(objectControlManager.markDestroyObjects(List.of(), List.of()))
            .thenReturn(ControllerResult.atomicOf(List.of(), true));
        replay(manager, manager.deleteStream(new DeleteStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0)).records());
        manager.reconcileStreamArchiveMetadataCleanup();
        assertNotNull(manager.getStreamArchiveMetadata(STREAM0));
        List<ArchiveUpdate> postDeleteUpdates = List.of(
            archiveUpdate(0L, 50L, 100L, 500L, 0L, 0L),
            archiveUpdate(0L, 100L, 100L, 600L, 0L, 0L),
            archiveUpdate(0L, 50L, 100L, 500L, 25L, 100L),
            archiveUpdate(25L, 50L, 100L, 400L, 25L, 0L));
        postDeleteUpdates.forEach(update -> assertEquals(Errors.STREAM_NOT_EXIST.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, update).response().errorCode()));

        time.sleep(TimeUnit.MINUTES.toMillis(4));
        String lateManifest = ArchiveObjectKey.manifestPrefix(STREAM0) + "late-prepared";
        writeArchiveComposite(lateManifest, 20_000L, 0L);
        manager.reconcileStreamArchiveMetadataCleanup();
        assertTrue(objectStorage.contains(lateManifest));

        time.sleep(TimeUnit.MINUTES.toMillis(1));
        manager.reconcileStreamArchiveMetadataCleanup();
        awaitCondition(() -> !objectStorage.contains(lateManifest));

        time.sleep(TimeUnit.MINUTES.toMillis(5));
        manager.reconcileStreamArchiveMetadataCleanup();
        awaitCondition(() -> manager.getStreamArchiveMetadata(STREAM0) == null);

        String acceptedLeak = ArchiveObjectKey.manifestPrefix(STREAM0) + "accepted-late-leak";
        writeArchiveComposite(acceptedLeak, 30_000L, 0L);
        manager.reconcileStreamArchiveMetadataCleanup();
        assertTrue(objectStorage.contains(acceptedLeak));
    }

    private String writeArchiveComposite(String manifestKey, long linkedObjectId, long startOffset)
        throws Exception {
        String linkedKey = ObjectUtils.genKey(0, linkedObjectId);
        objectStorage.write(new ObjectStorage.WriteOptions(), linkedKey,
            Unpooled.wrappedBuffer(new byte[] {1})).get();
        CompositeObjectWriter writer = new CompositeObjectWriter(
            objectStorage.writer(new ObjectStorage.WriteOptions(), manifestKey));
        writer.addComponent(new S3ObjectMetadata(linkedObjectId,
                ObjectAttributes.builder().bucket(objectStorage.bucketId()).build().attributes()),
            List.of(new DataBlockIndex(STREAM0, startOffset, 1, 1, 0L, 1)));
        writer.close().get();
        return linkedKey;
    }

    private void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadlineNs) {
            Thread.sleep(10L);
        }
        assertTrue(condition.getAsBoolean());
    }

    /**
     * Given compaction replaces the selected objects first, verify Archive prepare rejects the stale object list.
     */
    @Test
    public void testArchivePrepareConflictsAfterCompaction() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        addCommittedComposite(10L, 0L, 50L);
        addCommittedComposite(11L, 50L, 100L);
        mockSuccessfulObjectCommits();

        ControllerResult<CommitStreamObjectResponseData> compaction = manager.commitStreamObject(
            commitStreamObject(12L, 0L, 100L, List.of(10L, 11L)));
        assertEquals(Errors.NONE.code(), compaction.response().errorCode());
        replay(manager, compaction.records());

        ArchiveUpdate prepare = archiveUpdate(0L, 0L, 100L, 0L, 0L, 0L)
            .setArchiveObjectIds(List.of(10L, 11L));
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepare).response().errorCode());
    }

    /**
     * Given Archive prepare freezes an online range, verify every overlapping layout mutation is rejected.
     */
    @Test
    public void testArchivePrepareFencesOverlappingCompactionVariants() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        addCommittedComposite(10L, 0L, 50L);
        addCommittedComposite(11L, 50L, 100L);
        mockSuccessfulObjectCommits();
        ControllerResult<UpdateStreamResponse> prepare = manager.updateStreamArchive(BROKER0, BROKER_EPOCH0,
            archiveUpdate(0L, 0L, 100L, 0L, 0L, 0L).setArchiveObjectIds(List.of(10L, 11L)));
        replay(manager, prepare.records());

        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.commitStreamObject(
            commitStreamObject(12L, 0L, 100L, List.of(10L, 11L))).response().errorCode());
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.commitStreamObject(
            commitStreamObject(10L, 0L, 50L, List.of(10L))).response().errorCode());
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.commitStreamObject(
            commitStreamObject(NOOP_OBJECT_ID, ObjectUtils.NOOP_OFFSET, ObjectUtils.NOOP_OFFSET, List.of(10L)))
                .response().errorCode());
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.commitStreamObject(
            commitStreamObject(13L, 25L, 75L, Collections.emptyList())).response().errorCode());
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.commitStreamObject(
            commitStreamObject(14L, 100L, 150L, List.of(11L))).response().errorCode());

        verify(objectControlManager, never()).commitObject(anyLong(), anyLong(), anyLong(), anyInt());
        verify(objectControlManager, never()).replaceCommittedObject(anyLong(), anyInt());
        verify(objectControlManager, never()).markDestroyObjects(anyList(), anyList());

        assertEquals(Errors.NONE.code(), manager.commitStreamObject(
            commitStreamObject(15L, 100L, 150L, Collections.emptyList())).response().errorCode());
    }

    /**
     * Given a well-formed prepare payload, verify only the exact continuous committed Stream Object sequence is accepted.
     */
    @Test
    public void testArchivePrepareValidatesCurrentObjectSequence() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        addCommittedComposite(10L, 0L, 50L);
        addCommittedComposite(11L, 50L, 100L);

        ArchiveUpdate prepare = archiveUpdate(0L, 0L, 100L, 0L, 0L, 0L);
        prepare.setArchiveObjectIds(List.of(11L, 10L));
        assertArchiveConflict(prepare);
        prepare.setArchiveObjectIds(List.of(10L));
        assertArchiveConflict(prepare);
        prepare.setArchiveObjectIds(List.of(10L, 11L));
        prepare.setArchivePreparedEndOffset(75L);
        assertArchiveConflict(prepare);
        prepare.setArchivePreparedEndOffset(100L);

        when(objectControlManager.getObject(11L)).thenReturn(null);
        assertArchiveConflict(prepare);
        when(objectControlManager.getObject(11L)).thenReturn(new S3Object(11L, 999L, 0L,
            S3ObjectState.PREPARED,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes()));
        assertArchiveConflict(prepare);
        addCommittedComposite(11L, 60L, 100L);
        assertArchiveConflict(prepare);
        addCommittedComposite(11L, 50L, 100L);
        assertEquals(Errors.NONE.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepare).response().errorCode());
    }

    /**
     * Given a prepare was already persisted, verify an equal retry trusts the range fence established by prepare.
     */
    @Test
    public void testIdempotentArchivePrepareUsesPersistedIntent() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        addCommittedComposite(10L, 0L, 50L);
        ArchiveUpdate prepare = archiveUpdate(0L, 0L, 50L, 0L, 0L, 0L)
            .setArchiveObjectIds(List.of(10L));
        ControllerResult<UpdateStreamResponse> result =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepare);
        replay(manager, result.records());

        manager.replay(new RemoveS3StreamObjectRecord().setStreamId(STREAM0).setObjectId(10L));
        ControllerResult<UpdateStreamResponse> retry =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepare);
        assertEquals(Errors.NONE.code(), retry.response().errorCode());
        assertTrue(retry.records().isEmpty());
    }

    /**
     * Given an unreadable operation payload and a stale cursor, verify they return distinct non-retriable errors.
     */
    @Test
    public void testUpdateStreamArchiveInvalidRequestAndConflict() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);

        ArchiveUpdate malformed = new ArchiveUpdate().setStreamId(STREAM0).setStreamEpoch(EPOCH0)
            .setOperation(StreamArchiveOperationType.ARCHIVE_PUBLISH.value());
        assertEquals(Errors.INVALID_REQUEST.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, malformed).response().errorCode());

        ArchiveUpdate stale = archiveUpdate(0L, 10L, 10L, 100L, 0L, 0L);
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, stale).response().errorCode());
    }

    /**
     * Given published data and cleanup intent, verify cleanup prepare and commit are exact full-state transitions.
     */
    @Test
    public void testUpdateStreamArchiveCleanupTransitions() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, List.of(archiveMetadata(0L, 0L, 50L, 50L, 100L, 0L, 0L).toRecord()));

        ArchiveUpdate prepareCleanup = archiveUpdate(0L, 50L, 50L, 100L, 25L, 40L);
        ControllerResult<UpdateStreamResponse> prepareResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, prepareCleanup);
        assertEquals(Errors.NONE.code(), prepareResult.response().errorCode());
        replay(manager, prepareResult.records());

        ArchiveUpdate commitCleanup = archiveUpdate(25L, 50L, 50L, 60L, 25L, 0L);
        ControllerResult<UpdateStreamResponse> commitResult =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, commitCleanup);
        assertEquals(Errors.NONE.code(), commitResult.response().errorCode());
        replay(manager, commitResult.records());
        assertEquals(25L, manager.getStreamArchiveMetadata(STREAM0).archiveStartOffset());
        assertEquals(60L, manager.getStreamArchiveMetadata(STREAM0).archiveSize());
    }

    /**
     * Given one Archive lifecycle has a durable prepare intent, verify the Controller rejects a stale request that
     * attempts to prepare the other lifecycle concurrently.
     */
    @Test
    public void testArchiveAndCleanupPrepareAreMutuallyExclusive() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, List.of(archiveMetadata(0L, 0L, 50L, 100L, 100L, 0L, 0L).toRecord()));
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.updateStreamArchive(
            BROKER0, BROKER_EPOCH0, archiveUpdate(0L, 50L, 50L, 100L, 25L, 40L))
            .response().errorCode());
    }

    /**
     * Given retention ahead of an empty Archive, verify the cursor advances only to a current online boundary.
     */
    @Test
    public void testUpdateStreamArchiveEmptyCursorTransition() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, List.of(archiveMetadata(0L, 0L, 0L, 0L, 0L, 0L, 0L).toRecord()));
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setEpoch(EPOCH0)
            .setRangeIndex(0).setStartOffset(100L).setStreamState(StreamState.OPENED.toByte()));
        manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(10L)
            .setStartOffset(50L).setEndOffset(150L));

        ArchiveUpdate advance = archiveUpdate(50L, 50L, 50L, 0L, 50L, 0L);
        ControllerResult<UpdateStreamResponse> result =
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, advance);
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        replay(manager, result.records());
        assertEquals(50L, manager.getStreamArchiveMetadata(STREAM0).archiveStartOffset());
        assertEquals(0L, manager.getStreamArchiveMetadata(STREAM0).archiveMetadataEndOffset());
        assertTrue(manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, advance).records().isEmpty());

        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.updateStreamArchive(
            BROKER0, BROKER_EPOCH0, archiveUpdate(75L, 75L, 75L, 0L, 75L, 0L))
            .response().errorCode());
    }

    /**
     * Given expired and living Stream Objects, verify the Controller accepts the boundary of the object containing
     * Stream start.
     */
    @Test
    public void testUpdateStreamArchiveEmptyCursorAcceptsLivingStreamObjectBoundary() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, List.of(archiveMetadata(0L, 0L, 0L, 0L, 0L, 0L, 0L).toRecord()));
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setEpoch(EPOCH0)
            .setRangeIndex(0).setStartOffset(100L).setStreamState(StreamState.OPENED.toByte()));
        manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(10L)
            .setStartOffset(50L).setEndOffset(80L));
        manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(11L)
            .setStartOffset(80L).setEndOffset(150L));

        ControllerResult<UpdateStreamResponse> result = manager.updateStreamArchive(
            BROKER0, BROKER_EPOCH0, archiveUpdate(80L, 80L, 80L, 0L, 80L, 0L));

        assertEquals(Errors.NONE.code(), result.response().errorCode());
    }

    /**
     * Given no living Stream Object, verify the Controller rejects empty Archive cursor advancement.
     */
    @Test
    public void testUpdateStreamArchiveEmptyCursorWaitsWithoutLivingStreamObject() {
        registerAlwaysSuccessEpoch(BROKER0);
        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, List.of(archiveMetadata(0L, 0L, 0L, 0L, 0L, 0L, 0L).toRecord()));
        manager.replay(new S3StreamRecord().setStreamId(STREAM0).setEpoch(EPOCH0)
            .setRangeIndex(0).setStartOffset(100L).setStreamState(StreamState.OPENED.toByte()));

        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.updateStreamArchive(
            BROKER0, BROKER_EPOCH0, archiveUpdate(99L, 99L, 99L, 0L, 99L, 0L))
            .response().errorCode());
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(), manager.updateStreamArchive(
            BROKER0, BROKER_EPOCH0, archiveUpdate(100L, 100L, 100L, 0L, 100L, 0L))
            .response().errorCode());
    }

    /**
     * Given overlapping failures, verify Archive validation uses the specified error precedence.
     */
    @Test
    public void testUpdateStreamArchiveErrorPrecedence() {
        ArchiveUpdate malformed = archiveUpdate(-1L, 0L, 0L, 0L, 0L, 0L);
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V5);
        assertEquals(Errors.UNSUPPORTED_VERSION.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, malformed).response().errorCode());

        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        assertEquals(Errors.NODE_EPOCH_NOT_EXIST.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, malformed).response().errorCode());

        registerAlwaysSuccessEpoch(BROKER0);
        assertEquals(Errors.STREAM_NOT_EXIST.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, malformed).response().errorCode());

        createAndOpenStream(BROKER0, EPOCH0);
        malformed.setStreamEpoch(EPOCH1);
        assertEquals(Errors.STREAM_FENCED.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, malformed).response().errorCode());
    }

    private S3StreamArchiveMetadata archiveMetadata(long startOffset, long metadataEndOffset, long endOffset,
        long preparedEndOffset, long size, long cleanupEndOffset, long cleanupSize) {
        return new S3StreamArchiveMetadata(STREAM0, startOffset, metadataEndOffset, endOffset,
            preparedEndOffset, size, cleanupEndOffset, cleanupSize);
    }

    private ArchiveUpdate archiveUpdate(long startOffset, long endOffset,
        long preparedEndOffset, long size, long cleanupEndOffset, long cleanupSize) {
        S3StreamArchiveMetadata current = manager.getStreamArchiveMetadata(STREAM0);
        long currentStart = current == null ? 0L : current.archiveStartOffset();
        long currentEnd = current == null ? 0L : current.archiveEndOffset();
        ArchiveUpdate operation = new ArchiveUpdate().setStreamId(STREAM0).setStreamEpoch(EPOCH0);
        if (preparedEndOffset > endOffset) {
            return operation.setOperation(StreamArchiveOperationType.ARCHIVE_PREPARE.value())
                .setArchivePrepare(new ArchivePrepare().setExpectedArchiveEndOffset(endOffset)
                    .setArchivePreparedEndOffset(preparedEndOffset));
        }
        if (cleanupSize > 0) {
            return operation.setOperation(StreamArchiveOperationType.CLEANUP_PREPARE.value())
                .setCleanupPrepare(new UpdateStreamArchiveRequestData.CleanupPrepare()
                    .setExpectedArchiveStartOffset(startOffset).setArchiveCleanupEndOffset(cleanupEndOffset)
                    .setArchiveCleanupSize(cleanupSize));
        }
        if (current != null && current.archiveCleanupSize() > 0 && startOffset == cleanupEndOffset) {
            return operation.setOperation(StreamArchiveOperationType.CLEANUP_COMMIT.value())
                .setCleanupCommit(new UpdateStreamArchiveRequestData.CleanupCommit()
                    .setExpectedArchiveStartOffset(currentStart).setArchiveCleanupEndOffset(cleanupEndOffset));
        }
        if (startOffset == endOffset && size == 0 && startOffset > currentStart) {
            return operation.setOperation(StreamArchiveOperationType.ADVANCE_EMPTY_CURSOR.value())
                .setAdvanceEmptyCursor(new UpdateStreamArchiveRequestData.AdvanceEmptyCursor()
                    .setExpectedArchiveOffset(currentStart).setNewArchiveOffset(startOffset));
        }
        return operation.setOperation(StreamArchiveOperationType.ARCHIVE_PUBLISH.value())
            .setArchivePublish(new UpdateStreamArchiveRequestData.ArchivePublish()
                .setExpectedArchiveEndOffset(currentEnd).setArchiveEndOffset(endOffset).setArchiveSize(size));
    }

    private static final class ArchiveUpdate extends StreamArchiveOperation {
        @Override
        public ArchiveUpdate setStreamId(long streamId) {
            super.setStreamId(streamId);
            return this;
        }

        @Override
        public ArchiveUpdate setStreamEpoch(long streamEpoch) {
            super.setStreamEpoch(streamEpoch);
            return this;
        }

        @Override
        public ArchiveUpdate setOperation(byte operation) {
            super.setOperation(operation);
            return this;
        }

        @Override
        public ArchiveUpdate setArchivePrepare(ArchivePrepare payload) {
            super.setArchivePrepare(payload);
            return this;
        }

        @Override
        public ArchiveUpdate setCleanupPrepare(UpdateStreamArchiveRequestData.CleanupPrepare payload) {
            super.setCleanupPrepare(payload);
            return this;
        }

        @Override
        public ArchiveUpdate setCleanupCommit(UpdateStreamArchiveRequestData.CleanupCommit payload) {
            super.setCleanupCommit(payload);
            return this;
        }

        @Override
        public ArchiveUpdate setAdvanceEmptyCursor(UpdateStreamArchiveRequestData.AdvanceEmptyCursor payload) {
            super.setAdvanceEmptyCursor(payload);
            return this;
        }

        @Override
        public ArchiveUpdate setArchivePublish(UpdateStreamArchiveRequestData.ArchivePublish payload) {
            super.setArchivePublish(payload);
            return this;
        }

        ArchiveUpdate setArchiveObjectIds(List<Long> objectIds) {
            archivePrepare().setArchiveObjectIds(objectIds);
            return this;
        }

        ArchiveUpdate setArchivePreparedEndOffset(long offset) {
            archivePrepare().setArchivePreparedEndOffset(offset);
            return this;
        }
    }

    private ControllerResult<CommitStreamSetObjectResponseData> commitRange(long objectId, long startOffset,
        long endOffset) {
        return commitRange(BROKER0, BROKER_EPOCH0, objectId, startOffset, endOffset);
    }

    private ControllerResult<CommitStreamSetObjectResponseData> commitRange(int nodeId, long nodeEpoch,
        long objectId, long startOffset, long endOffset) {
        return manager.commitStreamSetObject(new CommitStreamSetObjectRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setObjectId(objectId)
            .setObjectSize(999L)
            .setOrderId(objectId)
            .setObjectStreamRanges(List.of(new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(startOffset)
                .setEndOffset(endOffset))));
    }

    private void mockSuccessfulObjectCommits() {
        when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).thenAnswer(args -> {
            long objectId = args.getArgument(0);
            return ControllerResult.of(List.of(new ApiMessageAndVersion(new S3ObjectRecord()
                .setObjectId(objectId).setObjectState(S3ObjectState.COMMITTED.toByte()), (short) 0)), Errors.NONE);
        });
        when(objectControlManager.markDestroyObjects(anyList(), anyList()))
            .thenReturn(ControllerResult.of(Collections.emptyList(), true));
    }

    private void addCommittedComposite(long objectId, long startOffset, long endOffset) {
        manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(objectId)
            .setStartOffset(startOffset).setEndOffset(endOffset));
        when(objectControlManager.getObject(objectId)).thenReturn(new S3Object(objectId, 999L, 0L,
            S3ObjectState.COMMITTED,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes()));
    }

    private void addCommittedNormal(long objectId, long startOffset, long endOffset) {
        manager.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(objectId)
            .setStartOffset(startOffset).setEndOffset(endOffset));
        when(objectControlManager.getObject(objectId)).thenReturn(new S3Object(objectId, 999L, 0L,
            S3ObjectState.COMMITTED,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Normal).build().attributes()));
    }

    private void assertArchiveConflict(ArchiveUpdate update) {
        assertEquals(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code(),
            manager.updateStreamArchive(BROKER0, BROKER_EPOCH0, update).response().errorCode());
    }

    private CommitStreamObjectRequestData commitStreamObject(long objectId, long startOffset, long endOffset,
        List<Long> sourceObjectIds) {
        return new CommitStreamObjectRequestData()
            .setNodeId(BROKER0)
            .setNodeEpoch(BROKER_EPOCH0)
            .setObjectId(objectId)
            .setObjectSize(999L)
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset)
            .setAttributes(ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes())
            .setSourceObjectIds(sourceObjectIds);
    }

    private long createStream() {
        CreateStreamRequest request0 = new CreateStreamRequest();
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, result0.records());
        return result0.response().streamId();
    }

    private long createStream(Map<String, String> tagMap) {
        CreateStreamRequest request0 = new CreateStreamRequest()
            .setTags(new CreateStreamsRequestData.TagCollection(
                tagMap.entrySet()
                    .stream()
                    .map(e -> new CreateStreamsRequestData.Tag().setKey(e.getKey()).setValue(e.getValue()))
                    .collect(Collectors.toList())
                    .iterator()));
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        replay(manager, result0.records());
        return result0.response().streamId();
    }

    private void openStream(int nodeId, long epoch, long streamId) {
        ControllerResult<OpenStreamResponse> result1 = manager.openStream(nodeId, 0,
            new OpenStreamRequest().setStreamId(streamId).setStreamEpoch(epoch));
        replay(manager, result1.records());
    }

    private void closeStream(int nodeId, long epoch, long streamId) {
        ControllerResult<CloseStreamResponse> result = manager.closeStream(nodeId, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(streamId)
            .setStreamEpoch(epoch));
        replay(manager, result.records());
    }

    private void createAndOpenStream(int nodeId, long epoch) {
        long streamId = createStream();
        openStream(nodeId, epoch, streamId);
    }

    @Test
    public void testCommitWalCompacted() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(ControllerResult.of(Collections.emptyList(), Errors.NONE));
        Mockito.when(objectControlManager.markDestroyObjects(anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), true));
        registerAlwaysSuccessEpoch(BROKER0);

        // 1. create and open stream_0 and stream_1
        createAndOpenStream(BROKER0, EPOCH0);
        createAndOpenStream(BROKER0, EPOCH0);

        // 2. commit first level stream set object of stream_0 and stream_1
        List<ObjectStreamRange> streamRanges0 = List.of(
            new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0L)
                .setEndOffset(100L),
            new ObjectStreamRange()
                .setStreamId(STREAM1)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0L)
                .setEndOffset(200L));
        CommitStreamSetObjectRequestData commitRequest0 = new CommitStreamSetObjectRequestData()
            .setObjectId(0L)
            .setOrderId(0L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges0);
        ControllerResult<CommitStreamSetObjectResponseData> result4 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result4.response().errorCode());
        replay(manager, result4.records());

        // 3. fetch range end offset
        GetOpeningStreamsRequestData request = new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(0L);
        GetOpeningStreamsResponseData streamsOffset = manager.getOpeningStreams(request).response();
        assertEquals(2, streamsOffset.streamMetadataList().size());
        assertEquals(STREAM0, streamsOffset.streamMetadataList().get(0).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(0).startOffset());
        assertEquals(100L, streamsOffset.streamMetadataList().get(0).endOffset());
        assertEquals(STREAM1, streamsOffset.streamMetadataList().get(1).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(1).startOffset());
        assertEquals(200L, streamsOffset.streamMetadataList().get(1).endOffset());
        long object0DataTs = manager.nodesMetadata().get(BROKER0).streamSetObjects().get(0L).dataTimeInMs();

        // 4. keep committing first level object of stream_0 and stream_1
        List<ObjectStreamRange> streamRanges1 = List.of(
            new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(100L)
                .setEndOffset(200L),
            new ObjectStreamRange()
                .setStreamId(STREAM1)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(200L)
                .setEndOffset(300L));
        CommitStreamSetObjectRequestData commitRequest1 = new CommitStreamSetObjectRequestData()
            .setObjectId(1L)
            .setOrderId(1L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges1);
        ControllerResult<CommitStreamSetObjectResponseData> result5 = manager.commitStreamSetObject(commitRequest1);
        assertEquals(Errors.NONE.code(), result5.response().errorCode());
        replay(manager, result5.records());

        // 5. fetch range end offset
        streamsOffset = manager.getOpeningStreams(request).response();
        assertEquals(2, streamsOffset.streamMetadataList().size());
        assertEquals(STREAM0, streamsOffset.streamMetadataList().get(0).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(0).startOffset());
        assertEquals(200L, streamsOffset.streamMetadataList().get(0).endOffset());
        assertEquals(STREAM1, streamsOffset.streamMetadataList().get(1).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(1).startOffset());
        assertEquals(300L, streamsOffset.streamMetadataList().get(1).endOffset());
        long object1DataTs = manager.nodesMetadata().get(BROKER0).streamSetObjects().get(1L).dataTimeInMs();

        // 6. commit an invalid stream set object which contains the destroyed or not exist stream set object
        Mockito.when(objectControlManager.markDestroyObjects(anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), false));
        List<ObjectStreamRange> streamRanges2 = List.of(
            new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0L)
                .setEndOffset(200L),
            new ObjectStreamRange()
                .setStreamId(STREAM1)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0L)
                .setEndOffset(300L));
        CommitStreamSetObjectRequestData commitRequest2 = new CommitStreamSetObjectRequestData()
            .setObjectId(2L)
            .setOrderId(0L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges2)
            .setCompactedObjectIds(List.of(0L, 1L, 10L));
        ControllerResult<CommitStreamSetObjectResponseData> result6 = manager.commitStreamSetObject(commitRequest2);
        assertEquals(Errors.COMPACTED_OBJECTS_NOT_FOUND.code(), result6.response().errorCode());
        assertEquals(0, result6.records().size());
        Mockito.when(objectControlManager.markDestroyObjects(anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), true));

        // 7. commit a second level stream set object which compact wal_0 and wal_1
        commitRequest2 = new CommitStreamSetObjectRequestData()
            .setObjectId(2L)
            .setOrderId(0L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges2)
            .setCompactedObjectIds(List.of(0L, 1L));
        result6 = manager.commitStreamSetObject(commitRequest2);
        assertEquals(Errors.NONE.code(), result6.response().errorCode());
        replay(manager, result6.records());

        // 8. fetch range end offset
        streamsOffset = manager.getOpeningStreams(request).response();
        assertEquals(2, streamsOffset.streamMetadataList().size());
        assertEquals(STREAM0, streamsOffset.streamMetadataList().get(0).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(0).startOffset());
        assertEquals(200L, streamsOffset.streamMetadataList().get(0).endOffset());
        assertEquals(STREAM1, streamsOffset.streamMetadataList().get(1).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(1).startOffset());
        assertEquals(300L, streamsOffset.streamMetadataList().get(1).endOffset());
        assertEquals(object0DataTs, manager.nodesMetadata().get(BROKER0).streamSetObjects().get(2L).dataTimeInMs());

        // 9. verify compacted stream set objects is removed
        assertEquals(1, manager.nodesMetadata().get(BROKER0).streamSetObjects().size());
        assertEquals(2, manager.nodesMetadata().get(BROKER0).streamSetObjects().get(2L).objectId());
        assertEquals(0, manager.nodesMetadata().get(BROKER0).streamSetObjects().get(2L).orderId());

    }

    @Test
    public void testCommitWalWithStreamObject() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(ControllerResult.of(Collections.emptyList(), Errors.NONE));
        Mockito.when(objectControlManager.markDestroyObjects(anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), true));
        registerAlwaysSuccessEpoch(BROKER0);

        // 1. create and open stream_0 and stream_1
        createAndOpenStream(BROKER0, EPOCH0);
        createAndOpenStream(BROKER0, EPOCH0);

        // 2. commit a wal with stream_0 and a stream object with stream_1 that is split out from wal
        List<ObjectStreamRange> streamRanges0 = List.of(
            new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0L)
                .setEndOffset(100L));
        CommitStreamSetObjectRequestData commitRequest0 = new CommitStreamSetObjectRequestData()
            .setObjectId(0L)
            .setOrderId(0L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges0)
            .setStreamObjects(List.of(
                new StreamObject()
                    .setStreamId(STREAM1)
                    .setObjectId(1L)
                    .setObjectSize(999)
                    .setStartOffset(0L)
                    .setEndOffset(200L)
            ));
        ControllerResult<CommitStreamSetObjectResponseData> result4 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result4.response().errorCode());
        replay(manager, result4.records());

        // 3. fetch range end offset
        GetOpeningStreamsRequestData request = new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(0L);
        GetOpeningStreamsResponseData streamsOffset = manager.getOpeningStreams(request).response();
        assertEquals(2, streamsOffset.streamMetadataList().size());
        assertEquals(STREAM0, streamsOffset.streamMetadataList().get(0).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(0).startOffset());
        assertEquals(100L, streamsOffset.streamMetadataList().get(0).endOffset());
        assertEquals(STREAM1, streamsOffset.streamMetadataList().get(1).streamId());
        assertEquals(0L, streamsOffset.streamMetadataList().get(1).startOffset());
        assertEquals(200L, streamsOffset.streamMetadataList().get(1).endOffset());

        // 4. verify stream object is added
        assertEquals(1, manager.streamsMetadata().get(STREAM1).streamObjects().size());

        // 5. commit stream set object with not continuous stream
        List<ObjectStreamRange> streamRanges1 = List.of(
            new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(99L)
                .setEndOffset(200L));
        CommitStreamSetObjectRequestData commitRequest1 = new CommitStreamSetObjectRequestData()
            .setObjectId(1L)
            .setOrderId(1L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges1)
            .setStreamObjects(List.of(
                new StreamObject()
                    .setStreamId(STREAM1)
                    .setObjectId(2L)
                    .setObjectSize(999)
                    .setStartOffset(200L)
                    .setEndOffset(400L)
            ));
        ControllerResult<CommitStreamSetObjectResponseData> result5 = manager.commitStreamSetObject(commitRequest1);
        assertEquals(Errors.OFFSET_NOT_MATCHED.code(), result5.response().errorCode());
    }

    @Test
    public void testCommitStreamObjectForFencedStream() {
        registerAlwaysSuccessEpoch(BROKER0);
        long streamId = createStream();
        openStream(BROKER0, EPOCH1, streamId);
        CommitStreamObjectRequestData streamObjectRequest = new CommitStreamObjectRequestData()
            .setObjectId(3L)
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(0L)
            .setEndOffset(400L)
            .setObjectSize(999)
            .setSourceObjectIds(List.of(1L, 2L));
        ControllerResult<CommitStreamObjectResponseData> result = manager.commitStreamObject(streamObjectRequest);
        assertEquals(Errors.STREAM_FENCED.code(), result.response().errorCode());
    }

    @Test
    public void testCommitStreamObject() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(ControllerResult.of(Collections.emptyList(), Errors.NONE));
        Mockito.when(objectControlManager.markDestroyObjects(anyList(), anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), true));
        registerAlwaysSuccessEpoch(BROKER0);

        // 1. create and open stream_0 and stream_1
        createAndOpenStream(BROKER0, EPOCH0);
        createAndOpenStream(BROKER0, EPOCH0);

        // 2. commit a wal with stream_0 and a stream object with stream_1 that is split out from wal
        List<ObjectStreamRange> streamRanges0 = List.of(
            new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0L)
                .setEndOffset(100L));
        CommitStreamSetObjectRequestData commitRequest0 = new CommitStreamSetObjectRequestData()
            .setObjectId(0L)
            .setOrderId(0L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges0)
            .setStreamObjects(List.of(
                new StreamObject()
                    .setStreamId(STREAM1)
                    .setObjectId(1L)
                    .setObjectSize(999)
                    .setStartOffset(0L)
                    .setEndOffset(200L)
            ));
        ControllerResult<CommitStreamSetObjectResponseData> result0 = manager.commitStreamSetObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result0.response().errorCode());
        replay(manager, result0.records());

        // 3. commit a wal with stream_0 and a stream object with stream_1 that is split out from wal
        List<ObjectStreamRange> streamRanges1 = List.of(
            new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(100L)
                .setEndOffset(200L));
        CommitStreamSetObjectRequestData commitRequest1 = new CommitStreamSetObjectRequestData()
            .setObjectId(2L)
            .setOrderId(1L)
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges1)
            .setStreamObjects(List.of(
                new StreamObject()
                    .setStreamId(STREAM1)
                    .setObjectId(3L)
                    .setObjectSize(999)
                    .setStartOffset(200L)
                    .setEndOffset(400L)
            ));
        ControllerResult<CommitStreamSetObjectResponseData> result1 = manager.commitStreamSetObject(commitRequest1);
        assertEquals(Errors.NONE.code(), result1.response().errorCode());
        replay(manager, result1.records());

        // 4. compact these two stream objects
        CommitStreamObjectRequestData streamObjectRequest = new CommitStreamObjectRequestData()
            .setObjectId(4L)
            .setStreamId(STREAM1)
            .setStartOffset(0L)
            .setEndOffset(400L)
            .setObjectSize(999)
            .setSourceObjectIds(List.of(1L, 3L));
        ControllerResult<CommitStreamObjectResponseData> result2 = manager.commitStreamObject(streamObjectRequest);
        assertEquals(Errors.NONE.code(), result2.response().errorCode());
        replay(manager, result2.records());

        // 5. fetch stream offset range
        GetOpeningStreamsRequestData request = new GetOpeningStreamsRequestData().setNodeId(BROKER0).setNodeEpoch(0L);
        GetOpeningStreamsResponseData response = manager.getOpeningStreams(request).response();
        assertEquals(2, response.streamMetadataList().size());
        assertEquals(STREAM0, response.streamMetadataList().get(0).streamId());
        assertEquals(0L, response.streamMetadataList().get(0).startOffset());
        assertEquals(200L, response.streamMetadataList().get(0).endOffset());
        assertEquals(STREAM1, response.streamMetadataList().get(1).streamId());
        assertEquals(0L, response.streamMetadataList().get(1).startOffset());
        assertEquals(400L, response.streamMetadataList().get(1).endOffset());

        // 6. compact a stream object from invalid source object
        Mockito.when(objectControlManager.markDestroyObjects(anyList(), anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), false));
        streamObjectRequest = new CommitStreamObjectRequestData()
            .setObjectId(5L)
            .setStreamId(STREAM1)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(400L)
            .setEndOffset(1000L)
            .setObjectSize(999)
            .setSourceObjectIds(List.of(10L));
        result2 = manager.commitStreamObject(streamObjectRequest);
        assertEquals(Errors.COMPACTED_OBJECTS_NOT_FOUND.code(), result2.response().errorCode());
        replay(manager, result2.records());

        // 7. verify stream objects
        assertEquals(1, manager.streamsMetadata().get(STREAM1).streamObjects().size());
        assertEquals(4L, manager.streamsMetadata().get(STREAM1).streamObjects().get(4L).objectId());
        assertEquals(0L, manager.streamsMetadata().get(STREAM1).streamObjects().get(4L).streamOffsetRange().startOffset());
        assertEquals(400L, manager.streamsMetadata().get(STREAM1).streamObjects().get(4L).streamOffsetRange().endOffset());
    }

    private void mockData0() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(ControllerResult.of(Collections.emptyList(), Errors.NONE));
        Mockito.when(objectControlManager.markDestroyObjects(anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), true));
        Mockito.when(objectControlManager.markDestroyObjects(anyList(), anyList())).thenReturn(ControllerResult.of(Collections.emptyList(), true));
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);

        // 1. create and open stream0 and stream1 for node0
        createAndOpenStream(BROKER0, EPOCH0);
        createAndOpenStream(BROKER0, EPOCH0);
        // 2. commit stream set object with stream0-[0, 10)
        CommitStreamSetObjectRequestData requestData = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setOrderId(0)
            .setObjectId(0)
            .setObjectStreamRanges(List.of(new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0)
                .setEndOffset(10)));
        ControllerResult<CommitStreamSetObjectResponseData> result = manager.commitStreamSetObject(requestData);
        replay(manager, result.records());
        // 3. commit stream set object with stream0-[10, 20), and stream1-[0, 10)
        requestData = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setOrderId(1)
            .setObjectId(1)
            .setObjectStreamRanges(List.of(new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(10)
                .setEndOffset(20), new ObjectStreamRange()
                .setStreamId(STREAM1)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(0)
                .setEndOffset(10)));
        result = manager.commitStreamSetObject(requestData);
        replay(manager, result.records());
        // 4. commit with a stream object with stream0-[20, 40)
        requestData = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER0)
            .setObjectSize(999)
            .setOrderId(S3StreamConstant.INVALID_ORDER_ID)
            .setObjectId(ObjectUtils.NOOP_OBJECT_ID)
            .setStreamObjects(List.of(new StreamObject()
                .setStreamId(STREAM0)
                .setObjectSize(999)
                .setObjectId(2)
                .setStartOffset(20)
                .setEndOffset(40)));
        result = manager.commitStreamSetObject(requestData);
        replay(manager, result.records());
        // 5. node0 close stream0 and node1 open stream0
        closeStream(BROKER0, EPOCH0, STREAM0);
        openStream(BROKER1, EPOCH1, STREAM0);
        // 6. commit stream set object with stream0-[40, 70)
        requestData = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER1)
            .setObjectSize(999)
            .setObjectId(3)
            .setOrderId(3)
            .setObjectStreamRanges(List.of(new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH1)
                .setStartOffset(40)
                .setEndOffset(70)));
        result = manager.commitStreamSetObject(requestData);
        replay(manager, result.records());
    }

    @Test
    public void testTrim() {
        mockData0();

        // 1. trim stream0 to [60, ..)
        TrimStreamRequest trimRequest = new TrimStreamRequest()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH1)
            .setNewStartOffset(60);
        ControllerResult<TrimStreamResponse> result1 = manager.trimStream(BROKER1, BROKER_EPOCH0, trimRequest);
        assertEquals(Errors.NONE.code(), result1.response().errorCode());
        replay(manager, result1.records());

        // 2. verify
        StreamRuntimeMetadata streamMetadata = manager.streamsMetadata().get(STREAM0);
        assertEquals(60, streamMetadata.startOffset());
        assertEquals(1, streamMetadata.ranges().size());
        RangeMetadata rangeMetadata = streamMetadata.currentRangeMetadata();
        assertEquals(1, rangeMetadata.rangeIndex());
        assertEquals(60, rangeMetadata.startOffset());
        assertEquals(70, rangeMetadata.endOffset());
        assertEquals(1, streamMetadata.streamObjects().size());

        // 3. trim stream0 to [100, ..)
        trimRequest = new TrimStreamRequest()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH1)
            .setNewStartOffset(100);
        result1 = manager.trimStream(BROKER1, BROKER_EPOCH0, trimRequest);
        assertEquals(Errors.NONE.code(), result1.response().errorCode());
        replay(manager, result1.records());

        // 4. verify
        streamMetadata = manager.streamsMetadata().get(STREAM0);
        assertEquals(100, streamMetadata.startOffset());
        assertEquals(100, streamMetadata.endOffset());
        assertEquals(1, streamMetadata.ranges().size());
        rangeMetadata = streamMetadata.currentRangeMetadata();
        assertEquals(1, rangeMetadata.rangeIndex());
        assertEquals(100, rangeMetadata.startOffset());
        assertEquals(100, rangeMetadata.endOffset());
        assertEquals(1, streamMetadata.streamObjects().size());

        // 5. commit stream set object with stream0-[70, 100)
        CommitStreamSetObjectRequestData requestData = new CommitStreamSetObjectRequestData()
            .setNodeId(BROKER1)
            .setObjectSize(999)
            .setObjectId(4)
            .setOrderId(4)
            .setObjectStreamRanges(List.of(new ObjectStreamRange()
                .setStreamId(STREAM0)
                .setStreamEpoch(EPOCH0)
                .setStartOffset(70)
                .setEndOffset(100)));
        ControllerResult<CommitStreamSetObjectResponseData> result = manager.commitStreamSetObject(requestData);
        replay(manager, result.records());

        // 6. verify
        streamMetadata = manager.streamsMetadata().get(STREAM0);
        assertEquals(100, streamMetadata.startOffset());
        assertEquals(1, streamMetadata.ranges().size());
        rangeMetadata = streamMetadata.currentRangeMetadata();
        assertEquals(1, rangeMetadata.rangeIndex());
        assertEquals(100, rangeMetadata.startOffset());
        assertEquals(100, streamMetadata.endOffset());
    }

    @Test
    public void testDelete() {
        mockData0();

        // 1. delete a OPEN status stream
        DeleteStreamRequest req = new DeleteStreamRequest()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH1);
        ControllerResult<DeleteStreamResponse> result = manager.deleteStream(req);
        assertEquals(Errors.STREAM_NOT_CLOSED.code(), result.response().errorCode());
        replay(manager, result.records());

        // 2. close the stream
        replay(manager, manager.closeStream(BROKER1, EPOCH1, new CloseStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH1)).records());

        req = new DeleteStreamRequest()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH1);
        result = manager.deleteStream(req);
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(1, result.records().size());
        replay(manager, result.records());

        assertNull(manager.streamsMetadata().get(STREAM0));

        assertEquals(2, manager.nodesMetadata().get(BROKER0).streamSetObjects().size());

        // 3. delete again
        req = new DeleteStreamRequest()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH1);
        result = manager.deleteStream(req);
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(0, result.records().size());
    }

    @Test
    public void testDescribeStreams() {
        // 1. describe stream by stream id
        DescribeStreamsRequestData request = new DescribeStreamsRequestData()
            .setStreamId(0);
        DescribeStreamsResponseData result = manager.describeStreams(request);
        assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
        assertEquals(0, result.streamMetadataList().size());

        registerAlwaysSuccessEpoch(BROKER0);
        long streamId = createStream();

        request = new DescribeStreamsRequestData()
            .setStreamId(streamId);
        result = manager.describeStreams(request);
        assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
        assertEquals(1, result.streamMetadataList().size());

        assertEquals(streamId, result.streamMetadataList().get(0).streamId());
        assertEquals(-1, result.streamMetadataList().get(0).nodeId());
        assertEquals(StreamState.CLOSED.name(), result.streamMetadataList().get(0).state());

        assertEquals(Uuid.ZERO_UUID, result.streamMetadataList().get(0).topicId());
        assertEquals("", result.streamMetadataList().get(0).topicName());
        assertEquals(-1, result.streamMetadataList().get(0).partitionIndex());

        // 2. describe stream by node id
        request = new DescribeStreamsRequestData()
            .setNodeId(BROKER2);
        result = manager.describeStreams(request);
        assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
        assertEquals(0, result.streamMetadataList().size());

        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER1, EPOCH1);

        request = new DescribeStreamsRequestData()
            .setNodeId(BROKER1);
        result = manager.describeStreams(request);
        assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
        assertEquals(1, result.streamMetadataList().size());

        assertEquals(BROKER1, result.streamMetadataList().get(0).nodeId());
        assertEquals(StreamState.OPENED.name(), result.streamMetadataList().get(0).state());

        // 3. describe stream by topic partition
        request = new DescribeStreamsRequestData()
            .setTopicPartitions(List.of(new DescribeStreamsRequestData.TopicPartitionData()
                .setTopicName(TOPIC)
                .setPartitions(List.of(new DescribeStreamsRequestData.PartitionData().setPartitionIndex(PARTITION)))));
        result = manager.describeStreams(request);
        assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
        assertEquals(0, result.streamMetadataList().size());

        createStream(Map.of(StreamTags.Topic.KEY, TOPIC_ID.toString(), StreamTags.Partition.KEY, String.valueOf(PARTITION)));
        result = manager.describeStreams(request);
        assertEquals(Errors.NONE, Errors.forCode(result.errorCode()));
        assertEquals(1, result.streamMetadataList().size());

        assertEquals(TOPIC_ID, result.streamMetadataList().get(0).topicId());
        assertEquals(TOPIC, result.streamMetadataList().get(0).topicName());
        assertEquals(PARTITION, result.streamMetadataList().get(0).partitionIndex());
    }

    @Test
    public void testGetOpeningStreams() {
        // 1. create stream without register
        CreateStreamRequest request0 = new CreateStreamRequest()
            .setNodeId(BROKER0);
        ControllerResult<CreateStreamResponse> result0 = manager.createStream(BROKER0, BROKER_EPOCH0, request0);
        assertEquals(Errors.NODE_EPOCH_NOT_EXIST, Errors.forCode(result0.response().errorCode()));

        // 2. register
        GetOpeningStreamsRequestData request1 = new GetOpeningStreamsRequestData()
            .setNodeId(BROKER0)
            .setNodeEpoch(1);
        ControllerResult<GetOpeningStreamsResponseData> result1 = manager.getOpeningStreams(request1);
        assertEquals(Errors.NONE, Errors.forCode(result1.response().errorCode()));
        assertEquals(0, result1.response().streamMetadataList().size());
        replay(manager, result1.records());

        replay(manager, manager.getOpeningStreams(new GetOpeningStreamsRequestData()
            .setNodeId(BROKER1)
            .setNodeEpoch(2)).records());

        // 3. register with lower epoch again
        request1 = new GetOpeningStreamsRequestData()
            .setNodeId(BROKER0)
            .setNodeEpoch(0);
        result1 = manager.getOpeningStreams(request1);
        assertEquals(Errors.NODE_EPOCH_EXPIRED, Errors.forCode(result1.response().errorCode()));

        // 4. register with higher epoch
        request1 = new GetOpeningStreamsRequestData()
            .setNodeId(BROKER0)
            .setNodeEpoch(2);
        result1 = manager.getOpeningStreams(request1);
        assertEquals(Errors.NONE, Errors.forCode(result1.response().errorCode()));
        assertEquals(0, result1.response().streamMetadataList().size());
        replay(manager, result1.records());

        // 5. verify node's epoch
        assertEquals(2, manager.nodesMetadata().get(BROKER0).getNodeEpoch());

        // 6. create stream with lower epoch
        CreateStreamRequest request2 = new CreateStreamRequest()
            .setNodeId(BROKER0);
        ControllerResult<CreateStreamResponse> result2 = manager.createStream(BROKER0, BROKER_EPOCH0, request2);
        assertEquals(Errors.NODE_EPOCH_EXPIRED, Errors.forCode(result2.response().errorCode()));

        // 7. create stream with matched epoch
        ControllerResult<CreateStreamResponse> result3 = manager.createStream(BROKER0, 2,
            new CreateStreamRequest().setNodeId(BROKER0));
        assertEquals(Errors.NONE, Errors.forCode(result3.response().errorCode()));
        replay(manager, result3.records());

        replay(manager, manager.createStream(BROKER0, 2, new CreateStreamRequest().setNodeId(BROKER0)).records());

        replay(manager, manager.openStream(BROKER0, 2, new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(0L)).records());
        replay(manager, manager.openStream(BROKER0, 2, new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(0L)).records());

        List<Long> streams = manager.getOpeningStreams(BROKER0).stream().map(StreamRuntimeMetadata::streamId).sorted().collect(Collectors.toList());
        assertEquals(List.of(STREAM0, STREAM1), streams);

        replay(manager, manager.closeStream(BROKER0, 2, new CloseStreamRequest().setStreamId(STREAM1).setStreamEpoch(0L)).records());
        streams = manager.getOpeningStreams(BROKER0).stream().map(StreamRuntimeMetadata::streamId).sorted().collect(Collectors.toList());
        assertEquals(List.of(STREAM0), streams);

        replay(manager, manager.openStream(BROKER1, 2, new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(1L)).records());
        streams = manager.getOpeningStreams(BROKER0).stream().map(StreamRuntimeMetadata::streamId).sorted().collect(Collectors.toList());
        assertEquals(List.of(STREAM0), streams);
        streams = manager.getOpeningStreams(BROKER1).stream().map(StreamRuntimeMetadata::streamId).sorted().collect(Collectors.toList());
        assertEquals(List.of(STREAM1), streams);

        replay(manager, manager.closeStream(BROKER1, 2, new CloseStreamRequest().setStreamId(STREAM1).setStreamEpoch(1L)).records());
        replay(manager, manager.openStream(BROKER0, 2, new OpenStreamRequest().setStreamId(STREAM1).setStreamEpoch(2L)).records());
        streams = manager.getOpeningStreams(BROKER0).stream().map(StreamRuntimeMetadata::streamId).sorted().collect(Collectors.toList());
        assertEquals(List.of(STREAM0, STREAM1), streams);
        streams = manager.getOpeningStreams(BROKER1).stream().map(StreamRuntimeMetadata::streamId).sorted().collect(Collectors.toList());
        assertEquals(Collections.emptyList(), streams);
    }

    @Test
    public void testCleanupScaleInNodes() throws ExecutionException, InterruptedException {
        when(objectControlManager.commitObject(anyLong(), anyLong(), anyLong(), anyInt())).thenReturn(ControllerResult.of(Collections.emptyList(), null));

        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        createAndOpenStream(BROKER0, 0);
        createAndOpenStream(BROKER0, 0);
        ControllerResult<?> rst = manager.commitStreamSetObject(new CommitStreamSetObjectRequestData().setNodeId(BROKER0).setObjectId(1L)
            .setStreamObjects(Collections.emptyList())
            .setObjectStreamRanges(List.of(
                new ObjectStreamRange().setStreamId(STREAM0).setStartOffset(0).setEndOffset(100),
                new ObjectStreamRange().setStreamId(STREAM1).setStartOffset(0).setEndOffset(100)
            )));
        replay(manager, rst.records());

        closeStream(BROKER0, 0, STREAM0);
        openStream(BROKER1, 1, STREAM0);
        rst = manager.commitStreamSetObject(new CommitStreamSetObjectRequestData().setNodeId(BROKER1).setObjectId(2L)
            .setStreamObjects(Collections.emptyList())
            .setObjectStreamRanges(List.of(
                new ObjectStreamRange().setStreamId(STREAM0).setStartOffset(100).setEndOffset(200)
            )));
        replay(manager, rst.records());
        rst = manager.commitStreamSetObject(new CommitStreamSetObjectRequestData().setNodeId(BROKER1).setObjectId(3L)
            .setStreamObjects(Collections.emptyList())
            .setObjectStreamRanges(List.of(
                new ObjectStreamRange().setStreamId(STREAM0).setStartOffset(200).setEndOffset(300)
            )));
        replay(manager, rst.records());

        rst = manager.trimStream(BROKER1, 0, new TrimStreamRequest().setStreamId(STREAM0).setNewStartOffset(200L).setStreamEpoch(1));
        replay(manager, rst.records());

        when(clusterControlManager.isActive(eq(1))).thenReturn(false);
        when(clusterControlManager.isActive(eq(2))).thenReturn(true);

        ApiMessageAndVersion record = new ApiMessageAndVersion(new S3ObjectRecord().setObjectId(2L).setObjectState(S3ObjectState.MARK_DESTROYED.toByte()), (short) 0);
        when(objectControlManager.markDestroyObjects(eq(List.of(2L)))).thenReturn(ControllerResult.of(List.of(record), null));

        when(objectControlManager.objectReader(eq(1L))).thenReturn(mockObjectReader(List.of(
            new StreamOffsetRange(STREAM0, 0, 100), new StreamOffsetRange(STREAM1, 0, 100))));
        when(objectControlManager.objectReader(eq(2L))).thenReturn(mockObjectReader(List.of(
            new StreamOffsetRange(STREAM0, 100, 200))));
        when(objectControlManager.objectReader(eq(3L))).thenReturn(mockObjectReader(List.of(
            new StreamOffsetRange(STREAM0, 200, 300))));

        manager = spy(manager);
        manager.cleanupScaleInNodes();

        verify(manager, timeout(1000).times(3)).checkStreamSetObjectExpired(any(), anyList());
        verify(objectControlManager, times(1)).markDestroyObjects(eq(List.of(2L)));
    }

    private void registerAlwaysSuccessEpoch(int nodeId) {
        GetOpeningStreamsRequestData req = new GetOpeningStreamsRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(-1);
        ControllerResult<GetOpeningStreamsResponseData> result = manager.getOpeningStreams(req);
        replay(manager, result.records());
    }

    private CrossedRecoveryState recoverCrossedOwnership(List<Integer> failoverOrder) {
        setUp();
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        mockSuccessfulObjectCommits();
        registerAlwaysSuccessEpoch(BROKER0);
        registerAlwaysSuccessEpoch(BROKER1);
        registerAlwaysSuccessEpoch(BROKER2);

        createAndOpenStream(BROKER0, EPOCH0);
        replay(manager, manager.closeStream(BROKER0, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM0).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER1, EPOCH1, STREAM0);

        createAndOpenStream(BROKER1, EPOCH0);
        replay(manager, manager.closeStream(BROKER1, BROKER_EPOCH0, new CloseStreamRequest()
            .setStreamId(STREAM1).setStreamEpoch(EPOCH0).setEndOffset(100L)).records());
        openStream(BROKER0, EPOCH1, STREAM1);

        failoverOrder.forEach(this::recoverCrossedNode);

        assertFalse(manager.hasOpeningStreams(BROKER0));
        assertFalse(manager.hasOpeningStreams(BROKER1));
        openStream(BROKER2, EPOCH2, STREAM0);
        openStream(BROKER2, EPOCH2, STREAM1);
        return new CrossedRecoveryState(
            List.of(crossedStreamState(STREAM0), crossedStreamState(STREAM1)),
            List.of(nodeObjectState(BROKER0), nodeObjectState(BROKER1)));
    }

    private void recoverCrossedNode(int nodeId) {
        ControllerResult<GetOpeningStreamsResponseData> barrier = manager.getOpeningStreams(
            new GetOpeningStreamsRequestData().setNodeId(nodeId).setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true));
        assertEquals(Errors.NONE.code(), barrier.response().errorCode());
        assertEquals(2, barrier.response().streamMetadataList().size());
        replay(manager, barrier.records());

        long historicalStreamId = nodeId == BROKER0 ? STREAM0 : STREAM1;
        long currentStreamId = nodeId == BROKER0 ? STREAM1 : STREAM0;
        ControllerResult<CommitStreamSetObjectResponseData> commit = manager.commitStreamSetObject(
            new CommitStreamSetObjectRequestData()
                .setNodeId(nodeId)
                .setNodeEpoch(BROKER_EPOCH0)
                .setFailoverMode(true)
                .setObjectId(10L + nodeId)
                .setOrderId(10L + nodeId)
                .setObjectSize(999L)
                .setObjectStreamRanges(List.of(
                    new ObjectStreamRange().setStreamId(historicalStreamId).setStreamEpoch(EPOCH0)
                        .setStartOffset(0L).setEndOffset(100L),
                    new ObjectStreamRange().setStreamId(currentStreamId).setStreamEpoch(EPOCH1)
                        .setStartOffset(100L).setEndOffset(200L))));
        assertEquals(Errors.NONE.code(), commit.response().errorCode());
        replay(manager, commit.records());

        for (GetOpeningStreamsResponseData.StreamMetadata stream : barrier.response().streamMetadataList()) {
            ControllerResult<CloseStreamResponse> close = manager.closeStream(nodeId, BROKER_EPOCH0,
                new CloseStreamRequest().setStreamId(stream.streamId()).setStreamEpoch(stream.epoch()));
            Errors closeError = Errors.forCode(close.response().errorCode());
            assertTrue(closeError == Errors.NONE || closeError == Errors.STREAM_FENCED);
            replay(manager, close.records());
        }
    }

    private CrossedStreamState crossedStreamState(long streamId) {
        StreamRuntimeMetadata stream = manager.streamsMetadata().get(streamId);
        return new CrossedStreamState(streamId, stream.currentState(), stream.currentEpoch(),
            stream.startOffset(), stream.endOffset(), stream.currentRangeOwner());
    }

    private CrossedNodeObjectState nodeObjectState(int nodeId) {
        return new CrossedNodeObjectState(nodeId,
            manager.nodesMetadata().get(nodeId).streamSetObjects().values().stream()
                .map(object -> new CrossedObjectState(object.objectId(), object.nodeId(), object.orderId(),
                    object.offsetRangeList()))
                .sorted((left, right) -> Long.compare(left.objectId(), right.objectId()))
                .collect(Collectors.toList()));
    }

    private record CrossedRecoveryState(
        List<CrossedStreamState> streams,
        List<CrossedNodeObjectState> nodes
    ) {
    }

    private record CrossedStreamState(
        long streamId,
        StreamState state,
        long epoch,
        long startOffset,
        long endOffset,
        int owner
    ) {
    }

    private record CrossedNodeObjectState(int nodeId, List<CrossedObjectState> objects) {
    }

    private record CrossedObjectState(
        long objectId,
        int nodeId,
        long orderId,
        List<StreamOffsetRange> ranges
    ) {
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void replay(StreamControlManager manager, List<ApiMessageAndVersion> records) {
        List<ApiMessage> messages = records.stream().map(x -> x.message())
            .collect(Collectors.toList());
        for (ApiMessage message : messages) {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            switch (type) {
                case ASSIGNED_STREAM_ID_RECORD:
                    manager.replay((AssignedStreamIdRecord) message);
                    break;
                case S3_STREAM_RECORD:
                    manager.replay((S3StreamRecord) message);
                    break;
                case REMOVE_S3_STREAM_RECORD:
                    manager.replay((RemoveS3StreamRecord) message);
                    break;
                case RANGE_RECORD:
                    manager.replay((RangeRecord) message);
                    break;
                case REMOVE_RANGE_RECORD:
                    manager.replay((RemoveRangeRecord) message);
                    break;
                case NODE_WALMETADATA_RECORD:
                    manager.replay((NodeWALMetadataRecord) message);
                    break;
                case REMOVE_NODE_WALMETADATA_RECORD:
                    manager.replay((RemoveNodeWALMetadataRecord) message);
                    break;
                case S3_STREAM_SET_OBJECT_RECORD:
                    manager.replay((S3StreamSetObjectRecord) message);
                    break;
                case REMOVE_STREAM_SET_OBJECT_RECORD:
                    manager.replay((RemoveStreamSetObjectRecord) message);
                    break;
                case S3_STREAM_OBJECT_RECORD:
                    manager.replay((S3StreamObjectRecord) message);
                    break;
                case REMOVE_S3_STREAM_OBJECT_RECORD:
                    manager.replay((RemoveS3StreamObjectRecord) message);
                    break;
                case S3_OBJECT_RECORD:
                case REMOVE_S3_OBJECT_RECORD:
                    break;
                case S3_STREAM_END_OFFSETS_RECORD:
                    manager.replay((S3StreamEndOffsetsRecord) message);
                    break;
                case S3_STREAM_ARCHIVE_RECORD:
                    manager.replay((S3StreamArchiveRecord) message);
                    break;
                case REMOVE_S3_STREAM_ARCHIVE_RECORD:
                    manager.replay((RemoveS3StreamArchiveRecord) message);
                    break;
                case NODE_WALUNCOMMITTED_OFFSETS_RECORD:
                    manager.replay((NodeWALUncommittedOffsetsRecord) message);
                    break;
                default:
                    throw new IllegalStateException("Unknown metadata record type " + type);
            }
        }
    }

    private void verifyInitializedStreamMetadata(StreamRuntimeMetadata metadata) {
        assertNotNull(metadata);
        assertEquals(S3StreamConstant.INIT_EPOCH, metadata.currentEpoch());
        assertEquals(S3StreamConstant.INIT_RANGE_INDEX, metadata.currentRangeIndex());
        assertEquals(S3StreamConstant.INIT_START_OFFSET, metadata.startOffset());
    }

    private void createAndOpenStream0() {
        registerAlwaysSuccessEpoch(BROKER0);
        ControllerResult<CreateStreamResponse> createResult = manager.createStream(BROKER0, BROKER_EPOCH0,
            new CreateStreamRequest().setNodeId(BROKER0));
        replay(manager, createResult.records());
        ControllerResult<OpenStreamResponse> openResult = manager.openStream(BROKER0, BROKER_EPOCH0,
            new OpenStreamRequest().setStreamId(STREAM0).setStreamEpoch(EPOCH0));
        replay(manager, openResult.records());
    }

    private void verifyFirstTimeOpenStreamResult(ControllerResult<OpenStreamResponse> result,
        long expectedEpoch, int expectedNodeId) {
        assertEquals(0, result.response().errorCode());
        assertEquals(0, result.response().startOffset());
        assertEquals(2, result.records().size());

        // first record must be stream update record
        ApiMessageAndVersion record0 = result.records().get(0);
        assertInstanceOf(S3StreamRecord.class, record0.message());
        S3StreamRecord streamRecord0 = (S3StreamRecord) record0.message();
        assertEquals(expectedEpoch, streamRecord0.epoch());
        assertEquals(0, streamRecord0.rangeIndex());
        assertEquals(0L, streamRecord0.startOffset());

        // second record must be range create record
        ApiMessageAndVersion record1 = result.records().get(1);
        assertInstanceOf(RangeRecord.class, record1.message());
        RangeRecord rangeRecord0 = (RangeRecord) record1.message();
        assertEquals(expectedNodeId, rangeRecord0.nodeId());
        assertEquals(expectedEpoch, rangeRecord0.epoch());
        assertEquals(0, rangeRecord0.rangeIndex());
        assertEquals(0L, rangeRecord0.startOffset());
        assertEquals(0L, rangeRecord0.endOffset());
    }

    private void verifyFirstRange(StreamRuntimeMetadata streamMetadata, long expectedEpoch, int expectedNodeId) {
        assertNotNull(streamMetadata);
        assertEquals(expectedEpoch, streamMetadata.currentEpoch());
        assertEquals(0, streamMetadata.currentRangeIndex());
        assertEquals(0L, streamMetadata.startOffset());
        assertEquals(1, streamMetadata.ranges().size());
        RangeMetadata rangeMetadata0 = streamMetadata.ranges().get(0);
        assertEquals(expectedNodeId, rangeMetadata0.nodeId());
        assertEquals(expectedEpoch, rangeMetadata0.epoch());
        assertEquals(0, rangeMetadata0.rangeIndex());
        assertEquals(0L, rangeMetadata0.startOffset());
        assertEquals(0L, rangeMetadata0.endOffset());
    }

    private Optional<ObjectReader> mockObjectReader(
        List<StreamOffsetRange> ranges) throws ExecutionException, InterruptedException {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage();
        ObjectWriter objectWriter = new ObjectWriter.DefaultObjectWriter(1, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE, new ObjectStorage.WriteOptions());
        ranges.forEach(range ->
            objectWriter.write(
                range.streamId(),
                List.of(
                    StreamRecordBatch.of(range.streamId(), 0, range.startOffset(), (int) (range.endOffset() - range.startOffset()), Unpooled.buffer(1), DefaultByteBufSupplier.INSTANCE)
                )
            )
        );
        objectWriter.close().get();
        return Optional.of(new ObjectReader.DefaultObjectReader(new S3ObjectMetadata(1, objectWriter.size(), S3ObjectType.STREAM_SET), objectStorage));
    }
}
