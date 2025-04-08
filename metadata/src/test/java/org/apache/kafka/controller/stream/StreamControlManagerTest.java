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
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.FeatureControlManager;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 40)
@Tag("S3Unit")
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

    @BeforeEach
    public void setUp() {
        LogContext context = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(context);
        quorumController = mock(QuorumController.class);

        objectControlManager = mock(S3ObjectControlManager.class);
        clusterControlManager = mock(ClusterControlManager.class);
        featureControlManager = mock(FeatureControlManager.class);
        replicationControlManager = mock(ReplicationControlManager.class);
        when(featureControlManager.autoMQVersion()).thenReturn(AutoMQVersion.LATEST);
        when(replicationControlManager.getTopicId(TOPIC)).thenReturn(TOPIC_ID);
        when(replicationControlManager.getTopic(TOPIC_ID)).thenReturn(new ReplicationControlManager.TopicControlInfo(TOPIC, new SnapshotRegistry(new LogContext()), TOPIC_ID));

        manager = new StreamControlManager(quorumController, registry, context, objectControlManager,
            clusterControlManager, featureControlManager, replicationControlManager);
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

        ControllerResult<CommitStreamSetObjectResponseData> commitRst = manager.commitStreamSetObject(new CommitStreamSetObjectRequestData().setNodeId(BROKER0).setNodeEpoch(EPOCH0).setObjectStreamRanges(
            List.of(
                new ObjectStreamRange().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setStartOffset(0).setEndOffset(10L)
            )
        ));
        replay(manager, commitRst.records());
        assertEquals(10L, manager.streamsMetadata().get(STREAM0).endOffset());
        assertEquals((short) 0, commitRst.response().errorCode());
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
        assertEquals(1, streamMetadata.ranges().size());
        rangeMetadata = streamMetadata.currentRangeMetadata();
        assertEquals(1, rangeMetadata.rangeIndex());
        assertEquals(70, rangeMetadata.startOffset());
        assertEquals(70, rangeMetadata.endOffset());
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
        assertEquals(70, rangeMetadata.startOffset());
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
                    break;
                case S3_STREAM_END_OFFSETS_RECORD:
                    manager.replay((S3StreamEndOffsetsRecord) message);
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
                    new StreamRecordBatch(range.streamId(), 0, range.startOffset(), (int) (range.endOffset() - range.startOffset()), Unpooled.buffer(1))
                )
            )
        );
        objectWriter.close().get();
        return Optional.of(new ObjectReader.DefaultObjectReader(new S3ObjectMetadata(1, objectWriter.size(), S3ObjectType.STREAM_SET), objectStorage));
    }
}
