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

package org.apache.kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyLong;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.CloseStreamRequestData;
import org.apache.kafka.common.message.CloseStreamResponseData;
import org.apache.kafka.common.message.CommitWALObjectRequestData;
import org.apache.kafka.common.message.CommitWALObjectRequestData.ObjectStreamRange;
import org.apache.kafka.common.message.CommitWALObjectResponseData;
import org.apache.kafka.common.message.CreateStreamRequestData;
import org.apache.kafka.common.message.CreateStreamResponseData;
import org.apache.kafka.common.message.GetStreamsOffsetRequestData;
import org.apache.kafka.common.message.GetStreamsOffsetResponseData;
import org.apache.kafka.common.message.OpenStreamRequestData;
import org.apache.kafka.common.message.OpenStreamResponseData;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.BrokerWALMetadataRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.S3ObjectControlManager;
import org.apache.kafka.controller.stream.S3StreamConstant;
import org.apache.kafka.controller.stream.StreamControlManager;
import org.apache.kafka.controller.stream.StreamControlManager.S3StreamMetadata;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@Timeout(value = 40)
@Tag("S3Unit")
public class StreamControlManagerTest {

    private final static long STREAM0 = 0;
    private final static long STREAM1 = 1;
    private final static long STREAM2 = 2;

    private final static int BROKER0 = 0;
    private final static int BROKER1 = 1;
    private final static int BROKER2 = 2;

    private final static long EPOCH0 = 0;
    private final static long EPOCH1 = 1;
    private final static long EPOCH2 = 2;

    private StreamControlManager manager;
    private S3ObjectControlManager objectControlManager;

    @BeforeEach
    public void setUp() {
        LogContext context = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(context);
        objectControlManager = Mockito.mock(S3ObjectControlManager.class);
        manager = new StreamControlManager(registry, context, objectControlManager);
    }

    @Test
    public void testBasicCreateStream() {
        // 1. create stream_0 success
        CreateStreamRequestData request0 = new CreateStreamRequestData();
        ControllerResult<CreateStreamResponseData> result0 = manager.createStream(request0);
        List<ApiMessageAndVersion> records0 = result0.records();
        CreateStreamResponseData response0 = result0.response();
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
        Map<Long, S3StreamMetadata> streamsMetadata =
            manager.streamsMetadata();
        assertEquals(1, streamsMetadata.size());
        verifyInitializedStreamMetadata(streamsMetadata.get(STREAM0));
        assertEquals(1, manager.nextAssignedStreamId());

        // 2. create stream_1
        CreateStreamRequestData request1 = new CreateStreamRequestData();
        ControllerResult<CreateStreamResponseData> result1 = manager.createStream(request1);
        List<ApiMessageAndVersion> records1 = result1.records();
        CreateStreamResponseData response1 = result1.response();
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
        // 1. create stream_0 and stream_1
        CreateStreamRequestData request0 = new CreateStreamRequestData();
        ControllerResult<CreateStreamResponseData> result0 = manager.createStream(request0);
        replay(manager, result0.records());
        CreateStreamRequestData request1 = new CreateStreamRequestData();
        ControllerResult<CreateStreamResponseData> result1 = manager.createStream(request1);
        replay(manager, result1.records());

        // verify the streams are created
        Map<Long, S3StreamMetadata> streamsMetadata = manager.streamsMetadata();
        assertEquals(2, streamsMetadata.size());
        verifyInitializedStreamMetadata(streamsMetadata.get(STREAM0));
        verifyInitializedStreamMetadata(streamsMetadata.get(STREAM1));
        assertEquals(2, manager.nextAssignedStreamId());

        // 2. broker_0 open stream_0 and stream_1 with epoch0
        ControllerResult<OpenStreamResponseData> result2 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
        ControllerResult<OpenStreamResponseData> result3 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
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
        S3StreamMetadata streamMetadata0 = manager.streamsMetadata().get(STREAM0);
        verifyFirstRange(manager.streamsMetadata().get(STREAM0), EPOCH0, BROKER0);
        verifyFirstRange(manager.streamsMetadata().get(STREAM1), EPOCH0, BROKER0);

        // TODO: support write range record, then roll the range and verify
        // 3. broker_1 try to open stream_0 with epoch0
        ControllerResult<OpenStreamResponseData> result4 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setBrokerId(BROKER1));
        assertEquals(Errors.STREAM_FENCED.code(), result4.response().errorCode());
        assertEquals(0, result4.records().size());

        // 4. broker_0 try to open stream_1 with epoch0
        ControllerResult<OpenStreamResponseData> result6 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
        assertEquals(Errors.NONE.code(), result6.response().errorCode());
        assertEquals(0L, result6.response().startOffset());
        assertEquals(0L, result6.response().nextOffset());
        assertEquals(0, result6.records().size());

        // 5. broker_0 try to open stream_1 with epoch1
        ControllerResult<OpenStreamResponseData> result7 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH1).setBrokerId(BROKER0));
        assertEquals(Errors.STREAM_NOT_CLOSED.code(), result7.response().errorCode());

        // 6. broker_1 try to open stream_1 with epoch0
        ControllerResult<OpenStreamResponseData> result8 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER1));
        assertEquals(Errors.STREAM_FENCED.code(), result8.response().errorCode());
        assertEquals(0, result8.records().size());

        // 7. broker_1 try to open stream_1 with epoch1
        ControllerResult<OpenStreamResponseData> result9 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH1).setBrokerId(BROKER1));
        assertEquals(Errors.STREAM_NOT_CLOSED.code(), result9.response().errorCode());

        // 8. broker_1 try to close stream_1 with epoch0
        ControllerResult<CloseStreamResponseData> result10 = manager.closeStream(
            new CloseStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER1));
        assertEquals(Errors.STREAM_FENCED.code(), result10.response().errorCode());

        // 9. broker_0 try to close stream_1 with epoch1
        ControllerResult<CloseStreamResponseData> result11 = manager.closeStream(
            new CloseStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH1).setBrokerId(BROKER0));
        assertEquals(Errors.STREAM_INNER_ERROR.code(), result11.response().errorCode());

        // 10. broker_0 try to close stream_1 with epoch0
        ControllerResult<CloseStreamResponseData> result12 = manager.closeStream(
            new CloseStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
        assertEquals(Errors.NONE.code(), result12.response().errorCode());
        replay(manager, result12.records());

        // 11. broker_0 try to close stream_1 with epoch0 again
        ControllerResult<CloseStreamResponseData> result13 = manager.closeStream(
            new CloseStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
        assertEquals(Errors.NONE.code(), result13.response().errorCode());

        // 12. broker_1 try to open stream_1 with epoch1
        ControllerResult<OpenStreamResponseData> result14 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH1).setBrokerId(BROKER1));
        assertEquals(Errors.NONE.code(), result14.response().errorCode());
        replay(manager, result14.records());

        // 13. verify the stream_1 metadata are updated, and the range_1 is created
        S3StreamMetadata streamMetadata1 = manager.streamsMetadata().get(STREAM1);
        assertEquals(EPOCH1, streamMetadata1.currentEpoch());
        RangeMetadata range = streamMetadata1.ranges().get(streamMetadata1.currentRangeIndex());
        assertEquals(EPOCH1, range.epoch());
        assertEquals(BROKER1, range.brokerId());
    }

    @Test
    public void testCommitWal() {
        Mockito.when(objectControlManager.commitObject(anyLong(), anyLong())).then(ink -> {
            long objectId = ink.getArgument(0);
            if (objectId == 1) {
                return ControllerResult.of(Collections.emptyList(), false);
            }
            return ControllerResult.of(Collections.emptyList(), true);
        });
        // 1. create and open stream_0
        CreateStreamRequestData request0 = new CreateStreamRequestData();
        ControllerResult<CreateStreamResponseData> result0 = manager.createStream(request0);
        replay(manager, result0.records());
        ControllerResult<OpenStreamResponseData> result2 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
        verifyFirstTimeOpenStreamResult(result2, EPOCH0, BROKER0);
        replay(manager, result2.records());
        // 2. commit valid wal object
        List<ObjectStreamRange> streamRanges0 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(0L)
            .setEndOffset(100L));
        CommitWALObjectRequestData commitRequest0 = new CommitWALObjectRequestData()
            .setObjectId(0L)
            .setBrokerId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges0);
        ControllerResult<CommitWALObjectResponseData> result3 = manager.commitWALObject(commitRequest0);
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        replay(manager, result3.records());
        // verify range's end offset advanced and wal object is added
        S3StreamMetadata streamMetadata0 = manager.streamsMetadata().get(STREAM0);
        assertEquals(1, streamMetadata0.ranges().size());
        RangeMetadata rangeMetadata0 = streamMetadata0.ranges().get(0);
        assertEquals(0L, rangeMetadata0.startOffset());
        assertEquals(100L, rangeMetadata0.endOffset());
        assertEquals(1, manager.brokersMetadata().get(BROKER0).walObjects().size());
        // 3. commit a wal object that doesn't exist
        List<ObjectStreamRange> streamRanges1 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH0)
            .setStartOffset(100)
            .setEndOffset(200));
        CommitWALObjectRequestData commitRequest1 = new CommitWALObjectRequestData()
            .setObjectId(1L)
            .setBrokerId(BROKER0)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges1);
        ControllerResult<CommitWALObjectResponseData> result4 = manager.commitWALObject(commitRequest1);
        assertEquals(Errors.OBJECT_NOT_EXIST.code(), result4.response().errorCode());
        // 4. broker_0 close stream_0 with epoch_0 and broker_1 open stream_0 with epoch_1
        ControllerResult<CloseStreamResponseData> result7 = manager.closeStream(
            new CloseStreamRequestData().setStreamId(STREAM0).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
        assertEquals(Errors.NONE.code(), result7.response().errorCode());
        replay(manager, result7.records());
        ControllerResult<OpenStreamResponseData> result8 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM0).setStreamEpoch(EPOCH1).setBrokerId(BROKER1));
        assertEquals(Errors.NONE.code(), result8.response().errorCode());
        assertEquals(0L, result8.response().startOffset());
        assertEquals(100L, result8.response().nextOffset());
        replay(manager, result8.records());
        // 5. broker_1 successfully commit wal object which contains stream_0's data
        List<ObjectStreamRange> streamRanges6 = List.of(new ObjectStreamRange()
            .setStreamId(STREAM0)
            .setStreamEpoch(EPOCH1)
            .setStartOffset(100)
            .setEndOffset(300));
        CommitWALObjectRequestData commitRequest6 = new CommitWALObjectRequestData()
            .setBrokerId(BROKER1)
            .setObjectId(6L)
            .setObjectSize(999)
            .setObjectStreamRanges(streamRanges6);
        ControllerResult<CommitWALObjectResponseData> result10 = manager.commitWALObject(commitRequest6);
        assertEquals(Errors.NONE.code(), result10.response().errorCode());
        replay(manager, result10.records());
        // verify range's end offset advanced and wal object is added
        streamMetadata0 = manager.streamsMetadata().get(STREAM0);
        assertEquals(2, streamMetadata0.ranges().size());
        assertEquals(0L, streamMetadata0.ranges().get(0).startOffset());
        assertEquals(100L, streamMetadata0.ranges().get(0).endOffset());
        RangeMetadata rangeMetadata1 = streamMetadata0.ranges().get(1);
        assertEquals(100L, rangeMetadata1.startOffset());
        assertEquals(300L, rangeMetadata1.endOffset());
        assertEquals(1, manager.brokersMetadata().get(BROKER1).walObjects().size());

        // 6. get stream's offset
        GetStreamsOffsetRequestData request = new GetStreamsOffsetRequestData()
            .setStreamIds(List.of(STREAM0));
        GetStreamsOffsetResponseData streamsOffset = manager.getStreamsOffset(request);
        assertEquals(1, streamsOffset.streamsOffset().size());
        assertEquals(STREAM0, streamsOffset.streamsOffset().get(0).streamId());
        assertEquals(0L, streamsOffset.streamsOffset().get(0).startOffset());
        assertEquals(300L, streamsOffset.streamsOffset().get(0).endOffset());

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
                case BROKER_WALMETADATA_RECORD:
                    manager.replay((BrokerWALMetadataRecord) message);
                    break;
                case WALOBJECT_RECORD:
                    manager.replay((WALObjectRecord) message);
                    break;
                default:
                    throw new IllegalStateException("Unknown metadata record type " + type);
            }
        }
    }

    private void verifyInitializedStreamMetadata(S3StreamMetadata metadata) {
        assertNotNull(metadata);
        assertEquals(S3StreamConstant.INIT_EPOCH, metadata.currentEpoch());
        assertEquals(S3StreamConstant.INIT_RANGE_INDEX, metadata.currentRangeIndex());
        assertEquals(S3StreamConstant.INIT_START_OFFSET, metadata.startOffset());
    }

    private void verifyFirstTimeOpenStreamResult(ControllerResult<OpenStreamResponseData> result,
        long expectedEpoch, int expectedBrokerId) {
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
        assertEquals(expectedBrokerId, rangeRecord0.brokerId());
        assertEquals(expectedEpoch, rangeRecord0.epoch());
        assertEquals(0, rangeRecord0.rangeIndex());
        assertEquals(0L, rangeRecord0.startOffset());
        assertEquals(0L, rangeRecord0.endOffset());
    }

    private void verifyFirstRange(S3StreamMetadata streamMetadata, long expectedEpoch, int expectedBrokerId) {
        assertNotNull(streamMetadata);
        assertEquals(expectedEpoch, streamMetadata.currentEpoch());
        assertEquals(0, streamMetadata.currentRangeIndex());
        assertEquals(0L, streamMetadata.startOffset());
        assertEquals(1, streamMetadata.ranges().size());
        RangeMetadata rangeMetadata0 = streamMetadata.ranges().get(0);
        assertEquals(expectedBrokerId, rangeMetadata0.brokerId());
        assertEquals(expectedEpoch, rangeMetadata0.epoch());
        assertEquals(0, rangeMetadata0.rangeIndex());
        assertEquals(0L, rangeMetadata0.startOffset());
        assertEquals(0L, rangeMetadata0.endOffset());
    }
}
