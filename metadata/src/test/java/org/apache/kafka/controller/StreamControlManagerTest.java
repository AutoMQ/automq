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

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.CreateStreamRequestData;
import org.apache.kafka.common.message.CreateStreamResponseData;
import org.apache.kafka.common.message.OpenStreamRequestData;
import org.apache.kafka.common.message.OpenStreamResponseData;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.StreamControlManager;
import org.apache.kafka.controller.stream.StreamControlManager.S3StreamMetadata;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

    @BeforeEach
    public void setUp() {
        LogContext context = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(context);
        manager = new StreamControlManager(registry, context, null);
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
        assertEquals(0, streamRecord0.epoch());
        assertEquals(-1, streamRecord0.rangeIndex());
        assertEquals(0L, streamRecord0.startOffset());

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
        assertEquals(0, streamRecord1.epoch());
        assertEquals(-1, streamRecord1.rangeIndex());
        assertEquals(0L, streamRecord1.startOffset());

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
    public void testBasicOpenStream() {
        // 1. create stream_0 and stream_1
        CreateStreamRequestData request0 = new CreateStreamRequestData();
        ControllerResult<CreateStreamResponseData> result0 = manager.createStream(request0);
        manager.replay((AssignedStreamIdRecord) result0.records().get(0).message());
        result0.records().stream().skip(1).map(x -> (S3StreamRecord) x.message()).forEach(manager::replay);
        CreateStreamRequestData request1 = new CreateStreamRequestData();
        ControllerResult<CreateStreamResponseData> result1 = manager.createStream(request1);
        manager.replay((AssignedStreamIdRecord) result1.records().get(0).message());
        result1.records().stream().skip(1).map(x -> (S3StreamRecord) x.message()).forEach(manager::replay);

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

        // 4. broker_1 try to open stream_0 with epoch1
        ControllerResult<OpenStreamResponseData> result5 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM0).setStreamEpoch(EPOCH1).setBrokerId(BROKER1));
        assertEquals(Errors.NONE.code(), result5.response().errorCode());
        assertEquals(0L, result5.response().startOffset());
        assertEquals(0L, result5.response().nextOffset());
        assertEquals(2, result5.records().size());
        streamRecord = (S3StreamRecord) result5.records().get(0).message();
        manager.replay(streamRecord);
        assertEquals(EPOCH1, streamRecord.epoch());
        assertEquals(1, streamRecord.rangeIndex());
        assertEquals(0L, streamRecord.startOffset());
        rangeRecord = (RangeRecord) result5.records().get(1).message();
        manager.replay(rangeRecord);
        assertEquals(BROKER1, rangeRecord.brokerId());
        assertEquals(EPOCH1, rangeRecord.epoch());
        assertEquals(1, rangeRecord.rangeIndex());
        assertEquals(0L, rangeRecord.startOffset());
        assertEquals(0L, rangeRecord.endOffset());

        // verify that stream_0's epoch update to epoch1, and range index update to 1
        streamMetadata0 = manager.streamsMetadata().get(STREAM0);
        assertEquals(EPOCH1, streamMetadata0.currentEpoch());
        assertEquals(1, streamMetadata0.currentRangeIndex());
        assertEquals(0L, streamMetadata0.startOffset());
        assertEquals(2, streamMetadata0.ranges().size());
        RangeMetadata rangeMetadata0 = streamMetadata0.ranges().get(1);
        assertEquals(BROKER1, rangeMetadata0.brokerId());
        assertEquals(EPOCH1, rangeMetadata0.epoch());
        assertEquals(1, rangeMetadata0.rangeIndex());
        assertEquals(0L, rangeMetadata0.startOffset());
        assertEquals(0L, rangeMetadata0.endOffset());

        // 5. broker_0 try to open stream_1 with epoch0
        ControllerResult<OpenStreamResponseData> result6 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER0));
        assertEquals(Errors.NONE.code(), result6.response().errorCode());
        assertEquals(0L, result6.response().startOffset());
        assertEquals(0L, result6.response().nextOffset());
        assertEquals(0, result6.records().size());

        // 6. broker_1 try to open stream_1 with epoch0
        ControllerResult<OpenStreamResponseData> result7 = manager.openStream(
            new OpenStreamRequestData().setStreamId(STREAM1).setStreamEpoch(EPOCH0).setBrokerId(BROKER1));
        assertEquals(Errors.STREAM_FENCED.code(), result7.response().errorCode());
        assertEquals(0, result7.records().size());
    }

    private void verifyInitializedStreamMetadata(S3StreamMetadata metadata) {
        assertNotNull(metadata);
        assertEquals(0, metadata.currentEpoch());
        assertEquals(-1, metadata.currentRangeIndex());
        assertEquals(0L, metadata.startOffset());
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
