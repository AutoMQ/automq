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

package org.apache.kafka.image;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveStreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveStreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.StreamObjectRecord;
import org.apache.kafka.common.metadata.StreamRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.ObjectStreamIndex;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.StreamObject;
import org.apache.kafka.metadata.stream.WALObject;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 40)
public class StreamsMetadataImageTest {

    private static final long KB = 1024;

    private static final long MB = 1024 * KB;

    private static final long GB = 1024 * MB;
    private static final long WAL_LOOSE_SIZE = 40 * MB;

    private static final long WAL_MINOR_COMPACT_SIZE = 5 * GB;

    private static final long WAL_MAJOR_COMPACT_SIZE = 320 * GB;

    private static final long STREAM_OBJECT_SIZE = 320 * GB;

    static final StreamsMetadataImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final StreamsMetadataImage IMAGE2;

    // TODO: complete the test for StreamsMetadataImage

    static {
        IMAGE1 = null;
        DELTA1_RECORDS = null;
        IMAGE2 = null;
    }

    @Test
    public void testBasicChange() {
        List<StreamMetadataImage> streamMetadataImages = new ArrayList<>();
        Integer brokerId0 = 0;
        Integer brokerId1 = 1;
        Integer brokerId2 = 2;

        // 1. empty image
        StreamsMetadataImage image0 = StreamsMetadataImage.EMPTY;

        // 2. create stream and create range
        Long streamId0 = 0L;
        Long streamId1 = 1L;
        List<ApiMessageAndVersion> records = new ArrayList<>();
        StreamRecord streamRecord00 = new StreamRecord()
            .setStreamId(streamId0)
            .setEpoch(1)
            .setStartOffset(0L);
        records.add(new ApiMessageAndVersion(streamRecord00, (short) 0));
        RangeRecord rangeRecord00 = new RangeRecord()
            .setStreamId(streamId0)
            .setRangeIndex(0)
            .setStartOffset(0L)
            .setBrokerId(brokerId1)
            .setEpoch(1);
        records.add(new ApiMessageAndVersion(rangeRecord00, (short) 0));
        StreamRecord streamRecord01 = new StreamRecord()
            .setStreamId(streamId1)
            .setEpoch(1)
            .setStartOffset(0L);
        records.add(new ApiMessageAndVersion(streamRecord01, (short) 0));
        RangeRecord rangeRecord01 = new RangeRecord()
            .setStreamId(streamId1)
            .setRangeIndex(0)
            .setStartOffset(0L)
            .setBrokerId(brokerId1)
            .setEpoch(1);
        records.add(new ApiMessageAndVersion(rangeRecord01, (short) 0));
        StreamsMetadataDelta delta0 = new StreamsMetadataDelta(image0);
        RecordTestUtils.replayAll(delta0, records);
        StreamsMetadataImage image1 = delta0.apply();

        // check the image1
        assertEquals(2, image1.getStreamsMetadata().size());
        StreamMetadataImage streamMetadataImage1 = image1.getStreamsMetadata().get(streamId0);
        assertNotNull(streamMetadataImage1);
        assertEquals(1, streamMetadataImage1.getRanges().size());
        assertEquals(1, streamMetadataImage1.getEpoch());
        assertEquals(0, streamMetadataImage1.getStartOffset());
        RangeMetadata rangeMetadata1 = streamMetadataImage1.getRanges().get(0);
        assertNotNull(rangeMetadata1);
        assertEquals(RangeMetadata.of(rangeRecord00), rangeMetadata1);

        StreamMetadataImage streamMetadataImage11 = image1.getStreamsMetadata().get(streamId1);
        assertNotNull(streamMetadataImage11);
        assertEquals(1, streamMetadataImage11.getRanges().size());
        assertEquals(1, streamMetadataImage11.getEpoch());
        assertEquals(0, streamMetadataImage11.getStartOffset());
        RangeMetadata rangeMetadata11 = streamMetadataImage11.getRanges().get(0);
        assertNotNull(rangeMetadata11);
        assertEquals(RangeMetadata.of(rangeRecord01), rangeMetadata11);

        // 3. apply WALObject0, WALObject1, WALObject2
        WALObjectRecord walObjectRecord0 = new WALObjectRecord()
            .setBrokerId(brokerId0)
            .setObjectId(0L)
            .setApplyTimeInMs(System.currentTimeMillis())
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setObjectState((byte) S3ObjectState.APPLIED.ordinal());
        WALObjectRecord walObjectRecord1 = new WALObjectRecord()
            .setBrokerId(brokerId1)
            .setObjectId(1L)
            .setApplyTimeInMs(System.currentTimeMillis())
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setObjectState((byte) S3ObjectState.APPLIED.ordinal());
        WALObjectRecord walObjectRecord2 = new WALObjectRecord()
            .setBrokerId(brokerId1)
            .setObjectId(2L)
            .setApplyTimeInMs(System.currentTimeMillis())
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setObjectState((byte) S3ObjectState.APPLIED.ordinal());
        records.clear();
        records.add(new ApiMessageAndVersion(walObjectRecord0, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord1, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord2, (short) 0));
        StreamsMetadataDelta delta1 = new StreamsMetadataDelta(image1);
        RecordTestUtils.replayAll(delta1, records);
        StreamsMetadataImage image2 = delta1.apply();

        // check the image2
        assertEquals(2, image2.getBrokerStreamsMetadata().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage20 = image2.getBrokerStreamsMetadata().get(brokerId0);
        assertNotNull(brokerStreamMetadataImage20);
        assertEquals(1, brokerStreamMetadataImage20.getWalObjects().size());
        WALObject walObject0 = brokerStreamMetadataImage20.getWalObjects().get(0);
        assertEquals(brokerId0, walObject0.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject0.getObjectType());
        assertEquals(S3ObjectState.APPLIED, walObject0.getS3ObjectState());
        assertEquals(0L, walObject0.getObjectId());
        BrokerStreamMetadataImage brokerStreamMetadataImage21 = image2.getBrokerStreamsMetadata().get(brokerId1);
        assertNotNull(brokerStreamMetadataImage21);
        assertEquals(2, brokerStreamMetadataImage21.getWalObjects().size());
        WALObject walObject1 = brokerStreamMetadataImage21.getWalObjects().get(0);
        assertEquals(brokerId1, walObject1.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject1.getObjectType());
        assertEquals(S3ObjectState.APPLIED, walObject1.getS3ObjectState());
        assertEquals(1L, walObject1.getObjectId());
        WALObject walObject2 = brokerStreamMetadataImage21.getWalObjects().get(1);
        assertEquals(brokerId1, walObject2.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject2.getObjectType());
        assertEquals(S3ObjectState.APPLIED, walObject2.getS3ObjectState());
        assertEquals(2L, walObject2.getObjectId());

        // 4. create WALObject1, WALObject2, mark delete WALObject0
        List<ObjectStreamIndex> streamIndicesInWALObject1 = Arrays.asList(
            new ObjectStreamIndex(streamId0, 0L, 100L),
            new ObjectStreamIndex(streamId1, 0L, 200L)
        );
        WALObjectRecord walObjectRecord11 = new WALObjectRecord()
            .setBrokerId(brokerId1)
            .setObjectId(1L)
            .setObjectSize(WAL_LOOSE_SIZE)
            .setCreateTimeInMs(System.currentTimeMillis())
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setStreamsIndex(streamIndicesInWALObject1.stream().map(ObjectStreamIndex::toRecordStreamIndex).collect(
                Collectors.toList()))
            .setObjectState((byte) S3ObjectState.CREATED.ordinal());

        List<ObjectStreamIndex> streamIndicesInWALObject2 = Arrays.asList(
            new ObjectStreamIndex(streamId0, 101L, 200L),
            new ObjectStreamIndex(streamId1, 201L, 300L)
        );
        WALObjectRecord walObjectRecord21 = new WALObjectRecord()
            .setBrokerId(brokerId1)
            .setObjectId(2L)
            .setObjectSize(WAL_LOOSE_SIZE)
            .setCreateTimeInMs(System.currentTimeMillis())
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setStreamsIndex(streamIndicesInWALObject2.stream().map(ObjectStreamIndex::toRecordStreamIndex).collect(
                Collectors.toList()))
            .setObjectState((byte) S3ObjectState.CREATED.ordinal());
        WALObjectRecord walObjectRecord01 = new WALObjectRecord()
            .setBrokerId(brokerId0)
            .setObjectId(0L)
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setObjectState((byte) S3ObjectState.MARK_DESTROYED.ordinal());
        records.clear();
        records.add(new ApiMessageAndVersion(walObjectRecord11, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord21, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord01, (short) 0));
        StreamsMetadataDelta delta2 = new StreamsMetadataDelta(image2);
        RecordTestUtils.replayAll(delta2, records);
        StreamsMetadataImage image3 = delta2.apply();

        // check the image3
        assertEquals(2, image3.getBrokerStreamsMetadata().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage30 = image3.getBrokerStreamsMetadata().get(brokerId0);
        assertNotNull(brokerStreamMetadataImage30);
        assertEquals(1, brokerStreamMetadataImage30.getWalObjects().size());
        WALObject walObject01 = brokerStreamMetadataImage30.getWalObjects().get(0);
        assertEquals(brokerId0, walObject01.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject01.getObjectType());
        assertEquals(S3ObjectState.MARK_DESTROYED, walObject01.getS3ObjectState());
        BrokerStreamMetadataImage brokerStreamMetadataImage31 = image3.getBrokerStreamsMetadata().get(brokerId1);
        assertNotNull(brokerStreamMetadataImage31);
        assertEquals(2, brokerStreamMetadataImage31.getWalObjects().size());
        WALObject walObject11 = brokerStreamMetadataImage31.getWalObjects().get(0);
        assertEquals(brokerId1, walObject11.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject11.getObjectType());
        assertEquals(S3ObjectState.CREATED, walObject11.getS3ObjectState());
        Map<Long, ObjectStreamIndex> streamIndexVerify1 = walObject11.getStreamsIndex();
        assertEquals(2, streamIndexVerify1.size());
        assertEquals(0L, streamIndexVerify1.get(streamId0).getStartOffset());
        assertEquals(100L, streamIndexVerify1.get(streamId0).getEndOffset());
        assertEquals(0L, streamIndexVerify1.get(streamId1).getStartOffset());
        assertEquals(200L, streamIndexVerify1.get(streamId1).getEndOffset());
        WALObject walObject21 = brokerStreamMetadataImage31.getWalObjects().get(1);
        assertEquals(brokerId1, walObject21.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject21.getObjectType());
        assertEquals(S3ObjectState.CREATED, walObject21.getS3ObjectState());
        Map<Long, ObjectStreamIndex> streamIndexVerify2 = walObject21.getStreamsIndex();
        assertEquals(2, streamIndexVerify2.size());
        assertEquals(101L, streamIndexVerify2.get(streamId0).getStartOffset());
        assertEquals(200L, streamIndexVerify2.get(streamId0).getEndOffset());
        assertEquals(201L, streamIndexVerify2.get(streamId1).getStartOffset());
        assertEquals(300L, streamIndexVerify2.get(streamId1).getEndOffset());

        // 5. destroy WALObject0, mark delete WALObject1 and WALObject2, compact these to WALObject3
        RemoveWALObjectRecord removeWALObjectRecord0 = new RemoveWALObjectRecord()
            .setObjectId(0L)
            .setBrokerId(brokerId0);
        WALObjectRecord walObjectRecord12 = new WALObjectRecord()
            .setObjectId(1L)
            .setBrokerId(brokerId1)
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setObjectState((byte) S3ObjectState.MARK_DESTROYED.ordinal());
        WALObjectRecord walObjectRecord22 = new WALObjectRecord()
            .setObjectId(2L)
            .setBrokerId(brokerId1)
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setObjectState((byte) S3ObjectState.MARK_DESTROYED.ordinal());
        List<ObjectStreamIndex> streamIndicesInWALObject3 = Arrays.asList(
            new ObjectStreamIndex(streamId0, 0L, 200L),
            new ObjectStreamIndex(streamId1, 0L, 300L)
        );
        WALObjectRecord walObjectRecord3 = new WALObjectRecord()
            .setObjectId(3L)
            .setBrokerId(brokerId1)
            .setObjectType((byte) S3ObjectType.WAL_MINOR.ordinal())
            .setCreateTimeInMs(System.currentTimeMillis())
            .setObjectState((byte) S3ObjectState.CREATED.ordinal())
            .setApplyTimeInMs(System.currentTimeMillis())
            .setObjectSize(WAL_MINOR_COMPACT_SIZE)
            .setStreamsIndex(streamIndicesInWALObject3.stream().map(ObjectStreamIndex::toRecordStreamIndex).collect(
                Collectors.toList()));
        records.clear();
        records.add(new ApiMessageAndVersion(removeWALObjectRecord0, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord12, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord22, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord3, (short) 0));
        StreamsMetadataDelta delta3 = new StreamsMetadataDelta(image3);
        RecordTestUtils.replayAll(delta3, records);
        StreamsMetadataImage image4 = delta3.apply();

        // check the image4
        assertEquals(2, image4.getBrokerStreamsMetadata().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage40 = image4.getBrokerStreamsMetadata().get(brokerId0);
        assertNotNull(brokerStreamMetadataImage40);
        assertEquals(0, brokerStreamMetadataImage40.getWalObjects().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage41 = image4.getBrokerStreamsMetadata().get(brokerId1);
        assertNotNull(brokerStreamMetadataImage41);
        assertEquals(3, brokerStreamMetadataImage41.getWalObjects().size());
        WALObject walObject12 = brokerStreamMetadataImage41.getWalObjects().get(0);
        assertEquals(brokerId1, walObject12.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject12.getObjectType());
        assertEquals(S3ObjectState.MARK_DESTROYED, walObject12.getS3ObjectState());
        WALObject walObject22 = brokerStreamMetadataImage41.getWalObjects().get(1);
        assertEquals(brokerId1, walObject22.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, walObject22.getObjectType());
        assertEquals(S3ObjectState.MARK_DESTROYED, walObject22.getS3ObjectState());
        WALObject walObject3 = brokerStreamMetadataImage41.getWalObjects().get(2);
        assertEquals(brokerId1, walObject3.getBrokerId());
        assertEquals(S3ObjectType.WAL_MINOR, walObject3.getObjectType());
        assertEquals(S3ObjectState.CREATED, walObject3.getS3ObjectState());
        assertEquals(3L, walObject3.getObjectId());
        Map<Long, ObjectStreamIndex> streamIndexVerify3 = walObject3.getStreamsIndex();
        assertEquals(2, streamIndexVerify3.size());
        assertEquals(0L, streamIndexVerify3.get(streamId0).getStartOffset());
        assertEquals(200L, streamIndexVerify3.get(streamId0).getEndOffset());
        assertEquals(0L, streamIndexVerify3.get(streamId1).getStartOffset());
        assertEquals(300L, streamIndexVerify3.get(streamId1).getEndOffset());

        // 6. split WALObject3 by streamId to StreamObject4 and StreamObject5
        ObjectStreamIndex objectStreamIndex4 = new ObjectStreamIndex(streamId0, 0L, 200L);
        ObjectStreamIndex objectStreamIndex5 = new ObjectStreamIndex(streamId1, 0L, 300L);
        StreamObjectRecord streamObjectRecord4 = new StreamObjectRecord()
            .setObjectId(4L)
            .setStreamId(streamId0)
            .setObjectSize(STREAM_OBJECT_SIZE)
            .setObjectType((byte) S3ObjectType.STREAM.ordinal())
            .setCreateTimeInMs(System.currentTimeMillis())
            .setStartOffset(objectStreamIndex4.getStartOffset())
            .setEndOffset(objectStreamIndex4.getEndOffset());
        StreamObjectRecord streamObjectRecord5 = new StreamObjectRecord()
            .setObjectId(5L)
            .setStreamId(streamId1)
            .setObjectSize(STREAM_OBJECT_SIZE)
            .setObjectType((byte) S3ObjectType.STREAM.ordinal())
            .setCreateTimeInMs(System.currentTimeMillis())
            .setStartOffset(objectStreamIndex5.getStartOffset())
            .setEndOffset(objectStreamIndex5.getEndOffset());
        RemoveWALObjectRecord removeWALObjectRecord3 = new RemoveWALObjectRecord()
            .setObjectId(3L)
            .setBrokerId(brokerId1);
        records.clear();
        records.add(new ApiMessageAndVersion(streamObjectRecord4, (short) 0));
        records.add(new ApiMessageAndVersion(streamObjectRecord5, (short) 0));
        records.add(new ApiMessageAndVersion(removeWALObjectRecord3, (short) 0));
        StreamsMetadataDelta delta4 = new StreamsMetadataDelta(image4);
        RecordTestUtils.replayAll(delta4, records);
        StreamsMetadataImage image5 = delta4.apply();

        // check the image5
        assertEquals(2, image5.getBrokerStreamsMetadata().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage50 = image5.getBrokerStreamsMetadata().get(brokerId0);
        assertNotNull(brokerStreamMetadataImage50);
        assertEquals(0, brokerStreamMetadataImage50.getWalObjects().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage51 = image5.getBrokerStreamsMetadata().get(brokerId1);
        assertNotNull(brokerStreamMetadataImage51);
        assertEquals(0, brokerStreamMetadataImage51.getWalObjects().size());
        assertEquals(2, image5.getStreamsMetadata().size());

        StreamMetadataImage streamMetadataImage50 = image5.getStreamsMetadata().get(streamId0);
        assertNotNull(streamMetadataImage50);
        assertEquals(1, streamMetadataImage50.getRanges().size());
        assertEquals(1, streamMetadataImage50.getEpoch());
        assertEquals(0, streamMetadataImage50.getStartOffset());
        assertEquals(1, streamMetadataImage50.getStreams());
        StreamObject streamObject4 = streamMetadataImage50.getStreams().get(0);
        assertEquals(4L, streamObject4.getObjectId());
        assertEquals(STREAM_OBJECT_SIZE, streamObject4.getObjectSize());
        assertEquals(S3ObjectType.STREAM, streamObject4.getObjectType());
        assertEquals(S3ObjectState.CREATED, streamObject4.getS3ObjectState());
        assertEquals(objectStreamIndex4, streamObject4.getStreamIndex());

        StreamMetadataImage streamMetadataImage51 = image5.getStreamsMetadata().get(streamId1);
        assertNotNull(streamMetadataImage51);
        assertEquals(1, streamMetadataImage51.getRanges().size());
        assertEquals(1, streamMetadataImage51.getEpoch());
        assertEquals(0, streamMetadataImage51.getStartOffset());
        assertEquals(1, streamMetadataImage51.getStreams());
        StreamObject streamObject5 = streamMetadataImage51.getStreams().get(0);
        assertEquals(5L, streamObject5.getObjectId());
        assertEquals(STREAM_OBJECT_SIZE, streamObject5.getObjectSize());
        assertEquals(S3ObjectType.STREAM, streamObject5.getObjectType());
        assertEquals(S3ObjectState.CREATED, streamObject5.getS3ObjectState());
        assertEquals(objectStreamIndex5, streamObject5.getStreamIndex());

        // 7. remove streamObject4 and remove stream1
        RemoveStreamObjectRecord removeStreamObjectRecord4 = new RemoveStreamObjectRecord()
            .setObjectId(4L)
            .setStreamId(streamId0);
        RemoveStreamRecord removeStreamRecord = new RemoveStreamRecord()
            .setStreamId(streamId1);
        records.clear();
        records.add(new ApiMessageAndVersion(removeStreamObjectRecord4, (short) 0));
        records.add(new ApiMessageAndVersion(removeStreamRecord, (short) 0));
        StreamsMetadataDelta delta5 = new StreamsMetadataDelta(image5);
        RecordTestUtils.replayAll(delta5, records);
        StreamsMetadataImage image6 = delta5.apply();

        // check the image6
        assertEquals(2, image6.getBrokerStreamsMetadata().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage60 = image6.getBrokerStreamsMetadata().get(brokerId0);
        assertNotNull(brokerStreamMetadataImage60);
        assertEquals(0, brokerStreamMetadataImage60.getWalObjects().size());
        BrokerStreamMetadataImage brokerStreamMetadataImage61 = image6.getBrokerStreamsMetadata().get(brokerId1);
        assertNotNull(brokerStreamMetadataImage61);
        assertEquals(0, brokerStreamMetadataImage61.getWalObjects().size());

        assertEquals(1, image6.getStreamsMetadata().size());
        StreamMetadataImage streamMetadataImage60 = image6.getStreamsMetadata().get(streamId0);
        assertNotNull(streamMetadataImage60);
        assertEquals(1, streamMetadataImage60.getRanges().size());
        assertEquals(0, streamMetadataImage60.getStreams().size());
    }


    private void testToImageAndBack(StreamsMetadataImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        StreamsMetadataDelta delta = new StreamsMetadataDelta(StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        StreamsMetadataImage newImage = delta.apply();
        assertEquals(image, newImage);
    }
}
