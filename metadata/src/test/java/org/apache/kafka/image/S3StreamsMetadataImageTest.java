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
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3ObjectStreamIndex;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 40)
public class S3StreamsMetadataImageTest {

    private static final long KB = 1024;

    private static final long MB = 1024 * KB;

    private static final long GB = 1024 * MB;
    private static final long WAL_LOOSE_SIZE = 40 * MB;

    private static final long WAL_MINOR_COMPACT_SIZE = 5 * GB;

    private static final long WAL_MAJOR_COMPACT_SIZE = 320 * GB;

    private static final long STREAM_OBJECT_SIZE = 320 * GB;

    static final S3StreamsMetadataImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final S3StreamsMetadataImage IMAGE2;

    // TODO: complete the test for StreamsMetadataImage

    static {
        IMAGE1 = null;
        DELTA1_RECORDS = null;
        IMAGE2 = null;
    }

    @Test
    public void testBasicChange() {
        List<S3StreamMetadataImage> s3StreamMetadataImages = new ArrayList<>();
        Integer brokerId0 = 0;
        Integer brokerId1 = 1;
        Integer brokerId2 = 2;

        // 1. empty image
        S3StreamsMetadataImage image0 = S3StreamsMetadataImage.EMPTY;

        // 2. create stream and create range
        Long streamId0 = 0L;
        Long streamId1 = 1L;
        List<ApiMessageAndVersion> records = new ArrayList<>();
        S3StreamRecord streamRecord00 = new S3StreamRecord()
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
        S3StreamRecord streamRecord01 = new S3StreamRecord()
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
        S3StreamsMetadataDelta delta0 = new S3StreamsMetadataDelta(image0);
        RecordTestUtils.replayAll(delta0, records);
        S3StreamsMetadataImage image1 = delta0.apply();

        // check the image1
        assertEquals(2, image1.getStreamsMetadata().size());
        S3StreamMetadataImage s3StreamMetadataImage1 = image1.getStreamsMetadata().get(streamId0);
        assertNotNull(s3StreamMetadataImage1);
        assertEquals(1, s3StreamMetadataImage1.getRanges().size());
        assertEquals(1, s3StreamMetadataImage1.getEpoch());
        assertEquals(0, s3StreamMetadataImage1.getStartOffset());
        RangeMetadata rangeMetadata1 = s3StreamMetadataImage1.getRanges().get(0);
        assertNotNull(rangeMetadata1);
        assertEquals(RangeMetadata.of(rangeRecord00), rangeMetadata1);

        S3StreamMetadataImage s3StreamMetadataImage11 = image1.getStreamsMetadata().get(streamId1);
        assertNotNull(s3StreamMetadataImage11);
        assertEquals(1, s3StreamMetadataImage11.getRanges().size());
        assertEquals(1, s3StreamMetadataImage11.getEpoch());
        assertEquals(0, s3StreamMetadataImage11.getStartOffset());
        RangeMetadata rangeMetadata11 = s3StreamMetadataImage11.getRanges().get(0);
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
        S3StreamsMetadataDelta delta1 = new S3StreamsMetadataDelta(image1);
        RecordTestUtils.replayAll(delta1, records);
        S3StreamsMetadataImage image2 = delta1.apply();

        // check the image2
        assertEquals(2, image2.getBrokerWALMetadata().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage20 = image2.getBrokerWALMetadata().get(brokerId0);
        assertNotNull(brokerS3WALMetadataImage20);
        assertEquals(1, brokerS3WALMetadataImage20.getWalObjects().size());
        S3WALObject s3WalObject0 = brokerS3WALMetadataImage20.getWalObjects().get(0);
        assertEquals(brokerId0, s3WalObject0.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject0.getObjectType());
        assertEquals(S3ObjectState.APPLIED, s3WalObject0.getS3ObjectState());
        assertEquals(0L, s3WalObject0.getObjectId());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage21 = image2.getBrokerWALMetadata().get(brokerId1);
        assertNotNull(brokerS3WALMetadataImage21);
        assertEquals(2, brokerS3WALMetadataImage21.getWalObjects().size());
        S3WALObject s3WalObject1 = brokerS3WALMetadataImage21.getWalObjects().get(0);
        assertEquals(brokerId1, s3WalObject1.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject1.getObjectType());
        assertEquals(S3ObjectState.APPLIED, s3WalObject1.getS3ObjectState());
        assertEquals(1L, s3WalObject1.getObjectId());
        S3WALObject s3WalObject2 = brokerS3WALMetadataImage21.getWalObjects().get(1);
        assertEquals(brokerId1, s3WalObject2.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject2.getObjectType());
        assertEquals(S3ObjectState.APPLIED, s3WalObject2.getS3ObjectState());
        assertEquals(2L, s3WalObject2.getObjectId());

        // 4. create WALObject1, WALObject2, mark delete WALObject0
        List<S3ObjectStreamIndex> streamIndicesInWALObject1 = Arrays.asList(
            new S3ObjectStreamIndex(streamId0, 0L, 100L),
            new S3ObjectStreamIndex(streamId1, 0L, 200L)
        );
        WALObjectRecord walObjectRecord11 = new WALObjectRecord()
            .setBrokerId(brokerId1)
            .setObjectId(1L)
            .setObjectSize(WAL_LOOSE_SIZE)
            .setCreateTimeInMs(System.currentTimeMillis())
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setStreamsIndex(streamIndicesInWALObject1.stream().map(S3ObjectStreamIndex::toRecordStreamIndex).collect(
                Collectors.toList()))
            .setObjectState((byte) S3ObjectState.CREATED.ordinal());

        List<S3ObjectStreamIndex> streamIndicesInWALObject2 = Arrays.asList(
            new S3ObjectStreamIndex(streamId0, 101L, 200L),
            new S3ObjectStreamIndex(streamId1, 201L, 300L)
        );
        WALObjectRecord walObjectRecord21 = new WALObjectRecord()
            .setBrokerId(brokerId1)
            .setObjectId(2L)
            .setObjectSize(WAL_LOOSE_SIZE)
            .setCreateTimeInMs(System.currentTimeMillis())
            .setObjectType((byte) S3ObjectType.WAL_LOOSE.ordinal())
            .setStreamsIndex(streamIndicesInWALObject2.stream().map(S3ObjectStreamIndex::toRecordStreamIndex).collect(
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
        S3StreamsMetadataDelta delta2 = new S3StreamsMetadataDelta(image2);
        RecordTestUtils.replayAll(delta2, records);
        S3StreamsMetadataImage image3 = delta2.apply();

        // check the image3
        assertEquals(2, image3.getBrokerWALMetadata().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage30 = image3.getBrokerWALMetadata().get(brokerId0);
        assertNotNull(brokerS3WALMetadataImage30);
        assertEquals(1, brokerS3WALMetadataImage30.getWalObjects().size());
        S3WALObject s3WalObject01 = brokerS3WALMetadataImage30.getWalObjects().get(0);
        assertEquals(brokerId0, s3WalObject01.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject01.getObjectType());
        assertEquals(S3ObjectState.MARK_DESTROYED, s3WalObject01.getS3ObjectState());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage31 = image3.getBrokerWALMetadata().get(brokerId1);
        assertNotNull(brokerS3WALMetadataImage31);
        assertEquals(2, brokerS3WALMetadataImage31.getWalObjects().size());
        S3WALObject s3WalObject11 = brokerS3WALMetadataImage31.getWalObjects().get(0);
        assertEquals(brokerId1, s3WalObject11.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject11.getObjectType());
        assertEquals(S3ObjectState.CREATED, s3WalObject11.getS3ObjectState());
        Map<Long, S3ObjectStreamIndex> streamIndexVerify1 = s3WalObject11.getStreamsIndex();
        assertEquals(2, streamIndexVerify1.size());
        assertEquals(0L, streamIndexVerify1.get(streamId0).getStartOffset());
        assertEquals(100L, streamIndexVerify1.get(streamId0).getEndOffset());
        assertEquals(0L, streamIndexVerify1.get(streamId1).getStartOffset());
        assertEquals(200L, streamIndexVerify1.get(streamId1).getEndOffset());
        S3WALObject s3WalObject21 = brokerS3WALMetadataImage31.getWalObjects().get(1);
        assertEquals(brokerId1, s3WalObject21.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject21.getObjectType());
        assertEquals(S3ObjectState.CREATED, s3WalObject21.getS3ObjectState());
        Map<Long, S3ObjectStreamIndex> streamIndexVerify2 = s3WalObject21.getStreamsIndex();
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
        List<S3ObjectStreamIndex> streamIndicesInWALObject3 = Arrays.asList(
            new S3ObjectStreamIndex(streamId0, 0L, 200L),
            new S3ObjectStreamIndex(streamId1, 0L, 300L)
        );
        WALObjectRecord walObjectRecord3 = new WALObjectRecord()
            .setObjectId(3L)
            .setBrokerId(brokerId1)
            .setObjectType((byte) S3ObjectType.WAL_MINOR.ordinal())
            .setCreateTimeInMs(System.currentTimeMillis())
            .setObjectState((byte) S3ObjectState.CREATED.ordinal())
            .setApplyTimeInMs(System.currentTimeMillis())
            .setObjectSize(WAL_MINOR_COMPACT_SIZE)
            .setStreamsIndex(streamIndicesInWALObject3.stream().map(S3ObjectStreamIndex::toRecordStreamIndex).collect(
                Collectors.toList()));
        records.clear();
        records.add(new ApiMessageAndVersion(removeWALObjectRecord0, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord12, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord22, (short) 0));
        records.add(new ApiMessageAndVersion(walObjectRecord3, (short) 0));
        S3StreamsMetadataDelta delta3 = new S3StreamsMetadataDelta(image3);
        RecordTestUtils.replayAll(delta3, records);
        S3StreamsMetadataImage image4 = delta3.apply();

        // check the image4
        assertEquals(2, image4.getBrokerWALMetadata().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage40 = image4.getBrokerWALMetadata().get(brokerId0);
        assertNotNull(brokerS3WALMetadataImage40);
        assertEquals(0, brokerS3WALMetadataImage40.getWalObjects().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage41 = image4.getBrokerWALMetadata().get(brokerId1);
        assertNotNull(brokerS3WALMetadataImage41);
        assertEquals(3, brokerS3WALMetadataImage41.getWalObjects().size());
        S3WALObject s3WalObject12 = brokerS3WALMetadataImage41.getWalObjects().get(0);
        assertEquals(brokerId1, s3WalObject12.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject12.getObjectType());
        assertEquals(S3ObjectState.MARK_DESTROYED, s3WalObject12.getS3ObjectState());
        S3WALObject s3WalObject22 = brokerS3WALMetadataImage41.getWalObjects().get(1);
        assertEquals(brokerId1, s3WalObject22.getBrokerId());
        assertEquals(S3ObjectType.WAL_LOOSE, s3WalObject22.getObjectType());
        assertEquals(S3ObjectState.MARK_DESTROYED, s3WalObject22.getS3ObjectState());
        S3WALObject s3WalObject3 = brokerS3WALMetadataImage41.getWalObjects().get(2);
        assertEquals(brokerId1, s3WalObject3.getBrokerId());
        assertEquals(S3ObjectType.WAL_MINOR, s3WalObject3.getObjectType());
        assertEquals(S3ObjectState.CREATED, s3WalObject3.getS3ObjectState());
        assertEquals(3L, s3WalObject3.getObjectId());
        Map<Long, S3ObjectStreamIndex> streamIndexVerify3 = s3WalObject3.getStreamsIndex();
        assertEquals(2, streamIndexVerify3.size());
        assertEquals(0L, streamIndexVerify3.get(streamId0).getStartOffset());
        assertEquals(200L, streamIndexVerify3.get(streamId0).getEndOffset());
        assertEquals(0L, streamIndexVerify3.get(streamId1).getStartOffset());
        assertEquals(300L, streamIndexVerify3.get(streamId1).getEndOffset());

        // 6. split WALObject3 by streamId to StreamObject4 and StreamObject5
        S3ObjectStreamIndex s3ObjectStreamIndex4 = new S3ObjectStreamIndex(streamId0, 0L, 200L);
        S3ObjectStreamIndex s3ObjectStreamIndex5 = new S3ObjectStreamIndex(streamId1, 0L, 300L);
        S3StreamObjectRecord streamObjectRecord4 = new S3StreamObjectRecord()
            .setObjectId(4L)
            .setStreamId(streamId0)
            .setObjectSize(STREAM_OBJECT_SIZE)
            .setObjectType((byte) S3ObjectType.STREAM.ordinal())
            .setCreateTimeInMs(System.currentTimeMillis())
            .setStartOffset(s3ObjectStreamIndex4.getStartOffset())
            .setEndOffset(s3ObjectStreamIndex4.getEndOffset());
        S3StreamObjectRecord streamObjectRecord5 = new S3StreamObjectRecord()
            .setObjectId(5L)
            .setStreamId(streamId1)
            .setObjectSize(STREAM_OBJECT_SIZE)
            .setObjectType((byte) S3ObjectType.STREAM.ordinal())
            .setCreateTimeInMs(System.currentTimeMillis())
            .setStartOffset(s3ObjectStreamIndex5.getStartOffset())
            .setEndOffset(s3ObjectStreamIndex5.getEndOffset());
        RemoveWALObjectRecord removeWALObjectRecord3 = new RemoveWALObjectRecord()
            .setObjectId(3L)
            .setBrokerId(brokerId1);
        records.clear();
        records.add(new ApiMessageAndVersion(streamObjectRecord4, (short) 0));
        records.add(new ApiMessageAndVersion(streamObjectRecord5, (short) 0));
        records.add(new ApiMessageAndVersion(removeWALObjectRecord3, (short) 0));
        S3StreamsMetadataDelta delta4 = new S3StreamsMetadataDelta(image4);
        RecordTestUtils.replayAll(delta4, records);
        S3StreamsMetadataImage image5 = delta4.apply();

        // check the image5
        assertEquals(2, image5.getBrokerWALMetadata().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage50 = image5.getBrokerWALMetadata().get(brokerId0);
        assertNotNull(brokerS3WALMetadataImage50);
        assertEquals(0, brokerS3WALMetadataImage50.getWalObjects().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage51 = image5.getBrokerWALMetadata().get(brokerId1);
        assertNotNull(brokerS3WALMetadataImage51);
        assertEquals(0, brokerS3WALMetadataImage51.getWalObjects().size());
        assertEquals(2, image5.getStreamsMetadata().size());

        S3StreamMetadataImage s3StreamMetadataImage50 = image5.getStreamsMetadata().get(streamId0);
        assertNotNull(s3StreamMetadataImage50);
        assertEquals(1, s3StreamMetadataImage50.getRanges().size());
        assertEquals(1, s3StreamMetadataImage50.getEpoch());
        assertEquals(0, s3StreamMetadataImage50.getStartOffset());
        assertEquals(1, s3StreamMetadataImage50.getStreams());
        S3StreamObject s3StreamObject4 = s3StreamMetadataImage50.getStreams().get(0);
        assertEquals(4L, s3StreamObject4.getObjectId());
        assertEquals(STREAM_OBJECT_SIZE, s3StreamObject4.getObjectSize());
        assertEquals(S3ObjectType.STREAM, s3StreamObject4.getObjectType());
        assertEquals(S3ObjectState.CREATED, s3StreamObject4.getS3ObjectState());
        assertEquals(s3ObjectStreamIndex4, s3StreamObject4.getStreamIndex());

        S3StreamMetadataImage s3StreamMetadataImage51 = image5.getStreamsMetadata().get(streamId1);
        assertNotNull(s3StreamMetadataImage51);
        assertEquals(1, s3StreamMetadataImage51.getRanges().size());
        assertEquals(1, s3StreamMetadataImage51.getEpoch());
        assertEquals(0, s3StreamMetadataImage51.getStartOffset());
        assertEquals(1, s3StreamMetadataImage51.getStreams());
        S3StreamObject s3StreamObject5 = s3StreamMetadataImage51.getStreams().get(0);
        assertEquals(5L, s3StreamObject5.getObjectId());
        assertEquals(STREAM_OBJECT_SIZE, s3StreamObject5.getObjectSize());
        assertEquals(S3ObjectType.STREAM, s3StreamObject5.getObjectType());
        assertEquals(S3ObjectState.CREATED, s3StreamObject5.getS3ObjectState());
        assertEquals(s3ObjectStreamIndex5, s3StreamObject5.getStreamIndex());

        // 7. remove streamObject4 and remove stream1
        RemoveS3StreamObjectRecord removeStreamObjectRecord4 = new RemoveS3StreamObjectRecord()
            .setObjectId(4L)
            .setStreamId(streamId0);
        RemoveS3StreamRecord removeStreamRecord = new RemoveS3StreamRecord()
            .setStreamId(streamId1);
        records.clear();
        records.add(new ApiMessageAndVersion(removeStreamObjectRecord4, (short) 0));
        records.add(new ApiMessageAndVersion(removeStreamRecord, (short) 0));
        S3StreamsMetadataDelta delta5 = new S3StreamsMetadataDelta(image5);
        RecordTestUtils.replayAll(delta5, records);
        S3StreamsMetadataImage image6 = delta5.apply();

        // check the image6
        assertEquals(2, image6.getBrokerWALMetadata().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage60 = image6.getBrokerWALMetadata().get(brokerId0);
        assertNotNull(brokerS3WALMetadataImage60);
        assertEquals(0, brokerS3WALMetadataImage60.getWalObjects().size());
        BrokerS3WALMetadataImage brokerS3WALMetadataImage61 = image6.getBrokerWALMetadata().get(brokerId1);
        assertNotNull(brokerS3WALMetadataImage61);
        assertEquals(0, brokerS3WALMetadataImage61.getWalObjects().size());

        assertEquals(1, image6.getStreamsMetadata().size());
        S3StreamMetadataImage s3StreamMetadataImage60 = image6.getStreamsMetadata().get(streamId0);
        assertNotNull(s3StreamMetadataImage60);
        assertEquals(1, s3StreamMetadataImage60.getRanges().size());
        assertEquals(0, s3StreamMetadataImage60.getStreams().size());
    }


    private void testToImageAndBack(S3StreamsMetadataImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        S3StreamsMetadataImage newImage = delta.apply();
        assertEquals(image, newImage);
    }
}
