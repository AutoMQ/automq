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

import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 40)
@Tag("S3Unit")
public class StreamRuntimeMetadataImageTest {

    private static final long STREAM0 = 0L;

    private static final int BROKER0 = 0;

    private static final int BROKER1 = 1;

    @Test
    public void testRanges() {
        S3StreamMetadataImage image0 = S3StreamMetadataImage.EMPTY;
        List<ApiMessageAndVersion> delta0Records = new ArrayList<>();
        S3StreamMetadataDelta delta0 = new S3StreamMetadataDelta(image0);
        // 1. create stream0
        delta0Records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(STREAM0)
            .setRangeIndex(S3StreamConstant.INIT_RANGE_INDEX)
            .setStreamState(StreamState.OPENED.toByte())
            .setStartOffset(S3StreamConstant.INIT_START_OFFSET)
            .setEpoch(0L)
            .setStartOffset(0L), (short) 0));
        RecordTestUtils.replayAll(delta0, delta0Records);
        // verify delta and check image's write
        S3StreamMetadataImage image1 = new S3StreamMetadataImage(
            STREAM0, 0L, StreamState.OPENED, new S3StreamRecord.TagCollection(), S3StreamConstant.INIT_START_OFFSET, Collections.emptyList(), DeltaList.empty());
        assertEquals(image1, delta0.apply());
        testToImageAndBack(image1);

        // 2. update stream0's epoch to 1
        // and create range0_0, format: range{streamId}_{rangeIndex}
        List<ApiMessageAndVersion> delta1Records = new ArrayList<>();
        S3StreamMetadataDelta delta1 = new S3StreamMetadataDelta(image1);
        delta1Records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(STREAM0)
            .setRangeIndex(S3StreamConstant.INIT_RANGE_INDEX)
            .setStreamState(StreamState.OPENED.toByte())
            .setStartOffset(S3StreamConstant.INIT_START_OFFSET)
            .setEpoch(1L), (short) 0));
        delta1Records.add(new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(STREAM0)
            .setRangeIndex(0)
            .setEpoch(1L)
            .setNodeId(BROKER0)
            .setStartOffset(0L), (short) 0));
        RecordTestUtils.replayAll(delta1, delta1Records);
        // verify delta and check image's write
        S3StreamMetadataImage image2 = new S3StreamMetadataImage(
            STREAM0, 1L, StreamState.OPENED, new S3StreamRecord.TagCollection(), S3StreamConstant.INIT_START_OFFSET,
            List.of(new RangeMetadata(STREAM0, 1L, 0, 0L, 0L, BROKER0)), DeltaList.empty());
        assertEquals(image2, delta1.apply());
        testToImageAndBack(image2);

        // 3. advance range 0_0, node1 is the new leader, and create range0_1
        List<ApiMessageAndVersion> delta2Records = new ArrayList<>();
        S3StreamMetadataDelta delta2 = new S3StreamMetadataDelta(image2);
        delta2Records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(STREAM0)
            .setRangeIndex(0)
            .setStreamState(StreamState.OPENED.toByte())
            .setStartOffset(S3StreamConstant.INIT_START_OFFSET)
            .setEpoch(2L), (short) 0));
        delta2Records.add(new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(STREAM0)
            .setRangeIndex(1)
            .setEpoch(2L)
            .setNodeId(BROKER1)
            .setStartOffset(100L)
            .setEndOffset(100L), (short) 0));
        RecordTestUtils.replayAll(delta2, delta2Records);
        // verify delta and check image's write
        S3StreamMetadataImage image3 = new S3StreamMetadataImage(
            STREAM0, 2L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 0L, List.of(
            new RangeMetadata(STREAM0, 1L, 0, 0, 0, BROKER0),
            new RangeMetadata(STREAM0, 2L, 1, 100, 100, BROKER1)), DeltaList.empty());
        assertEquals(image3, delta2.apply());
        testToImageAndBack(image3);

        // 4. trim stream to start in 100 and remove range 0_0
        List<ApiMessageAndVersion> delta3Records = new ArrayList<>();
        S3StreamMetadataDelta delta3 = new S3StreamMetadataDelta(image3);
        delta3Records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(STREAM0)
            .setEpoch(2L)
            .setRangeIndex(0)
            .setStreamState(StreamState.OPENED.toByte())
            .setStartOffset(100L), (short) 0));
        delta3Records.add(new ApiMessageAndVersion(new RemoveRangeRecord()
            .setStreamId(STREAM0)
            .setRangeIndex(0), (short) 0));
        RecordTestUtils.replayAll(delta3, delta3Records);
        // verify delta and check image's write
        S3StreamMetadataImage image4 = new S3StreamMetadataImage(
            STREAM0, 2L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 100L, List.of(
            new RangeMetadata(STREAM0, 2L, 1, 100L, 100L, BROKER1)), DeltaList.empty());
        assertEquals(image4, delta3.apply());
    }

    @Test
    public void testStreamObjects() {
        S3StreamMetadataImage image0 = new S3StreamMetadataImage(
            STREAM0, 0L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 0L, Collections.emptyList(), DeltaList.empty());
        List<ApiMessageAndVersion> delta0Records = new ArrayList<>();
        S3StreamMetadataDelta delta0 = new S3StreamMetadataDelta(image0);
        // 1. create streamObject0 and streamObject1
        delta0Records.add(new ApiMessageAndVersion(new S3StreamObjectRecord()
            .setObjectId(0L)
            .setStreamId(STREAM0)
            .setStartOffset(0L)
            .setEndOffset(100L), AutoMQVersion.LATEST.streamObjectRecordVersion()));
        delta0Records.add(new ApiMessageAndVersion(new S3StreamObjectRecord()
            .setObjectId(1L)
            .setStreamId(STREAM0)
            .setStartOffset(100L)
            .setEndOffset(200L), AutoMQVersion.LATEST.streamObjectRecordVersion()));
        RecordTestUtils.replayAll(delta0, delta0Records);
        // verify delta and check image's write
        S3StreamMetadataImage image1 = new S3StreamMetadataImage(
            STREAM0, 0L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 0L, Collections.emptyList(), DeltaList.of(
            new S3StreamObject(0L, 999, STREAM0, 0L),
            new S3StreamObject(1L, 999, STREAM0, 100L)));
        assertEquals(image1, delta0.apply());
        testToImageAndBack(image1);

        // 2. remove streamObject0
        List<ApiMessageAndVersion> delta1Records = new ArrayList<>();
        S3StreamMetadataDelta delta1 = new S3StreamMetadataDelta(image1);
        delta1Records.add(new ApiMessageAndVersion(new RemoveS3StreamObjectRecord()
            .setObjectId(0L), (short) 0));
        RecordTestUtils.replayAll(delta1, delta1Records);
        // verify delta and check image's write
        S3StreamMetadataImage image2 = new S3StreamMetadataImage(
            STREAM0, 0L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 0L, Collections.emptyList(), DeltaList.of(
            new S3StreamObject(1L, 999, STREAM0, 100L)));
        assertEquals(image2, delta1.apply());
        testToImageAndBack(image2);
    }

    @Test
    public void testGetRangeContainsOffset() {
        List<RangeMetadata> ranges = List.of(
            new RangeMetadata(STREAM0, 0L, 1, 0, 10, 100),
            new RangeMetadata(STREAM0, 1L, 2, 10, 10, 101),
            new RangeMetadata(STREAM0, 2L, 3, 10, 20, 102),
            new RangeMetadata(STREAM0, 3L, 4, 20, 0, 103)
        );
        S3StreamMetadataImage image = new S3StreamMetadataImage(
            STREAM0, 3L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 0L, ranges, DeltaList.empty());
        Assertions.assertEquals(0, image.getRangeContainsOffset(0));
        Assertions.assertEquals(0, image.getRangeContainsOffset(5));
        Assertions.assertEquals(2, image.getRangeContainsOffset(10));
        Assertions.assertEquals(3, image.getRangeContainsOffset(100));
    }

    private void testToImageAndBack(S3StreamMetadataImage image) {
        RecordListWriter writer = new RecordListWriter();
        MetadataVersion metadataVersion = MetadataVersion.LATEST_PRODUCTION;
        metadataVersion.setAutoMQVersion(AutoMQVersion.LATEST);
        ImageWriterOptions options = new ImageWriterOptions.Builder().setMetadataVersion(metadataVersion).build();
        image.write(writer, options);
        S3StreamMetadataDelta delta = new S3StreamMetadataDelta(S3StreamMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        S3StreamMetadataImage newImage = delta.apply();
        assertEquals(image, newImage);
    }

}
