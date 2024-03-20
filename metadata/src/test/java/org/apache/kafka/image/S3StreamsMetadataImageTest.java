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

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 40)
@Tag("S3Unit")
public class S3StreamsMetadataImageTest {

    private static final int BROKER0 = 0;
    private static final int BROKER1 = 1;
    private static final long STREAM0 = 0;
    private static final long STREAM1 = 1;
    private static final long KB = 1024;

    private static final long MB = 1024 * KB;

    private static final long GB = 1024 * MB;

    static final S3StreamsMetadataImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final S3StreamsMetadataImage IMAGE2;

    // TODO: complete the test for StreamsMetadataImage

    static {
        IMAGE1 = S3StreamsMetadataImage.EMPTY;
        DELTA1_RECORDS = List.of();
        IMAGE2 = S3StreamsMetadataImage.EMPTY;
    }

    @Test
    public void testAssignedChange() {
        S3StreamsMetadataImage image0 = S3StreamsMetadataImage.EMPTY;
        ApiMessageAndVersion record0 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
                .setAssignedStreamId(0), (short) 0);
        S3StreamsMetadataDelta delta0 = new S3StreamsMetadataDelta(image0);
        RecordTestUtils.replayAll(delta0, List.of(record0));
        S3StreamsMetadataImage image1 = new S3StreamsMetadataImage(0, new DeltaMap<>(), new DeltaMap<>());
        assertEquals(image1, delta0.apply());
        testToImageAndBack(image1);

        ApiMessageAndVersion record1 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
                .setAssignedStreamId(10), (short) 0);
        S3StreamsMetadataDelta delta1 = new S3StreamsMetadataDelta(image1);
        RecordTestUtils.replayAll(delta1, List.of(record1));
        S3StreamsMetadataImage image2 = new S3StreamsMetadataImage(10, new DeltaMap<>(), new DeltaMap<>());
        assertEquals(image2, delta1.apply());
    }

    private void testToImageAndBack(S3StreamsMetadataImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        S3StreamsMetadataImage newImage = delta.apply();
        assertEquals(image, newImage);
    }

    @Test
    public void testGetObjects() {
        DeltaMap<Long, S3StreamSetObject> broker0Objects = DeltaMap.of(
                0L, new S3StreamSetObject(0, BROKER0, List.of(new StreamOffsetRange(STREAM0, 100L, 120L)), 0L),
                1L, new S3StreamSetObject(1, BROKER0, List.of(new StreamOffsetRange(STREAM0, 120L, 140L)), 1L),
                2L, new S3StreamSetObject(2, BROKER0, List.of(new StreamOffsetRange(STREAM0, 180L, 200L)), 2L),
                3L, new S3StreamSetObject(3, BROKER0, List.of(
                        new StreamOffsetRange(STREAM0, 400L, 420L)), 3L),
                4L, new S3StreamSetObject(4, BROKER0, List.of(new StreamOffsetRange(STREAM0, 520L, 600L)), 4L));
        DeltaMap<Long, S3StreamSetObject> broker1Objects = DeltaMap.of(
                5L, new S3StreamSetObject(5, BROKER1, List.of(new StreamOffsetRange(STREAM0, 140L, 160L)), 0L),
                6L, new S3StreamSetObject(6, BROKER1, List.of(new StreamOffsetRange(STREAM0, 160L, 180L)), 1L),
                7L, new S3StreamSetObject(7, BROKER1, List.of(new StreamOffsetRange(STREAM0, 420L, 520L)), 2L));
        NodeS3StreamSetObjectMetadataImage broker0WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH,
                broker0Objects);
        NodeS3StreamSetObjectMetadataImage broker1WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER1, S3StreamConstant.INVALID_BROKER_EPOCH,
                broker1Objects);
        List<RangeMetadata> ranges = List.of(
                new RangeMetadata(STREAM0, 0L, 0, 10L, 140L, BROKER0),
                new RangeMetadata(STREAM0, 1L, 1, 140L, 180L, BROKER1),
                new RangeMetadata(STREAM0, 2L, 2, 180L, 420L, BROKER0),
                new RangeMetadata(STREAM0, 3L, 3, 420L, 520L, BROKER1),
                new RangeMetadata(STREAM0, 4L, 4, 520L, 600L, BROKER0));
        List<S3StreamObject> streamObjects = List.of(
                new S3StreamObject(8, STREAM0, 10L, 100L, S3StreamConstant.INVALID_TS),
                new S3StreamObject(9, STREAM0, 200L, 300L, S3StreamConstant.INVALID_TS),
                new S3StreamObject(10, STREAM0, 300L, 400L, S3StreamConstant.INVALID_TS));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 4L, StreamState.OPENED, 10, ranges, streamObjects);
        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(STREAM0, DeltaMap.of(STREAM0, streamImage),
                DeltaMap.of(BROKER0, broker0WALMetadataImage, BROKER1, broker1WALMetadataImage));

        // 1. search stream_1
        InRangeObjects objects = streamsImage.getObjects(STREAM1, 10, 100, Integer.MAX_VALUE);
        assertEquals(InRangeObjects.INVALID, objects);

        // 2. search stream_0 in [0, 600)
        // failed for trimmed startOffset
        objects = streamsImage.getObjects(STREAM0, 0, 600, Integer.MAX_VALUE);
        assertEquals(InRangeObjects.INVALID, objects);

        // 3. search stream_0 for full range [10, 600)
        objects = streamsImage.getObjects(STREAM0, 10, 600, Integer.MAX_VALUE);
        assertEquals(10, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(11, objects.objects().size());
        List<Long> expectedObjectIds = List.of(
                8L, 0L, 1L, 5L, 6L, 2L, 9L, 10L, 3L, 7L, 4L);
        assertEquals(expectedObjectIds, objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 4. search stream_0 in [20, 550)
        objects = streamsImage.getObjects(STREAM0, 20, 550, Integer.MAX_VALUE);
        assertEquals(10, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(11, objects.objects().size());
        assertEquals(expectedObjectIds, objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 5. search stream_0 in [20, 550) with limit 5
        objects = streamsImage.getObjects(STREAM0, 20, 550, 5);
        assertEquals(10, objects.startOffset());
        assertEquals(180, objects.endOffset());
        assertEquals(5, objects.objects().size());
        assertEquals(expectedObjectIds.subList(0, 5), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 6. search stream_0 in [400, 520)
        objects = streamsImage.getObjects(STREAM0, 400, 520, Integer.MAX_VALUE);
        assertEquals(400, objects.startOffset());
        assertEquals(520, objects.endOffset());
        assertEquals(2, objects.objects().size());
        assertEquals(expectedObjectIds.subList(8, 10), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 7. search stream_0 in [401, 519)
        objects = streamsImage.getObjects(STREAM0, 401, 519, Integer.MAX_VALUE);
        assertEquals(400, objects.startOffset());
        assertEquals(520, objects.endOffset());
        assertEquals(2, objects.objects().size());
        assertEquals(expectedObjectIds.subList(8, 10), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 8. search stream_0 in [399, 521)
        objects = streamsImage.getObjects(STREAM0, 399, 521, Integer.MAX_VALUE);
        assertEquals(300, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(4, objects.objects().size());
        assertEquals(expectedObjectIds.subList(7, 11), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 9. search stream0 in [399, 1000)
        objects = streamsImage.getObjects(STREAM0, 399, 1000, Integer.MAX_VALUE);
        assertEquals(300, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(4, objects.objects().size());
        assertEquals(expectedObjectIds.subList(7, 11), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        objects = streamsImage.getObjects(STREAM0, 101, 400L, Integer.MAX_VALUE);
        assertEquals(100L, objects.startOffset());
        assertEquals(400L, objects.endOffset());
        assertEquals(7, objects.objects().size());
        assertEquals(expectedObjectIds.subList(1, 8), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        objects = streamsImage.getObjects(STREAM0, 10, ObjectUtils.NOOP_OFFSET, 9);
        assertEquals(10, objects.startOffset());
        assertEquals(420, objects.endOffset());
        assertEquals(9, objects.objects().size());
        assertEquals(List.of(8L, 0L, 1L, 5L, 6L, 2L, 9L, 10L, 3L), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
    }

    /**
     * Test get objects with the first hit object is a stream object.
     */
    @Test
    public void testGetObjectsWithFirstStreamObject() {
        DeltaMap<Long, S3StreamSetObject> broker0Objects = DeltaMap.of(
                0L, new S3StreamSetObject(0, BROKER0, List.of(new StreamOffsetRange(STREAM0, 20L, 40L)), 0L));
        NodeS3StreamSetObjectMetadataImage broker0WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH,
                broker0Objects);
        List<RangeMetadata> ranges = List.of(
                new RangeMetadata(STREAM0, 0L, 0, 10L, 40L, BROKER0),
                new RangeMetadata(STREAM0, 2L, 2, 40L, 60L, BROKER0));
        List<S3StreamObject> streamObjects = List.of(
                new S3StreamObject(8, STREAM0, 10L, 20L, S3StreamConstant.INVALID_TS),
                new S3StreamObject(8, STREAM0, 40L, 60L, S3StreamConstant.INVALID_TS));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 4L, StreamState.OPENED, 10, ranges, streamObjects);
        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(STREAM0, DeltaMap.of(STREAM0, streamImage),
                DeltaMap.of(BROKER0, broker0WALMetadataImage));

        InRangeObjects objects = streamsImage.getObjects(STREAM0, 22L, 55, 4);
        assertEquals(2, objects.objects().size());
        assertEquals(20L, objects.startOffset());
        assertEquals(60L, objects.endOffset());

        objects = streamsImage.getObjects(STREAM0, 22L, 55, 1);
        assertEquals(1, objects.objects().size());
        assertEquals(20L, objects.startOffset());
        assertEquals(40L, objects.endOffset());
    }


    /**
     * Test get objects with the first hit object is a stream set object.
     */
    @Test
    public void testGetObjectsWithFirstStreamSetObject() {
        DeltaMap<Long, S3StreamSetObject> broker0Objects = DeltaMap.of(
                0L, new S3StreamSetObject(0, BROKER0, List.of(new StreamOffsetRange(STREAM0, 10L, 20L)), 0L),
                1L, new S3StreamSetObject(1, BROKER0, List.of(new StreamOffsetRange(STREAM0, 40L, 60L)), 1L));
        NodeS3StreamSetObjectMetadataImage broker0WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH,
                broker0Objects);
        List<RangeMetadata> ranges = List.of(
                new RangeMetadata(STREAM0, 0L, 0, 10L, 40L, BROKER0),
                new RangeMetadata(STREAM0, 2L, 2, 40L, 60L, BROKER0));
        List<S3StreamObject> streamObjects = List.of(
                new S3StreamObject(8, STREAM0, 20L, 40L, S3StreamConstant.INVALID_TS));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 4L, StreamState.OPENED, 10, ranges, streamObjects);
        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(STREAM0, DeltaMap.of(STREAM0, streamImage),
                DeltaMap.of(BROKER0, broker0WALMetadataImage));

        InRangeObjects objects = streamsImage.getObjects(STREAM0, 12L, 30, 4);
        assertEquals(2, objects.objects().size());
        assertEquals(10L, objects.startOffset());
        assertEquals(40L, objects.endOffset());

        objects = streamsImage.getObjects(STREAM0, 12L, 30, 1);
        assertEquals(1, objects.objects().size());
        assertEquals(10L, objects.startOffset());
        assertEquals(20L, objects.endOffset());
    }
}
