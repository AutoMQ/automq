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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectStreamIndex;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
        S3StreamsMetadataImage image1 = new S3StreamsMetadataImage(0, Collections.emptyMap(), Collections.emptyMap());
        assertEquals(image1, delta0.apply());
        testToImageAndBack(image1);

        ApiMessageAndVersion record1 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
            .setAssignedStreamId(10), (short) 0);
        S3StreamsMetadataDelta delta1 = new S3StreamsMetadataDelta(image1);
        RecordTestUtils.replayAll(delta1, List.of(record1));
        S3StreamsMetadataImage image2 = new S3StreamsMetadataImage(10, Collections.emptyMap(), Collections.emptyMap());
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
        List<S3WALObject> broker0WalObjects = List.of(
            new S3WALObject(0, BROKER0, Map.of(STREAM0, List.of(new S3ObjectStreamIndex(STREAM0, 100L, 120L)))),
            new S3WALObject(1, BROKER0, Map.of(STREAM0, List.of(new S3ObjectStreamIndex(STREAM0, 120L, 140L)))),
            new S3WALObject(2, BROKER0, Map.of(STREAM0, List.of(new S3ObjectStreamIndex(STREAM0, 180L, 200L)))),
            new S3WALObject(3, BROKER0, Map.of(STREAM0, List.of(
                new S3ObjectStreamIndex(STREAM0, 400L, 420L), new S3ObjectStreamIndex(STREAM0, 500L, 520L)))),
            new S3WALObject(4, BROKER0, Map.of(STREAM0, List.of(new S3ObjectStreamIndex(STREAM0, 520L, 600L)))));
        List<S3WALObject> broker1WalObjects = List.of(
            new S3WALObject(5, BROKER1, Map.of(STREAM0, List.of(new S3ObjectStreamIndex(STREAM0, 140L, 160L)))),
            new S3WALObject(6, BROKER1, Map.of(STREAM0, List.of(new S3ObjectStreamIndex(STREAM0, 160L, 180L)))),
            new S3WALObject(7, BROKER1, Map.of(STREAM0, List.of(new S3ObjectStreamIndex(STREAM0, 420L, 500L)))));
        BrokerS3WALMetadataImage broker0WALMetadataImage = new BrokerS3WALMetadataImage(BROKER0, broker0WalObjects);
        BrokerS3WALMetadataImage broker1WALMetadataImage = new BrokerS3WALMetadataImage(BROKER1, broker1WalObjects);
        Map<Integer, RangeMetadata> ranges = Map.of(
            0, new RangeMetadata(STREAM0, 0L, 0, 10L, 140L, BROKER0),
            1, new RangeMetadata(STREAM0, 1L, 1, 140L, 180L, BROKER1),
            2, new RangeMetadata(STREAM0, 2L, 2, 180L, 420L, BROKER0),
            3, new RangeMetadata(STREAM0, 3L, 3, 420L, 500L, BROKER1),
            4, new RangeMetadata(STREAM0, 4L, 4, 500L, 600L, BROKER0));
        Map<Long, S3StreamObject> streamObjects = Map.of(
            8L, new S3StreamObject(8, STREAM0, 10L, 100L),
            9L, new S3StreamObject(9, STREAM0, 200L, 300L),
            10L, new S3StreamObject(10, STREAM0, 300L, 400L));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 4L, 10, ranges, streamObjects);
        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(STREAM0, Map.of(STREAM0, streamImage),
            Map.of(BROKER0, broker0WALMetadataImage, BROKER1, broker1WALMetadataImage));

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
        assertEquals(12, objects.objects().size());
        List<Long> expectedObjectIds = List.of(
            8L, 0L, 1L, 5L, 6L, 2L, 9L, 10L, 3L, 7L, 3L, 4L);
        assertEquals(expectedObjectIds, objects.objects().stream().map(S3ObjectMetadata::getObjectId).collect(Collectors.toList()));

        // 4. search stream_0 in [20, 550)
        objects = streamsImage.getObjects(STREAM0, 20, 550, Integer.MAX_VALUE);
        assertEquals(20, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(12, objects.objects().size());
        assertEquals(expectedObjectIds, objects.objects().stream().map(S3ObjectMetadata::getObjectId).collect(Collectors.toList()));

        // 5. search stream_0 in [20, 550) with limit 5
        objects = streamsImage.getObjects(STREAM0, 20, 550, 5);
        assertEquals(20, objects.startOffset());
        assertEquals(180, objects.endOffset());
        assertEquals(5, objects.objects().size());
        assertEquals(expectedObjectIds.subList(0, 5), objects.objects().stream().map(S3ObjectMetadata::getObjectId).collect(Collectors.toList()));

        // 6. search stream_0 in [400, 520)
        objects = streamsImage.getObjects(STREAM0, 400, 520, Integer.MAX_VALUE);
        assertEquals(400, objects.startOffset());
        assertEquals(520, objects.endOffset());
        assertEquals(3, objects.objects().size());
        assertEquals(expectedObjectIds.subList(8, 11), objects.objects().stream().map(S3ObjectMetadata::getObjectId).collect(Collectors.toList()));

        // 7. search stream_0 in [401, 519)
        objects = streamsImage.getObjects(STREAM0, 401, 519, Integer.MAX_VALUE);
        assertEquals(401, objects.startOffset());
        assertEquals(520, objects.endOffset());
        assertEquals(3, objects.objects().size());
        assertEquals(expectedObjectIds.subList(8, 11), objects.objects().stream().map(S3ObjectMetadata::getObjectId).collect(Collectors.toList()));

        // 8. search stream_0 in [399, 521)
        objects = streamsImage.getObjects(STREAM0, 399, 521, Integer.MAX_VALUE);
        assertEquals(399, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(5, objects.objects().size());
        assertEquals(expectedObjectIds.subList(7, 12), objects.objects().stream().map(S3ObjectMetadata::getObjectId).collect(Collectors.toList()));
    }
}
