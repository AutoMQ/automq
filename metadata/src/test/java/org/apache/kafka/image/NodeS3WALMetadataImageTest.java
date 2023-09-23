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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 40)
@Tag("S3Unit")
public class NodeS3WALMetadataImageTest {

    private static final int BROKER0 = 0;

    private static final long STREAM0 = 0;

    private static final long STREAM1 = 1;

    @Test
    public void testS3WALObjects() {
        NodeS3WALMetadataImage image0 = new NodeS3WALMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH, Collections.emptyMap());
        List<ApiMessageAndVersion> delta0Records = new ArrayList<>();
        NodeS3WALMetadataDelta delta0 = new NodeS3WALMetadataDelta(image0);
        // 1. create WALObject0 and WALObject1
        delta0Records.add(new ApiMessageAndVersion(new NodeWALMetadataRecord()
            .setNodeId(BROKER0)
            .setNodeEpoch(1), (short) 0));
        delta0Records.add(new ApiMessageAndVersion(new WALObjectRecord()
            .setObjectId(0L)
            .setNodeId(BROKER0)
            .setOrderId(0L)
            .setStreamsIndex(List.of(
                new WALObjectRecord.StreamIndex()
                    .setStreamId(STREAM0)
                    .setStartOffset(0L)
                    .setEndOffset(100L),
                new WALObjectRecord.StreamIndex()
                    .setStreamId(STREAM1)
                    .setStartOffset(0)
                    .setEndOffset(200))), (short) 0));
        delta0Records.add(new ApiMessageAndVersion(new WALObjectRecord()
            .setObjectId(1L)
            .setNodeId(BROKER0)
            .setOrderId(1L)
            .setStreamsIndex(List.of(
                new WALObjectRecord.StreamIndex()
                    .setStreamId(STREAM0)
                    .setStartOffset(101L)
                    .setEndOffset(200L))), (short) 0));
        RecordTestUtils.replayAll(delta0, delta0Records);
        // verify delta and check image's write
        NodeS3WALMetadataImage image1 = new NodeS3WALMetadataImage(BROKER0, 1,
            Map.of(
                0L, new S3WALObject(0L, BROKER0, Map.of(
                    STREAM0, new StreamOffsetRange(STREAM0, 0L, 100L),
                    STREAM1, new StreamOffsetRange(STREAM1, 0L, 200L)), 0L),
                1L, new S3WALObject(1L, BROKER0, Map.of(
                    STREAM0, new StreamOffsetRange(STREAM0, 101L, 200L)), 1L)));
        assertEquals(image1, delta0.apply());
        testToImageAndBack(image1);

        // 2. remove range of stream0 in WALObject0 and update epoch
        List<ApiMessageAndVersion> delta1Records = new ArrayList<>();
        NodeS3WALMetadataDelta delta1 = new NodeS3WALMetadataDelta(image1);
        delta1Records.add(new ApiMessageAndVersion(new NodeWALMetadataRecord()
            .setNodeId(BROKER0)
            .setNodeEpoch(2), (short) 0));
        delta1Records.add(new ApiMessageAndVersion(new WALObjectRecord()
            .setObjectId(0L)
            .setNodeId(BROKER0)
            .setOrderId(0L)
            .setStreamsIndex(List.of(
                new WALObjectRecord.StreamIndex()
                    .setStreamId(STREAM1)
                    .setStartOffset(0)
                    .setEndOffset(200))), (short) 0));
        RecordTestUtils.replayAll(delta1, delta1Records);
        // verify delta and check image's write
        NodeS3WALMetadataImage image2 = new NodeS3WALMetadataImage(BROKER0, 2,
            Map.of(
                0L, new S3WALObject(0L, BROKER0, Map.of(
                    STREAM1, new StreamOffsetRange(STREAM1, 0L, 200L)), 0L),
                1L, new S3WALObject(1L, BROKER0, Map.of(
                    STREAM0, new StreamOffsetRange(STREAM0, 101L, 200L)), 1L)));
        assertEquals(image2, delta1.apply());
        testToImageAndBack(image2);

        // 3. remove WALObject1
        List<ApiMessageAndVersion> delta2Records = new ArrayList<>();
        NodeS3WALMetadataDelta delta2 = new NodeS3WALMetadataDelta(image2);
        delta2Records.add(new ApiMessageAndVersion(new RemoveWALObjectRecord()
            .setObjectId(1L), (short) 0));
        RecordTestUtils.replayAll(delta2, delta2Records);
        // verify delta and check image's write
        NodeS3WALMetadataImage image3 = new NodeS3WALMetadataImage(BROKER0, 2,
            Map.of(
                0L, new S3WALObject(0L, BROKER0, Map.of(
                    STREAM1, new StreamOffsetRange(STREAM1, 0L, 200L)), 0L)));
        assertEquals(image3, delta2.apply());
        testToImageAndBack(image3);
    }

    private void testToImageAndBack(NodeS3WALMetadataImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        NodeS3WALMetadataDelta delta = new NodeS3WALMetadataDelta(NodeS3WALMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        NodeS3WALMetadataImage newImage = delta.apply();
        assertEquals(image, newImage);
    }

}
