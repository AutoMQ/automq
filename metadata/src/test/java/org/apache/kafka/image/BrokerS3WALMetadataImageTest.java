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
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.metadata.BrokerWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.metadata.stream.SortedWALObjectsList;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 40)
@Tag("S3Unit")
public class BrokerS3WALMetadataImageTest {

    private static final int BROKER0 = 0;

    private static final long STREAM0 = 0;

    private static final long STREAM1 = 1;

    @Test
    public void testS3WALObjects() {
        BrokerS3WALMetadataImage image0 = new BrokerS3WALMetadataImage(BROKER0, new SortedWALObjectsList());
        List<ApiMessageAndVersion> delta0Records = new ArrayList<>();
        BrokerS3WALMetadataDelta delta0 = new BrokerS3WALMetadataDelta(image0);
        // 1. create WALObject0 and WALObject1
        delta0Records.add(new ApiMessageAndVersion(new BrokerWALMetadataRecord()
            .setBrokerId(BROKER0), (short) 0));
        delta0Records.add(new ApiMessageAndVersion(new WALObjectRecord()
            .setObjectId(0L)
            .setBrokerId(BROKER0)
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
            .setBrokerId(BROKER0)
            .setOrderId(1L)
            .setStreamsIndex(List.of(
                new WALObjectRecord.StreamIndex()
                    .setStreamId(STREAM0)
                    .setStartOffset(101L)
                    .setEndOffset(200L))), (short) 0));
        RecordTestUtils.replayAll(delta0, delta0Records);
        // verify delta and check image's write
        BrokerS3WALMetadataImage image1 = new BrokerS3WALMetadataImage(BROKER0, new SortedWALObjectsList(List.of(
            new S3WALObject(0L, BROKER0, Map.of(
                STREAM0, List.of(new StreamOffsetRange(STREAM0, 0L, 100L)),
                STREAM1, List.of(new StreamOffsetRange(STREAM1, 0L, 200L))), 0L),
            new S3WALObject(1L, BROKER0, Map.of(
                STREAM0, List.of(new StreamOffsetRange(STREAM0, 101L, 200L))), 1L))));
        assertEquals(image1, delta0.apply());
        testToImageAndBack(image1);

        // 2. remove WALObject0
        List<ApiMessageAndVersion> delta1Records = new ArrayList<>();
        BrokerS3WALMetadataDelta delta1 = new BrokerS3WALMetadataDelta(image1);
        delta1Records.add(new ApiMessageAndVersion(new RemoveWALObjectRecord()
            .setObjectId(0L), (short) 0));
        RecordTestUtils.replayAll(delta1, delta1Records);
        // verify delta and check image's write
        BrokerS3WALMetadataImage image2 = new BrokerS3WALMetadataImage(BROKER0, new SortedWALObjectsList(List.of(
            new S3WALObject(1L, BROKER0, Map.of(
                STREAM0, List.of(new StreamOffsetRange(STREAM0, 101L, 200L))), 1L))));
        assertEquals(image2, delta1.apply());
        testToImageAndBack(image2);
    }

    private void testToImageAndBack(BrokerS3WALMetadataImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        BrokerS3WALMetadataDelta delta = new BrokerS3WALMetadataDelta(BrokerS3WALMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        BrokerS3WALMetadataImage newImage = delta.apply();
        assertEquals(image, newImage);
    }

}
