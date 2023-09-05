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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.KVRecord.KeyValue;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
@Tag("S3Unit")
public class KVImageTest {

    final static KVImage IMAGE1;

    final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static KVDelta DELTA1;

    final static KVImage IMAGE2;


    static {
        Map<String, ByteBuffer> map = Map.of(
            "key1", ByteBuffer.wrap(new String("value1").getBytes()),
            "key2", ByteBuffer.wrap(new String("value2").getBytes()));
        IMAGE1 = new KVImage(map);
        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new KVRecord()
            .setKeyValues(List.of(
                new KeyValue()
                    .setKey("key3")
                    .setValue("value3".getBytes()),
                new KeyValue()
                    .setKey("key2")
                    .setValue("value2".getBytes()))), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveKVRecord()
            .setKeys(List.of("key1")), (short) 0));
        DELTA1 = new KVDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);

        Map<String, ByteBuffer> map2 = Map.of(
            "key2", ByteBuffer.wrap(new String("value2").getBytes()),
            "key3", ByteBuffer.wrap(new String("value3").getBytes()));
        IMAGE2 = new KVImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImageAndBack(KVImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(KVImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        KVDelta delta = new KVDelta(KVImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        KVImage newImage = delta.apply();
        assertEquals(image, newImage);
    }
}
