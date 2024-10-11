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

import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.KVRecord.KeyValue;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(40)
@Tag("S3Unit")
public class KVImageTest {

    static final KVImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final KVDelta DELTA1;

    static final KVImage IMAGE2;


    static {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<String, ByteBuffer> map = new TimelineHashMap<>(registry, 10000);
        RegistryRef ref = new RegistryRef(registry, 0, new ArrayList<>());
        map.put("key1", ByteBuffer.wrap(new String("value1").getBytes()));
        map.put("key2", ByteBuffer.wrap(new String("value2").getBytes()));
        registry.getOrCreateSnapshot(0);

        IMAGE1 = new KVImage(map, ref);
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

        registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<String, ByteBuffer> map2 = new TimelineHashMap<>(registry, 10000);
        RegistryRef ref2 = new RegistryRef(registry, 0, new ArrayList<>());

        map2.put("key2", ByteBuffer.wrap(new String("value2").getBytes()));
        map2.put("key3", ByteBuffer.wrap(new String("value3").getBytes()));
        registry.getOrCreateSnapshot(0);

        IMAGE2 = new KVImage(map2, ref2);
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
