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
import org.apache.kafka.controller.stream.KVKey;
import org.apache.kafka.controller.stream.KVNamespace;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
@Tag("S3Unit")
public class KVImageTest {

    // AutoMQ inject start
    /**
     * Given the globally shared empty image, releasing one owner's reference must not invalidate future owners.
     */
    @ParameterizedTest
    @ValueSource(strings = {"kv", "streams", "objects"})
    public void testEmptyImageCanBeReusedAfterRelease(String type) {
        ReferenceCounted empty;
        switch (type) {
            case "kv":
                empty = KVImage.EMPTY;
                break;
            case "streams":
                empty = S3StreamsMetadataImage.EMPTY;
                break;
            default:
                empty = S3ObjectsImage.EMPTY;
        }
        empty.release();
        assertDoesNotThrow(() -> {
            assertSame(empty, empty.retain());
        });
        assertSame(empty, empty.retain(2));
        assertFalse(empty.release(2));
        assertFalse(empty.release());
        assertEquals(1, empty.refCnt());
        assertThrows(IllegalArgumentException.class, () -> empty.retain(0));
        assertThrows(IllegalArgumentException.class, () -> empty.release(0));
        assertThrows(IllegalArgumentException.class, () -> empty.retain(-1));
        assertThrows(IllegalArgumentException.class, () -> empty.release(-1));
    }

    /**
     * Given an ordinary image with a successor, retain/release overloads retire its epoch and reject reuse.
     */
    @ParameterizedTest
    @ValueSource(strings = {"kv", "streams", "objects"})
    public void testOrdinaryImageKeepsReferenceCountingContract(String type) {
        ReferenceCounted first;
        RegistryRef registryRef;
        switch (type) {
            case "kv":
                KVImage kv = new KVDelta(KVImage.EMPTY).apply();
                new KVDelta(kv).apply();
                first = kv;
                registryRef = kv.registryRef();
                break;
            case "streams":
                S3StreamsMetadataImage streams = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY).apply();
                new S3StreamsMetadataDelta(streams).apply();
                first = streams;
                registryRef = streams.registryRef();
                break;
            default:
                S3ObjectsImage objects = new S3ObjectsDelta(S3ObjectsImage.EMPTY).apply();
                new S3ObjectsDelta(objects).apply();
                first = objects;
                registryRef = objects.registryRef();
        }
        assertEquals(1, first.refCnt());
        assertEquals(2, registryRef.registry().epochsList().size());
        assertSame(first, first.retain());
        assertSame(first, first.retain(2));
        assertEquals(4, first.refCnt());
        assertFalse(first.release(2));
        assertEquals(2, first.refCnt());
        assertFalse(first.release());
        assertEquals(1, first.refCnt());
        assertThrows(IllegalArgumentException.class, () -> first.retain(0));
        assertThrows(IllegalArgumentException.class, () -> first.retain(-1));
        assertThrows(IllegalArgumentException.class, () -> first.release(0));
        assertThrows(IllegalArgumentException.class, () -> first.release(-1));
        assertThrows(IllegalReferenceCountException.class, () -> first.release(2));
        assertEquals(1, first.refCnt());
        assertTrue(first.release(1));
        assertEquals(0, first.refCnt());
        assertEquals(0, registryRef.refCnt());
        assertEquals(1, registryRef.registry().epochsList().size());
        assertThrows(IllegalReferenceCountException.class, first::retain);
        assertThrows(IllegalReferenceCountException.class, () -> first.retain(1));
        assertThrows(IllegalReferenceCountException.class, first::release);
        assertThrows(IllegalReferenceCountException.class, () -> first.release(1));
    }
    // AutoMQ inject end

    static final KVImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final KVDelta DELTA1;

    static final KVImage IMAGE2;


    static {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map = kvMap(registry);
        RegistryRef ref = new RegistryRef(registry, 0, new ArrayList<>());
        put(map, registry, null, "key1", ByteBuffer.wrap("value1".getBytes()));
        put(map, registry, null, "key2", ByteBuffer.wrap("value2".getBytes()));
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
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map2 = kvMap(registry);
        RegistryRef ref2 = new RegistryRef(registry, 0, new ArrayList<>());

        put(map2, registry, null, "key2", ByteBuffer.wrap("value2".getBytes()));
        put(map2, registry, null, "key3", ByteBuffer.wrap("value3".getBytes()));
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
    public void testFinishSnapshotRemovesMissingKVs() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map = kvMap(registry);
        put(map, registry, null, "key1", ByteBuffer.wrap("value1".getBytes()));
        put(map, registry, null, "key2", ByteBuffer.wrap("value2".getBytes()));
        registry.getOrCreateSnapshot(0);
        KVDelta delta = new KVDelta(new KVImage(map, new RegistryRef(registry, 0, new ArrayList<>())));
        delta.replay(new KVRecord()
            .setKeyValues(List.of(new KeyValue()
                .setKey("key2")
                .setValue("value2".getBytes()))));
        delta.finishSnapshot();

        assertEquals(Set.of(KVKey.of("key1")), delta.removedKeys());
        assertEquals(Set.of(KVKey.of("key2")), delta.changedKV().keySet());

        assertEquals(Map.of("key2", ByteBuffer.wrap("value2".getBytes())), delta.apply().namespaceKVs(null));
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

    private static TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> kvMap(SnapshotRegistry registry) {
        return new TimelineHashMap<>(registry, 10000);
    }

    private static void put(
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map,
        SnapshotRegistry registry,
        String namespace,
        String key,
        ByteBuffer value
    ) {
        KVNamespace kvNamespace = KVNamespace.of(namespace);
        TimelineHashMap<String, ByteBuffer> namespaceKVs = map.get(kvNamespace);
        if (namespaceKVs == null) {
            namespaceKVs = new TimelineHashMap<>(registry, 1000);
            map.put(kvNamespace, namespaceKVs);
        }
        namespaceKVs.put(key, value);
    }
}
