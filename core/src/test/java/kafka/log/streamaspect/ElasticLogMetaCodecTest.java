/*
 * Copyright 2025 AutoMQ HK Limited.
 *
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

package kafka.log.streamaspect;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import org.apache.avro.SchemaNormalization;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the persistent ElasticLogMeta wire contract and legacy compatibility boundary.
 */
@Tag("S3Unit")
public class ElasticLogMetaCodecTest {

    /**
     * Given V5, encoding must retain the legacy JSON representation and remain dual-readable.
     */
    @Test
    public void testV5WritesLegacyJson() {
        ElasticLogMeta expected = exampleMeta();

        ByteBuffer encoded = ElasticLogMetaCodec.encode(expected, AutoMQVersion.V5);

        assertEquals('{', encoded.get(encoded.position()));
        assertMetaEquals(expected, ElasticLogMetaCodec.decode(encoded));
    }

    /**
     * Given a small V6 value, encoding must use an uncompressed V0 envelope with the fixed golden bytes.
     */
    @Test
    public void testV6EmptyMetaGoldenEnvelope() {
        ByteBuffer encoded = ElasticLogMetaCodec.encode(new ElasticLogMeta(), AutoMQVersion.V6);
        byte[] actual = new byte[encoded.remaining()];
        encoded.get(actual);

        byte[] expected = new byte[] {
            0x45, 0x4c, 0x4d, 0x00,
            0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02,
            0x00, 0x00
        };
        assertArrayEquals(expected, actual);
    }

    /**
     * Given every SegmentMeta field, the published V0 Avro field order and binary encoding must remain stable.
     */
    @Test
    public void testV0FullSegmentMetaGoldenPayload() {
        ElasticLogMeta meta = new ElasticLogMeta();
        meta.setSegmentMetas(List.of(exampleSegmentMeta()));

        ByteBuffer encoded = ElasticLogMetaCodec.encode(meta, AutoMQVersion.V6);
        byte[] actual = new byte[encoded.remaining()];
        encoded.get(actual);

        assertEquals("RUxNAAAAAAAAAAAAGQAC9gGQB6oMAjCAEAIEBggKDIIFnAq2DwA=",
            Base64.getEncoder().encodeToString(actual));
    }

    /**
     * Given complete nested metadata, the V0 Avro mapping must preserve every domain field.
     */
    @Test
    public void testV6AvroRoundTrip() {
        ElasticLogMeta expected = exampleMeta();

        ByteBuffer encoded = ElasticLogMetaCodec.encode(expected, AutoMQVersion.V6);

        assertMetaEquals(expected, ElasticLogMetaCodec.decode(encoded));
    }

    /**
     * Given raw Avro at or above the threshold, V6 must use Zstd and retain its exact raw size.
     */
    @Test
    public void testV6UsesZstdForLargeMeta() {
        ElasticLogMeta expected = exampleMeta();
        List<ElasticStreamSegmentMeta> segmentMetas = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ElasticStreamSegmentMeta segmentMeta = exampleSegmentMeta();
            segmentMeta.baseOffset(i * 1024L);
            segmentMeta.streamSuffix("segment-suffix-" + i);
            segmentMetas.add(segmentMeta);
        }
        expected.setSegmentMetas(segmentMetas);

        ByteBuffer encoded = ElasticLogMetaCodec.encode(expected, AutoMQVersion.V6);
        ByteBuf envelope = Unpooled.wrappedBuffer(encoded.duplicate());
        assertEquals(ElasticLogMetaCodec.MAGIC, envelope.readInt());
        assertEquals(ElasticLogMetaCodec.ENCODING_VERSION_V0, envelope.readUnsignedByte());
        assertEquals(CompressionType.ZSTD.id, envelope.readInt());
        int uncompressedSize = envelope.readInt();
        assertTrue(uncompressedSize >= ElasticLogMetaCodec.COMPRESSION_THRESHOLD);
        assertEquals(ElasticLogMetaCodec.HEADER_SIZE, envelope.readerIndex());
        assertMetaEquals(expected, ElasticLogMetaCodec.decode(encoded));
    }

    /**
     * Given the writer threshold, only raw Avro payloads of at least 16 KiB must use Zstd.
     */
    @Test
    public void testCompressionThresholdBoundary() {
        assertEquals(CompressionType.NONE,
            ElasticLogMetaCodec.compressionTypeForSize(ElasticLogMetaCodec.COMPRESSION_THRESHOLD - 1));
        assertEquals(CompressionType.ZSTD,
            ElasticLogMetaCodec.compressionTypeForSize(ElasticLogMetaCodec.COMPRESSION_THRESHOLD));
    }

    /**
     * Given a published V0 schema, its parsing fingerprint must remain immutable.
     */
    @Test
    public void testV0SchemaFingerprint() {
        assertEquals(3460547824240030597L,
            SchemaNormalization.parsingFingerprint64(ElasticLogMetaCodec.SCHEMA_V0));
    }

    /**
     * Given a recognized magic, malformed envelopes must fail closed instead of falling back to JSON.
     */
    @Test
    public void testMalformedEnvelopesFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> ElasticLogMetaCodec.decode(
            envelope(1, 0, 0, new byte[0])));
        assertThrows(IllegalArgumentException.class, () -> ElasticLogMetaCodec.decode(
            envelope(0, 0x08, 0, new byte[0])));
        assertThrows(IllegalArgumentException.class, () -> ElasticLogMetaCodec.decode(
            envelope(0, CompressionType.GZIP.id, 0, new byte[0])));
        assertThrows(IllegalArgumentException.class, () -> ElasticLogMetaCodec.decode(
            envelope(0, CompressionType.NONE.id, -1, new byte[0])));
        assertThrows(IllegalArgumentException.class, () -> ElasticLogMetaCodec.decode(
            envelope(0, CompressionType.NONE.id, 1, new byte[0])));
        assertThrows(IllegalArgumentException.class, () -> ElasticLogMetaCodec.decode(
            envelope(0, CompressionType.ZSTD.id, 32, new byte[] {1, 2, 3})));

        ByteBuffer truncatedHeader = ByteBuffer.allocate(Integer.BYTES).putInt(ElasticLogMetaCodec.MAGIC);
        truncatedHeader.flip();
        assertThrows(IllegalArgumentException.class, () -> ElasticLogMetaCodec.decode(truncatedHeader));
    }

    private static ByteBuffer envelope(int version, int attributes, int uncompressedSize, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(ElasticLogMetaCodec.HEADER_SIZE + payload.length);
        buffer.putInt(ElasticLogMetaCodec.MAGIC);
        buffer.put((byte) version);
        buffer.putInt(attributes);
        buffer.putInt(uncompressedSize);
        buffer.put(payload);
        buffer.flip();
        return buffer;
    }

    private static ElasticLogMeta exampleMeta() {
        ElasticLogMeta meta = new ElasticLogMeta();
        Map<String, Long> streamMap = new HashMap<>();
        streamMap.put("log0", 11L);
        streamMap.put("tim0", 12L);
        streamMap.put("txn0", 13L);
        meta.setStreamMap(streamMap);
        meta.setSegmentMetas(List.of(exampleSegmentMeta()));
        return meta;
    }

    private static ElasticStreamSegmentMeta exampleSegmentMeta() {
        ElasticStreamSegmentMeta meta = new ElasticStreamSegmentMeta();
        meta.baseOffset(123L);
        meta.createTimestamp(456L);
        meta.lastModifiedTimestamp(789L);
        meta.streamSuffix("0");
        meta.logSize(1024);
        meta.log(SliceRange.of(1L, 2L));
        meta.time(SliceRange.of(3L, 4L));
        meta.txn(SliceRange.of(5L, 6L));
        meta.firstBatchTimestamp(321L);
        meta.timeIndexLastEntry(ElasticStreamSegmentMeta.TimestampOffsetData.of(654L, 987L));
        return meta;
    }

    private static void assertMetaEquals(ElasticLogMeta expected, ElasticLogMeta actual) {
        assertEquals(expected.getStreamMap(), actual.getStreamMap());
        assertEquals(expected.getSegmentMetas().size(), actual.getSegmentMetas().size());
        for (int i = 0; i < expected.getSegmentMetas().size(); i++) {
            assertSegmentMetaEquals(expected.getSegmentMetas().get(i), actual.getSegmentMetas().get(i));
        }
    }

    private static void assertSegmentMetaEquals(ElasticStreamSegmentMeta expected, ElasticStreamSegmentMeta actual) {
        assertEquals(expected.baseOffset(), actual.baseOffset());
        assertEquals(expected.createTimestamp(), actual.createTimestamp());
        assertEquals(expected.lastModifiedTimestamp(), actual.lastModifiedTimestamp());
        assertEquals(expected.streamSuffix(), actual.streamSuffix());
        assertEquals(expected.logSize(), actual.logSize());
        assertSliceRangeEquals(expected.log(), actual.log());
        assertSliceRangeEquals(expected.time(), actual.time());
        assertSliceRangeEquals(expected.txn(), actual.txn());
        assertEquals(expected.firstBatchTimestamp(), actual.firstBatchTimestamp());
        assertEquals(expected.timeIndexLastEntry().timestamp(), actual.timeIndexLastEntry().timestamp());
        assertEquals(expected.timeIndexLastEntry().offset(), actual.timeIndexLastEntry().offset());
    }

    private static void assertSliceRangeEquals(SliceRange expected, SliceRange actual) {
        assertEquals(expected.start(), actual.start());
        assertEquals(expected.end(), actual.end());
    }
}
