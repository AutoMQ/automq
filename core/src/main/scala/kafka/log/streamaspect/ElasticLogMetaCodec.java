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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

/**
 * Encodes and decodes the persistent representation of {@link ElasticLogMeta}.
 * New values use a versioned Avro envelope while legacy JSON remains readable.
 */
final class ElasticLogMetaCodec {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ObjectWriter JSON_WRITER = JSON_MAPPER.writer();
    private static final ObjectReader JSON_READER = JSON_MAPPER.readerFor(ElasticLogMeta.class);
    static final int MAGIC = 0x454C4D00;
    static final int HEADER_SIZE = 13;
    static final int COMPRESSION_MASK = 0x07;
    static final int COMPRESSION_THRESHOLD = 16 * 1024;
    static final int ZSTD_LEVEL = 3;
    static final int ENCODING_VERSION_V0 = 0;

    private static final String SCHEMA_NAMESPACE = "kafka.log.streamaspect";

    private static final Schema SLICE_RANGE_SCHEMA = SchemaBuilder.record("SliceRange")
        .namespace(SCHEMA_NAMESPACE)
        .fields()
        .requiredLong("start")
        .requiredLong("end")
        .endRecord();

    private static final Schema TIMESTAMP_OFFSET_SCHEMA = SchemaBuilder.record("TimestampOffset")
        .namespace(SCHEMA_NAMESPACE)
        .fields()
        .requiredLong("timestamp")
        .requiredLong("offset")
        .endRecord();

    private static final Schema SEGMENT_META_SCHEMA = SchemaBuilder.record("SegmentMeta")
        .namespace(SCHEMA_NAMESPACE)
        .fields()
        .requiredLong("baseOffset")
        .requiredLong("createTimestamp")
        .requiredLong("lastModifiedTimestamp")
        .requiredString("streamSuffix")
        .requiredInt("logSize")
        .name("log").type(SLICE_RANGE_SCHEMA).noDefault()
        .name("time").type(SLICE_RANGE_SCHEMA).noDefault()
        .name("txn").type(SLICE_RANGE_SCHEMA).noDefault()
        .requiredLong("firstBatchTimestamp")
        .name("timeIndexLastEntry").type(TIMESTAMP_OFFSET_SCHEMA).noDefault()
        .endRecord();

    static final Schema SCHEMA_V0 = SchemaBuilder.record("ElasticLogMeta")
        .namespace(SCHEMA_NAMESPACE)
        .fields()
        .name("streamMap").type().map().values().longType().noDefault()
        .name("segmentMetas").type().array().items(SEGMENT_META_SCHEMA).noDefault()
        .endRecord();

    private ElasticLogMetaCodec() {
    }

    /** Encodes metadata using the legacy JSON representation. */
    static ByteBuffer encodeJson(ElasticLogMeta meta) {
        try {
            return ByteBuffer.wrap(JSON_WRITER.writeValueAsBytes(meta));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to encode ElasticLogMeta as JSON", e);
        }
    }

    /** Decodes metadata using the legacy JSON representation. */
    static ElasticLogMeta decodeJson(ByteBuffer encoded) {
        try {
            return JSON_READER.readValue(StandardCharsets.UTF_8.decode(encoded).toString());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to decode ElasticLogMeta JSON", e);
        }
    }

    /**
     * Encodes metadata according to the finalized AutoMQ feature version.
     */
    static ByteBuffer encode(ElasticLogMeta meta, AutoMQVersion version) {
        if (!version.isElasticLogMetaAvroSupported()) {
            return encodeJson(meta);
        }
        byte[] rawPayload = encodeAvro(meta);
        CompressionType compressionType = compressionTypeForSize(rawPayload.length);
        byte[] payload = compressionType == CompressionType.NONE
            ? rawPayload
            : Zstd.compress(rawPayload, ZSTD_LEVEL);

        ByteBuf envelope = Unpooled.wrappedBuffer(new byte[HEADER_SIZE + payload.length]);
        envelope.clear();
        envelope.writeInt(MAGIC);
        envelope.writeByte(ENCODING_VERSION_V0);
        envelope.writeInt(compressionType.id);
        envelope.writeInt(rawPayload.length);
        envelope.writeBytes(payload);
        return envelope.nioBuffer(0, envelope.writerIndex());
    }

    static CompressionType compressionTypeForSize(int rawPayloadSize) {
        return rawPayloadSize < COMPRESSION_THRESHOLD ? CompressionType.NONE : CompressionType.ZSTD;
    }

    /**
     * Decodes either a legacy JSON value or a self-identifying Avro envelope.
     */
    static ElasticLogMeta decode(ByteBuffer encoded) {
        ByteBuffer input = encoded.slice();
        if (input.remaining() < Integer.BYTES || input.getInt(input.position()) != MAGIC) {
            return decodeJson(input);
        }
        return decodeEnvelope(Unpooled.wrappedBuffer(input));
    }

    private static ElasticLogMeta decodeEnvelope(ByteBuf envelope) {
        if (envelope.readableBytes() < HEADER_SIZE) {
            throw new IllegalArgumentException("Truncated ElasticLogMeta envelope header");
        }
        envelope.readInt();
        int encodingVersion = envelope.readUnsignedByte();
        if (encodingVersion != ENCODING_VERSION_V0) {
            throw new IllegalArgumentException("Unsupported ElasticLogMeta encoding version: " + encodingVersion);
        }

        int attributes = envelope.readInt();
        if ((attributes & ~COMPRESSION_MASK) != 0) {
            throw new IllegalArgumentException("Unsupported ElasticLogMeta attributes: " + attributes);
        }
        int compressionId = attributes & COMPRESSION_MASK;
        if (compressionId != CompressionType.NONE.id && compressionId != CompressionType.ZSTD.id) {
            throw new IllegalArgumentException("Unsupported ElasticLogMeta compression type: " + compressionId);
        }
        CompressionType compressionType = CompressionType.forId(compressionId);

        int uncompressedSize = envelope.readInt();
        if (uncompressedSize < 0) {
            throw new IllegalArgumentException("Negative ElasticLogMeta uncompressed size: " + uncompressedSize);
        }
        return compressionType == CompressionType.NONE
            ? decodeUncompressed(envelope, uncompressedSize)
            : decodeCompressed(envelope, uncompressedSize);
    }

    private static ElasticLogMeta decodeUncompressed(ByteBuf payload, int uncompressedSize) {
        if (payload.readableBytes() != uncompressedSize) {
            throw new IllegalArgumentException("ElasticLogMeta payload size does not match uncompressed size");
        }
        return decodeAvro(payload);
    }

    private static ElasticLogMeta decodeCompressed(ByteBuf payload, int uncompressedSize) {
        byte[] compressedPayload = new byte[payload.readableBytes()];
        payload.readBytes(compressedPayload);
        byte[] rawPayload = new byte[uncompressedSize];
        long actualSize;
        try {
            actualSize = Zstd.decompress(rawPayload, compressedPayload);
        } catch (ZstdException e) {
            throw new IllegalArgumentException("Failed to decompress ElasticLogMeta", e);
        }
        if (Zstd.isError(actualSize)) {
            throw new IllegalArgumentException("Failed to decompress ElasticLogMeta: " + Zstd.getErrorName(actualSize));
        }
        if (actualSize != uncompressedSize) {
            throw new IllegalArgumentException("ElasticLogMeta decompressed size " + actualSize
                + " does not match declared size " + uncompressedSize);
        }
        return decodeAvro(rawPayload);
    }

    private static byte[] encodeAvro(ElasticLogMeta meta) {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<GenericRecord>(SCHEMA_V0).write(toRecord(meta), encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to encode ElasticLogMeta as Avro", e);
        }
    }

    private static ElasticLogMeta decodeAvro(ByteBuf payload) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteBufInputStream(payload), null);
            return readAvro(decoder);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode ElasticLogMeta Avro payload", e);
        }
    }

    private static ElasticLogMeta decodeAvro(byte[] payload) {
        return decodeAvro(Unpooled.wrappedBuffer(payload));
    }

    @SuppressWarnings("unchecked")
    private static ElasticLogMeta readAvro(BinaryDecoder decoder) throws IOException {
        GenericRecord record = new GenericDatumReader<GenericRecord>(SCHEMA_V0).read(null, decoder);
        if (!decoder.isEnd()) {
            throw new IllegalArgumentException("Trailing bytes in ElasticLogMeta Avro payload");
        }
        ElasticLogMeta meta = new ElasticLogMeta();
        Map<String, Long> streamMap = new HashMap<>();
        ((Map<CharSequence, Long>) record.get("streamMap"))
            .forEach((key, value) -> streamMap.put(key.toString(), value));
        meta.setStreamMap(streamMap);

        List<ElasticStreamSegmentMeta> segmentMetas = new ArrayList<>();
        for (GenericRecord segmentRecord : (List<GenericRecord>) record.get("segmentMetas")) {
            segmentMetas.add(segmentMetaFromRecord(segmentRecord));
        }
        meta.setSegmentMetas(segmentMetas);
        return meta;
    }

    private static GenericRecord toRecord(ElasticLogMeta meta) {
        GenericRecord record = new GenericData.Record(SCHEMA_V0);
        record.put("streamMap", meta.getStreamMap());
        List<GenericRecord> segmentRecords = new ArrayList<>(meta.getSegmentMetas().size());
        for (ElasticStreamSegmentMeta segmentMeta : meta.getSegmentMetas()) {
            segmentRecords.add(toRecord(segmentMeta));
        }
        record.put("segmentMetas", segmentRecords);
        return record;
    }

    private static GenericRecord toRecord(ElasticStreamSegmentMeta meta) {
        GenericRecord record = new GenericData.Record(SEGMENT_META_SCHEMA);
        record.put("baseOffset", meta.baseOffset());
        record.put("createTimestamp", meta.createTimestamp());
        record.put("lastModifiedTimestamp", meta.lastModifiedTimestamp());
        record.put("streamSuffix", meta.streamSuffix());
        record.put("logSize", meta.logSize());
        record.put("log", toRecord(meta.log()));
        record.put("time", toRecord(meta.time()));
        record.put("txn", toRecord(meta.txn()));
        record.put("firstBatchTimestamp", meta.firstBatchTimestamp());
        record.put("timeIndexLastEntry", toRecord(meta.timeIndexLastEntry()));
        return record;
    }

    private static GenericRecord toRecord(SliceRange range) {
        GenericRecord record = new GenericData.Record(SLICE_RANGE_SCHEMA);
        record.put("start", range.start());
        record.put("end", range.end());
        return record;
    }

    private static GenericRecord toRecord(ElasticStreamSegmentMeta.TimestampOffsetData timestampOffset) {
        GenericRecord record = new GenericData.Record(TIMESTAMP_OFFSET_SCHEMA);
        record.put("timestamp", timestampOffset.timestamp());
        record.put("offset", timestampOffset.offset());
        return record;
    }

    private static ElasticStreamSegmentMeta segmentMetaFromRecord(GenericRecord record) {
        ElasticStreamSegmentMeta meta = new ElasticStreamSegmentMeta();
        meta.baseOffset((Long) record.get("baseOffset"));
        meta.createTimestamp((Long) record.get("createTimestamp"));
        meta.lastModifiedTimestamp((Long) record.get("lastModifiedTimestamp"));
        meta.streamSuffix(record.get("streamSuffix").toString());
        meta.logSize((Integer) record.get("logSize"));
        meta.log(sliceRangeFromRecord((GenericRecord) record.get("log")));
        meta.time(sliceRangeFromRecord((GenericRecord) record.get("time")));
        meta.txn(sliceRangeFromRecord((GenericRecord) record.get("txn")));
        meta.firstBatchTimestamp((Long) record.get("firstBatchTimestamp"));
        meta.timeIndexLastEntry(timestampOffsetFromRecord((GenericRecord) record.get("timeIndexLastEntry")));
        return meta;
    }

    private static SliceRange sliceRangeFromRecord(GenericRecord record) {
        return SliceRange.of((Long) record.get("start"), (Long) record.get("end"));
    }

    private static ElasticStreamSegmentMeta.TimestampOffsetData timestampOffsetFromRecord(GenericRecord record) {
        return ElasticStreamSegmentMeta.TimestampOffsetData.of(
            (Long) record.get("timestamp"), (Long) record.get("offset"));
    }
}
