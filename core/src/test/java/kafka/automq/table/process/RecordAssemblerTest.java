/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.automq.table.process;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("S3Unit")
class RecordAssemblerTest {

    @Test
    void ensureOptionalShouldWrapNonUnionSchema() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);

        Schema optionalSchema = RecordAssembler.ensureOptional(stringSchema);

        assertEquals(Schema.Type.UNION, optionalSchema.getType());
        List<Schema> types = optionalSchema.getTypes();
        assertEquals(2, types.size());
        assertEquals(Schema.Type.NULL, types.get(0).getType());
        assertSame(stringSchema, types.get(1));
    }

    @Test
    void ensureOptionalShouldPrefixNullWhenMissing() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema intSchema = Schema.create(Schema.Type.INT);
        Schema unionWithoutNull = Schema.createUnion(List.of(stringSchema, intSchema));

        Schema optionalSchema = RecordAssembler.ensureOptional(unionWithoutNull);

        assertEquals(Schema.Type.UNION, optionalSchema.getType());
        List<Schema> types = optionalSchema.getTypes();
        assertEquals(3, types.size());
        assertEquals(Schema.Type.NULL, types.get(0).getType());
        assertSame(stringSchema, types.get(1));
        assertSame(intSchema, types.get(2));
    }

    @Test
    void ensureOptionalShouldReturnOriginalUnionWhenNullPresent() {
        Schema unionWithNull = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));

        Schema result = RecordAssembler.ensureOptional(unionWithNull);

        assertSame(unionWithNull, result);
    }

    @Test
    void assembleShouldExposeOptionalSyntheticFields() {
        Schema baseSchema = SchemaBuilder.record("BaseRecord")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("name").type().stringType().noDefault()
            .endRecord();
        GenericRecord baseRecord = new GenericData.Record(baseSchema);
        baseRecord.put("name", "Alice");

        Schema headerSchema = SchemaBuilder.record("HeaderRecord")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("headerKey").type().stringType().noDefault()
            .endRecord();
        GenericRecord headerRecord = new GenericData.Record(headerSchema);
        headerRecord.put("headerKey", "headerValue");
        ConversionResult headerResult = new ConversionResult(headerRecord, "header-identity");

        Schema keySchema = Schema.create(Schema.Type.BYTES);
        ByteBuffer keyValue = ByteBuffer.wrap("key-value".getBytes(StandardCharsets.UTF_8));
        ConversionResult keyResult = new ConversionResult(keyValue, keySchema, "key-identity");

        int partition = 5;
        long offset = 42L;
        long timestamp = 1_700_000_000_000L;

        RecordAssembler assembler = new RecordAssembler();
        GenericRecord assembledRecord = assembler.reset(baseRecord)
            .withHeader(headerResult)
            .withKey(keyResult)
            .withMetadata(partition, offset, timestamp)
            .assemble();

        Schema finalSchema = assembledRecord.getSchema();
        assertEquals(4, finalSchema.getFields().size());

        Schema headerFieldSchema = finalSchema.getField(RecordAssembler.KAFKA_HEADER_FIELD).schema();
        assertEquals(Schema.Type.UNION, headerFieldSchema.getType());
        assertEquals(Schema.Type.NULL, headerFieldSchema.getTypes().get(0).getType());
        assertSame(headerSchema, headerFieldSchema.getTypes().get(1));

        Schema keyFieldSchema = finalSchema.getField(RecordAssembler.KAFKA_KEY_FIELD).schema();
        assertEquals(Schema.Type.UNION, keyFieldSchema.getType());
        assertEquals(Schema.Type.NULL, keyFieldSchema.getTypes().get(0).getType());
        assertSame(keySchema, keyFieldSchema.getTypes().get(1));

        Schema metadataFieldSchema = finalSchema.getField(RecordAssembler.KAFKA_METADATA_FIELD).schema();
        assertEquals(Schema.Type.UNION, metadataFieldSchema.getType());
        assertEquals(Schema.Type.NULL, metadataFieldSchema.getTypes().get(0).getType());
        Schema metadataSchema = metadataFieldSchema.getTypes().get(1);
        assertEquals("KafkaMetadata", metadataSchema.getName());

        assertEquals("Alice", assembledRecord.get("name").toString());
        assertSame(headerRecord, assembledRecord.get(RecordAssembler.KAFKA_HEADER_FIELD));
        assertSame(keyValue, assembledRecord.get(RecordAssembler.KAFKA_KEY_FIELD));

        GenericRecord metadataRecord = (GenericRecord) assembledRecord.get(RecordAssembler.KAFKA_METADATA_FIELD);
        assertNotNull(metadataRecord);
        assertEquals(partition, metadataRecord.get(RecordAssembler.METADATA_PARTITION_FIELD));
        assertEquals(offset, metadataRecord.get(RecordAssembler.METADATA_OFFSET_FIELD));
        assertEquals(timestamp, metadataRecord.get(RecordAssembler.METADATA_TIMESTAMP_FIELD));
    }

    @Test
    void assembleShouldSkipHeaderWhenAbsent() {
        Schema baseSchema = SchemaBuilder.record("BaseRecordNoHeader")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("id").type().longType().noDefault()
            .endRecord();
        GenericRecord baseRecord = new GenericData.Record(baseSchema);
        baseRecord.put("id", 100L);

        Schema keySchema = Schema.create(Schema.Type.STRING);
        ConversionResult keyResult = new ConversionResult("primary-key", keySchema, "key-identity");

        RecordAssembler assembler = new RecordAssembler();
        GenericRecord assembledRecord = assembler.reset(baseRecord)
            .withKey(keyResult)
            .withMetadata(1, 2L, 3L)
            .assemble();

        Schema finalSchema = assembledRecord.getSchema();
        assertNull(finalSchema.getField(RecordAssembler.KAFKA_HEADER_FIELD));
        assertNotNull(finalSchema.getField(RecordAssembler.KAFKA_KEY_FIELD));
        assertNotNull(finalSchema.getField(RecordAssembler.KAFKA_METADATA_FIELD));

        assertEquals("primary-key", assembledRecord.get(RecordAssembler.KAFKA_KEY_FIELD));
        GenericRecord metadataRecord = (GenericRecord) assembledRecord.get(RecordAssembler.KAFKA_METADATA_FIELD);
        assertNotNull(metadataRecord.getSchema().getField(RecordAssembler.METADATA_PARTITION_FIELD));
    }
}
