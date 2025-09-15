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

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * A specialized assembler for constructing the final record structure
 * in a clean, fluent manner following the builder pattern.
 * <p>
 * This class is designed to be reused at the Processor level to avoid
 * unnecessary object allocation on the hot path.
 * <p>
 * This class also serves as the holder for the public contract of field names.
 */
public final class RecordAssembler {

    public static final String KAFKA_HEADER_FIELD = "_kafka_header";
    public static final String KAFKA_KEY_FIELD = "_kafka_key";
    public static final String KAFKA_VALUE_FIELD = "_kafka_value";
    public static final String KAFKA_METADATA_FIELD = "_kafka_metadata";
    public static final String METADATA_PARTITION_FIELD = "partition";
    public static final String METADATA_OFFSET_FIELD = "offset";
    public static final String METADATA_TIMESTAMP_FIELD = "timestamp";

    private static final Schema METADATA_SCHEMA = SchemaBuilder
            .record("KafkaMetadata")
            .namespace("kafka.automq.table.process")
            .doc("Holds metadata about the original Kafka record.")
            .fields()
                .name(METADATA_PARTITION_FIELD).doc("Partition id").type().intType().noDefault()
                .name(METADATA_OFFSET_FIELD).doc("Record offset").type().longType().noDefault()
                .name(METADATA_TIMESTAMP_FIELD).doc("Record timestamp").type().longType().noDefault()
            .endRecord();

    private final Map<String, Schema> schemaMap = new HashMap<>();

    // Reusable state - reset for each record
    private GenericRecord baseRecord;
    private ConversionResult headerResult;
    private ConversionResult keyResult;
    private int partition;
    private long offset;
    private long timestamp;
    private String schemaIdentity;

    public RecordAssembler() {
    }

    public RecordAssembler reset(GenericRecord baseRecord) {
        this.baseRecord = baseRecord;
        this.headerResult = null;
        this.keyResult = null;
        this.partition = 0;
        this.offset = 0L;
        this.timestamp = 0L;
        this.schemaIdentity = null;
        return this;
    }

    public RecordAssembler withHeader(ConversionResult headerResult) {
        this.headerResult = headerResult;
        return this;
    }

    public RecordAssembler withKey(ConversionResult keyResult) {
        this.keyResult = keyResult;
        return this;
    }


    public RecordAssembler withSchemaIdentity(String schemaIdentity) {
        this.schemaIdentity = schemaIdentity;
        return this;
    }

    public RecordAssembler withMetadata(int partition, long offset, long timestamp) {
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        return this;
    }

    public GenericRecord assemble() {
        Schema finalSchema = getOrCreateSchema();
        GenericRecord finalRecord = new GenericData.Record(finalSchema);
        populateFields(finalRecord);
        return finalRecord;
    }

    private Schema getOrCreateSchema() {
        if (schemaIdentity == null) {
            long baseFp = SchemaNormalization.parsingFingerprint64(baseRecord.getSchema());
            long keyFp = keyResult != null ? SchemaNormalization.parsingFingerprint64(keyResult.getSchema()) : 0L;
            long headerFp = headerResult != null ? SchemaNormalization.parsingFingerprint64(headerResult.getSchema()) : 0L;
            long metadataFp = SchemaNormalization.parsingFingerprint64(METADATA_SCHEMA);

            schemaIdentity = "v:" + Long.toUnsignedString(baseFp) +
                           "|k:" + Long.toUnsignedString(keyFp) +
                           "|h:" + Long.toUnsignedString(headerFp) +
                           "|m:" + Long.toUnsignedString(metadataFp);
        }
        final String cacheKey = schemaIdentity;
        return schemaMap.computeIfAbsent(cacheKey, k -> buildFinalSchema());
    }

    private Schema buildFinalSchema() {
        List<Schema.Field> finalFields = new ArrayList<>(baseRecord.getSchema().getFields().size() + 3);
        Schema baseSchema = baseRecord.getSchema();
        for (Schema.Field field : baseSchema.getFields()) {
            finalFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
        }

        if (headerResult != null) {
            finalFields.add(new Schema.Field(KAFKA_HEADER_FIELD, headerResult.getSchema(), "Kafka record headers", null));
        }
        if (keyResult != null) {
            finalFields.add(new Schema.Field(KAFKA_KEY_FIELD, keyResult.getSchema(), "Kafka record key", null));
        }
        finalFields.add(new Schema.Field(KAFKA_METADATA_FIELD, METADATA_SCHEMA, "Kafka record metadata", null));

        return Schema.createRecord(baseSchema.getName() + "WithMetadata", null,
            "kafka.automq.table.process", false, finalFields);
    }

    private void populateFields(GenericRecord finalRecord) {
        Schema baseSchema = baseRecord.getSchema();
        for (Schema.Field field : baseSchema.getFields()) {
            finalRecord.put(field.name(), baseRecord.get(field.name()));
        }

        if (headerResult != null) {
            finalRecord.put(KAFKA_HEADER_FIELD, headerResult.getValue());
        }
        if (keyResult != null) {
            finalRecord.put(KAFKA_KEY_FIELD, keyResult.getValue());
        }

        GenericRecord metadata = new GenericData.Record(METADATA_SCHEMA);
        metadata.put(METADATA_PARTITION_FIELD, partition);
        metadata.put(METADATA_OFFSET_FIELD, offset);
        metadata.put(METADATA_TIMESTAMP_FIELD, timestamp);
        finalRecord.put(KAFKA_METADATA_FIELD, metadata);
    }
}
