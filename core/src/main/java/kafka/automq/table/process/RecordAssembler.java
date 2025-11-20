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

import org.apache.kafka.common.cache.LRUCache;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.internal.Accessor;

import java.util.ArrayList;
import java.util.List;

/**
 * A specialized assembler for constructing the final record structure
 * in a clean, fluent manner following the builder pattern.
 * <p>
 * This class also serves as the holder for the public contract of field names.
 */
public final class RecordAssembler {
    private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

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

    private static final int SCHEMA_CACHE_MAX = 32;
    // Cache of assembled schema + precomputed indexes bound to a schema identity
    private final LRUCache<String, AssemblerSchema> assemblerSchemaCache = new LRUCache<>(SCHEMA_CACHE_MAX);

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
        AssemblerSchema aSchema = getOrCreateAssemblerSchema();
        // Return a lightweight view that implements GenericRecord
        // and adapts schema position/name lookups to the underlying values
        // without copying the base record data.
        return new AssembledRecordView(aSchema, baseRecord,
            headerResult != null ? headerResult.getValue() : null,
            keyResult != null ? keyResult.getValue() : null,
            partition, offset, timestamp);
    }

    private AssemblerSchema getOrCreateAssemblerSchema() {
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
        AssemblerSchema cached = assemblerSchemaCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        AssemblerSchema created = buildFinalAssemblerSchema();
        assemblerSchemaCache.put(cacheKey, created);
        return created;
    }

    private AssemblerSchema buildFinalAssemblerSchema() {
        List<Schema.Field> finalFields = new ArrayList<>(baseRecord.getSchema().getFields().size() + 3);
        Schema baseSchema = baseRecord.getSchema();
        for (Schema.Field field : baseSchema.getFields()) {
            // Accessor keeps the original Schema instance (preserving logical types) while skipping default-value revalidation.
            Schema.Field f = Accessor.createField(field.name(), field.schema(), field.doc(), Accessor.defaultValue(field), false, field.order());
            finalFields.add(f);
        }

        int baseFieldCount = baseSchema.getFields().size();
        int headerIndex = -1;
        int keyIndex = -1;
        int metadataIndex = -1;

        if (headerResult != null) {
            Schema optionalHeaderSchema = ensureOptional(headerResult.getSchema());
            finalFields.add(new Schema.Field(KAFKA_HEADER_FIELD, optionalHeaderSchema, "Kafka record headers", JsonProperties.NULL_VALUE));
            headerIndex = baseFieldCount;
        }
        if (keyResult != null) {
            Schema optionalKeySchema = ensureOptional(keyResult.getSchema());
            finalFields.add(new Schema.Field(KAFKA_KEY_FIELD, optionalKeySchema, "Kafka record key", JsonProperties.NULL_VALUE));
            keyIndex = (headerIndex >= 0) ? baseFieldCount + 1 : baseFieldCount;
        }

        Schema optionalMetadataSchema = ensureOptional(METADATA_SCHEMA);
        finalFields.add(new Schema.Field(KAFKA_METADATA_FIELD, optionalMetadataSchema, "Kafka record metadata", JsonProperties.NULL_VALUE));
        metadataIndex = baseFieldCount + (headerIndex >= 0 ? 1 : 0) + (keyIndex >= 0 ? 1 : 0);

        Schema finalSchema = Schema.createRecord(baseSchema.getName() + "WithMetadata", null,
            "kafka.automq.table.process", false, finalFields);

        return new AssemblerSchema(finalSchema, baseFieldCount, headerIndex, keyIndex, metadataIndex);
    }

    public static Schema ensureOptional(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            boolean hasNull = false;
            List<Schema> types = schema.getTypes();
            for (Schema type : types) {
                if (type.getType() == Schema.Type.NULL) {
                    hasNull = true;
                    break;
                }
            }
            if (hasNull) {
                return schema;
            }
            List<Schema> withNull = new ArrayList<>(types.size() + 1);
            withNull.add(NULL_SCHEMA);
            withNull.addAll(types);
            return Schema.createUnion(withNull);
        }
        return Schema.createUnion(List.of(NULL_SCHEMA, schema));
    }

    /**
     * A read-only GenericRecord view that adapts accesses (by name or position)
     * to the underlying base record and the synthetic kafka fields.
     */
    private static final class AssembledRecordView implements GenericRecord {
        private final Schema finalSchema;
        private final int finalFieldCount;
        private final GenericRecord baseRecord;
        private final Object headerValue;   // May be null if not present in schema
        private final Object keyValue;      // May be null if not present in schema
        private final int baseFieldCount;
        private final int headerIndex;   // -1 if absent
        private final int keyIndex;      // -1 if absent
        private final int metadataIndex; // always >= 0

        private GenericRecord metadataRecord;

        AssembledRecordView(AssemblerSchema aSchema,
                            GenericRecord baseRecord,
                            Object headerValue,
                            Object keyValue,
                            int partition,
                            long offset,
                            long timestamp) {
            this.finalSchema = aSchema.schema;
            this.finalFieldCount = finalSchema.getFields().size();
            this.baseRecord = baseRecord;
            this.headerValue = headerValue;
            this.keyValue = keyValue;

            this.baseFieldCount = aSchema.baseFieldCount;
            this.headerIndex = aSchema.headerIndex;
            this.keyIndex = aSchema.keyIndex;
            this.metadataIndex = aSchema.metadataIndex;

            this.metadataRecord = new GenericData.Record(METADATA_SCHEMA);
            metadataRecord.put(METADATA_PARTITION_FIELD, partition);
            metadataRecord.put(METADATA_OFFSET_FIELD, offset);
            metadataRecord.put(METADATA_TIMESTAMP_FIELD, timestamp);
        }

        @Override
        public void put(String key, Object v) {
            throw new UnsupportedOperationException("AssembledRecordView is read-only");
        }

        @Override
        public Object get(String key) {
            Schema.Field field = finalSchema.getField(key);
            if (field == null) {
                return null;
            }
            return get(field.pos());
        }

        @Override
        public Schema getSchema() {
            return finalSchema;
        }

        @Override
        public void put(int i, Object v) {
            throw new UnsupportedOperationException("AssembledRecordView is read-only");
        }

        @Override
        public Object get(int i) {
            if (i < 0 || i >= finalFieldCount) {
                throw new IndexOutOfBoundsException("Field position out of bounds: " + i);
            }
            // Base fields delegate directly
            if (i < baseFieldCount) {
                return baseRecord.get(i);
            }
            // Synthetic fields
            if (i == headerIndex) {
                return headerValue;
            }
            if (i == keyIndex) {
                return keyValue;
            }
            if (i == metadataIndex) {
                return metadataRecord;
            }
            // Should not happen if schema is consistent
            return null;
        }
    }

    private static final class AssemblerSchema {
        final Schema schema;
        final int baseFieldCount;
        final int headerIndex;
        final int keyIndex;
        final int metadataIndex;

        AssemblerSchema(Schema schema, int baseFieldCount, int headerIndex, int keyIndex, int metadataIndex) {
            this.schema = schema;
            this.baseFieldCount = baseFieldCount;
            this.headerIndex = headerIndex;
            this.keyIndex = keyIndex;
            this.metadataIndex = metadataIndex;
        }
    }
}
