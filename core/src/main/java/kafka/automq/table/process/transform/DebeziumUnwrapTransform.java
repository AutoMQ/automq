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

package kafka.automq.table.process.transform;

import kafka.automq.table.process.Transform;
import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;

/**
 * Transform for unwrapping Debezium CDC formatted records.
 *
 * <p>Extracts business data from Debezium CDC envelope format. Handles create,
 * update, and delete operations, adding unified CDC metadata.</p>
 *
 * <ul>
 *   <li>CREATE/READ: uses 'after' field</li>
 *   <li>UPDATE: uses 'after' field</li>
 *   <li>DELETE: uses 'before' field</li>
 * </ul>
 *
 * <p>Adds metadata: operation type, timestamp, offset, source table.</p>
 */
public class DebeziumUnwrapTransform implements Transform {

    public static final DebeziumUnwrapTransform INSTANCE = new DebeziumUnwrapTransform();

    private static final Logger log = LoggerFactory.getLogger(DebeziumUnwrapTransform.class);

    // Debezium standard field names
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_OP = "op";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_TS_MS = "ts_ms";

    // Debezium operation types
    private static final String OP_CREATE = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";
    private static final String OP_READ = "r";

    //CDC field names
    private static final String CDC_RECORD_NAME = "_cdc";
    private static final String CDC_FIELD_OP = "op";
    private static final String CDC_FIELD_TS = "ts";
    private static final String CDC_FIELD_OFFSET = "offset";
    private static final String CDC_FIELD_SOURCE = "source";

    private static final Schema CDC_SCHEMA = SchemaBuilder.record(CDC_RECORD_NAME)
        .fields()
        .optionalString(CDC_FIELD_OP)
        .optionalLong(CDC_FIELD_TS)
        .optionalLong(CDC_FIELD_OFFSET)
        .optionalString(CDC_FIELD_SOURCE)
        .endRecord();

    // Cache enriched schemas keyed by base schema fingerprint (bounded, concurrent)
    private static final int ENRICHED_SCHEMA_CACHE_MAX = 1024;
    private final Map<SchemaKey, Schema> enrichedSchemaCache = new BoundedConcurrentHashMap<>(ENRICHED_SCHEMA_CACHE_MAX);

    @Override
    public void configure(Map<String, ?> configs) {
        // ignore
    }

    @Override
    public GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException {
        Objects.requireNonNull(record, "Input record cannot be null");

        try {
            // If it's not a Debezium record, throw an exception.
            if (!isDebeziumRecord(record)) {
                throw new TransformException("Record is not in a recognizable Debezium format.");
            }

            // Extract operation type
            String operation = getStringValue(record, FIELD_OP);
            if (operation == null) {
                throw new TransformException("Invalid Debezium record: missing required field '" + FIELD_OP + "'");
            }

            // Extract business data based on operation type
            GenericRecord businessData = extractBusinessData(record, operation);
            if (businessData == null) {
                throw new TransformException("Invalid Debezium record: no extractable data for operation '" + operation + "'");
            }

            // Enrich with metadata
            return enrichWithMetadata(businessData, record, operation, context);

        } catch (TransformException e) {
            throw e;
        } catch (Exception e) {
            throw new TransformException("Failed to process Debezium record", e);
        }
    }

    private boolean isDebeziumRecord(GenericRecord record) {
        if (record == null) {
            return false;
        }
        Schema schema = unwrapSchema(record.getSchema());
        if (schema == null) {
            return false;
        }
        return schema.getField(FIELD_OP) != null &&
               (schema.getField(FIELD_BEFORE) != null || schema.getField(FIELD_AFTER) != null);
    }

    private GenericRecord extractBusinessData(GenericRecord record, String operation) throws TransformException {
        switch (operation) {
            case OP_CREATE:
            case OP_READ:
                // INSERT and READ operations use 'after' field
                return getRecordValue(record, FIELD_AFTER);

            case OP_UPDATE:
                // UPDATE operations must have 'after' field
                GenericRecord after = getRecordValue(record, FIELD_AFTER);
                if (after == null) {
                    throw new TransformException("Invalid UPDATE record: missing required 'after' data");
                }
                return after;

            case OP_DELETE:
                // DELETE operations use 'before' field
                GenericRecord beforeDelete = getRecordValue(record, FIELD_BEFORE);
                if (beforeDelete == null) {
                    throw new TransformException("Invalid DELETE record: missing required 'before' data");
                }
                return beforeDelete;

            default:
                log.warn("Unknown Debezium operation type: {}. Attempting to use 'after' data", operation);
                GenericRecord fallback = getRecordValue(record, FIELD_AFTER);
                if (fallback == null) {
                    throw new TransformException("Unsupported operation '" + operation + "' with no usable data");
                }
                return fallback;
        }
    }


    private GenericRecord enrichWithMetadata(GenericRecord businessData,
                                           GenericRecord debeziumRecord,
                                           String operation,
                                           TransformContext context) throws TransformException {
        try {
            Schema schemaWithMetadata = createSchemaWithMetadata(businessData.getSchema());

            // Build the enriched record using GenericData.Record to reduce allocations
            GenericData.Record result = new GenericData.Record(schemaWithMetadata);
            for (Schema.Field field : businessData.getSchema().getFields()) {
                result.put(field.name(), businessData.get(field.name()));
            }

            GenericData.Record cdc = new GenericData.Record(CDC_SCHEMA);
            cdc.put(CDC_FIELD_OP, mapOperation(operation));

            Object tsMs = debeziumRecord.get(FIELD_TS_MS);
            if (tsMs instanceof Long) {
                cdc.put(CDC_FIELD_TS, tsMs);
            }

            cdc.put(CDC_FIELD_OFFSET, context.getKafkaRecord().offset());

            GenericRecord source = getRecordValue(debeziumRecord, FIELD_SOURCE);
            if (source != null) {
                String schemaName = null;
                if (source.hasField("schema")) {
                    schemaName = getStringValue(source, "schema");
                }
                String db = (schemaName == null) ? getStringValue(source, "db") : schemaName;
                String table = getStringValue(source, "table");
                if (db != null && table != null) {
                    cdc.put(CDC_FIELD_SOURCE, db + "." + table);
                }
            }

            result.put(CDC_RECORD_NAME, cdc);
            return result;

        } catch (Exception e) {
            throw new TransformException("Failed to enrich record with Debezium metadata:" + e.getMessage(), e);
        }
    }

    private String mapOperation(String originalOp) {
        switch (originalOp) {
            case "u":
                return "U";
            case "d":
                return "D";
            default:
                // Debezium ops "c", "r", and any others
                return "I";
        }
    }

    private Schema createSchemaWithMetadata(Schema originalSchema) {
        SchemaKey schemaKey = new SchemaKey(originalSchema);
        return enrichedSchemaCache.computeIfAbsent(schemaKey, k -> {
            List<Schema.Field> enhancedFields = new ArrayList<>();
            for (Schema.Field field : originalSchema.getFields()) {
                enhancedFields.add(new Schema.Field(field, field.schema()));
            }
            enhancedFields.add(new Schema.Field(CDC_RECORD_NAME, CDC_SCHEMA, "CDC metadata", null));

            String enhancedName = originalSchema.getName() != null ?
                originalSchema.getName() + "_cdc_enriched" : "enriched_record";

            return Schema.createRecord(
                enhancedName,
                "Record enriched with CDC metadata",
                originalSchema.getNamespace(),
                false,
                enhancedFields
            );
        });
    }


    private GenericRecord getRecordValue(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        return (value instanceof GenericRecord) ? (GenericRecord) value : null;
    }
    private String getStringValue(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        return (value != null) ? value.toString() : null;
    }

    private Schema unwrapSchema(Schema schema) {
        if (schema == null) {
            return null;
        }
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                .filter(s -> s.getType() == Schema.Type.RECORD)
                .findFirst()
                .orElse(null);
        }
        return schema.getType() == Schema.Type.RECORD ? schema : null;
    }

    @Override
    public String getName() {
        return "DebeziumUnwrap";
    }

    private static final class SchemaKey {
        private final Schema schema;
        private final int hashCode;
        private volatile long fingerprint;
        private volatile boolean fingerprintComputed;

        private SchemaKey(Schema schema) {
            this.schema = Objects.requireNonNull(schema, "schema");
            this.hashCode = schema.hashCode();
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SchemaKey)) {
                return false;
            }
            SchemaKey other = (SchemaKey) obj;
            if (hashCode != other.hashCode) {
                return false;
            }
            if (schema == other.schema) {
                return true;
            }
            return fingerprint() == other.fingerprint();
        }

        private long fingerprint() {
            if (!fingerprintComputed) {
                fingerprint = org.apache.avro.SchemaNormalization.parsingFingerprint64(schema);
                fingerprintComputed = true;
            }
            return fingerprint;
        }
    }
}
