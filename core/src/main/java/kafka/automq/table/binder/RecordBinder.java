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
package kafka.automq.table.binder;


import kafka.automq.table.metric.FieldMetric;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A factory that creates lazy-evaluation Record views of Avro GenericRecords.
 * Field values are converted only when accessed, avoiding upfront conversion overhead.
 */
public class RecordBinder {

    private final org.apache.iceberg.Schema icebergSchema;
    private final TypeAdapter<Schema> typeAdapter;
    private final Map<String, Integer> fieldNameToPosition;
    private final FieldMapping[] fieldMappings;

    // Pre-computed RecordBinders for nested STRUCT fields
    private final Map<String, RecordBinder> nestedStructBinders;

    // Field count statistics for this batch
    private final AtomicLong batchFieldCount;


    public RecordBinder(GenericRecord avroRecord) {
        this(AvroSchemaUtil.toIceberg(avroRecord.getSchema()), avroRecord.getSchema());
    }

    public RecordBinder(org.apache.iceberg.Schema icebergSchema, Schema avroSchema) {
        this(icebergSchema, avroSchema, new AvroValueAdapter());
    }

    public RecordBinder(org.apache.iceberg.Schema icebergSchema, Schema avroSchema, TypeAdapter<Schema> typeAdapter) {
        this(icebergSchema, avroSchema, typeAdapter, new AtomicLong(0));
    }

    public RecordBinder(org.apache.iceberg.Schema icebergSchema, Schema avroSchema, TypeAdapter<Schema> typeAdapter, AtomicLong batchFieldCount) {
        this.icebergSchema = icebergSchema;
        this.typeAdapter = typeAdapter;
        this.batchFieldCount = batchFieldCount;

        // Pre-compute field name to position mapping
        this.fieldNameToPosition = new HashMap<>();
        for (int i = 0; i < icebergSchema.columns().size(); i++) {
            fieldNameToPosition.put(icebergSchema.columns().get(i).name(), i);
        }

        // Initialize field mappings
        this.fieldMappings = new FieldMapping[icebergSchema.columns().size()];
        initializeFieldMappings(avroSchema);

        // Pre-compute nested struct binders
        this.nestedStructBinders = precomputeNestedStructBinders(typeAdapter);
    }

    public RecordBinder createBinderForNewSchema(org.apache.iceberg.Schema icebergSchema, Schema avroSchema) {
        return new RecordBinder(icebergSchema, avroSchema, typeAdapter, batchFieldCount);
    }


    public org.apache.iceberg.Schema getIcebergSchema() {
        return icebergSchema;
    }

    /**
     * Creates a new immutable Record view of the given Avro record.
     * Each call returns a separate instance with its own data reference.
     */
    public Record bind(GenericRecord avroRecord) {
        if (avroRecord == null) {
            return null;
        }
        return new AvroRecordView(avroRecord, icebergSchema, typeAdapter,
            fieldNameToPosition, fieldMappings, nestedStructBinders, this);
    }

    /**
     * Gets the accumulated field count for this batch and resets it to zero.
     * Should be called after each flush to collect field statistics.
     */
    public long getAndResetFieldCount() {
        return batchFieldCount.getAndSet(0);
    }

    /**
     * Adds field count to the batch total. Called by AvroRecordView instances.
     */
    void addFieldCount(long count) {
        batchFieldCount.addAndGet(count);
    }

    private void initializeFieldMappings(Schema avroSchema) {
        Schema recordSchema = avroSchema;

        if (recordSchema.getType() == Schema.Type.UNION) {
            recordSchema = recordSchema.getTypes().stream()
                .filter(s -> s.getType() == Schema.Type.RECORD)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("UNION schema does not contain a RECORD type: " + avroSchema));
        }

        for (int icebergPos = 0; icebergPos < icebergSchema.columns().size(); icebergPos++) {
            Types.NestedField icebergField = icebergSchema.columns().get(icebergPos);
            String fieldName = icebergField.name();

            Schema.Field avroField = recordSchema.getField(fieldName);
            if (avroField != null) {
                fieldMappings[icebergPos] = createOptimizedMapping(
                    avroField.name(),
                    avroField.pos(),
                    icebergField.type(),
                    avroField.schema()
                );
            } else {
                fieldMappings[icebergPos] = null;
            }
        }
    }

    private FieldMapping createOptimizedMapping(String avroFieldName, int avroPosition, Type icebergType, Schema avroType) {
        org.apache.iceberg.Schema nestedSchema = null;
        String nestedSchemaId = null;
        if (icebergType.isStructType()) {
            nestedSchema = icebergType.asStructType().asSchema();
            nestedSchemaId = icebergType.toString();
        }
        return new FieldMapping(avroPosition, avroFieldName, icebergType, icebergType.typeId(), avroType, nestedSchema, nestedSchemaId);
    }


    /**
     * Pre-computes RecordBinders for nested STRUCT fields.
     */
    private Map<String, RecordBinder> precomputeNestedStructBinders(TypeAdapter<Schema> typeAdapter) {
        Map<String, RecordBinder> binders = new HashMap<>();

        for (FieldMapping mapping : fieldMappings) {
            if (mapping != null && mapping.typeId() == Type.TypeID.STRUCT) {
                String structId = mapping.nestedSchemaId();
                if (!binders.containsKey(structId)) {
                    RecordBinder nestedBinder = new RecordBinder(
                        mapping.nestedSchema(),
                        mapping.avroSchema(),
                        typeAdapter,
                        batchFieldCount
                    );
                    binders.put(structId, nestedBinder);
                }
            }
        }

        return binders;
    }

    private static class AvroRecordView implements Record {
        private final GenericRecord avroRecord;
        private final org.apache.iceberg.Schema icebergSchema;
        private final TypeAdapter<Schema> typeAdapter;
        private final Map<String, Integer> fieldNameToPosition;
        private final FieldMapping[] fieldMappings;
        private final Map<String, RecordBinder> nestedStructBinders;
        private final RecordBinder parentBinder;

        AvroRecordView(GenericRecord avroRecord,
                    org.apache.iceberg.Schema icebergSchema,
                    TypeAdapter<Schema> typeAdapter,
                    Map<String, Integer> fieldNameToPosition,
                    FieldMapping[] fieldMappings,
                    Map<String, RecordBinder> nestedStructBinders,
                    RecordBinder parentBinder) {
            this.avroRecord = avroRecord;
            this.icebergSchema = icebergSchema;
            this.typeAdapter = typeAdapter;
            this.fieldNameToPosition = fieldNameToPosition;
            this.fieldMappings = fieldMappings;
            this.nestedStructBinders = nestedStructBinders;
            this.parentBinder = parentBinder;
        }

        @Override
        public Object get(int pos) {
            if (avroRecord == null) {
                throw new IllegalStateException("Avro record is null");
            }
            if (pos < 0 || pos >= fieldMappings.length) {
                throw new IndexOutOfBoundsException("Field position " + pos + " out of bounds");
            }

            FieldMapping mapping = fieldMappings[pos];
            if (mapping == null || !avroRecord.hasField(mapping.avroKey())) {
                return null;
            }

            Object avroValue = avroRecord.get(mapping.avroPosition());
            if (avroValue == null) {
                return null;
            }

            // Handle STRUCT type - delegate to nested binder
            if (mapping.typeId() == Type.TypeID.STRUCT) {
                String structId = mapping.nestedSchemaId();
                RecordBinder nestedBinder = nestedStructBinders.get(structId);
                if (nestedBinder == null) {
                    throw new IllegalStateException("Nested binder not found for struct: " + structId);
                }
                parentBinder.addFieldCount(1);
                return nestedBinder.bind((GenericRecord) avroValue);
            }

            // Convert non-STRUCT types
            Object result = typeAdapter.convert(avroValue, mapping.avroSchema(), mapping.icebergType());

            // Calculate and accumulate field count
            long fieldCount = calculateFieldCount(result, mapping.icebergType());
            parentBinder.addFieldCount(fieldCount);

            return result;
        }

        /**
         * Calculates the field count for a converted value based on its size.
         * Large fields are counted multiple times based on the size threshold.
         */
        private long calculateFieldCount(Object value, Type icebergType) {
            if (value == null) {
                return 0;
            }

            switch (icebergType.typeId()) {
                case STRING:
                    return FieldMetric.count((String) value);
                case BINARY:
                    return FieldMetric.count((ByteBuffer) value);
                case FIXED:
                    return FieldMetric.count((byte[]) value);
                case LIST:
                    return calculateListFieldCount(value, ((Types.ListType) icebergType).elementType());
                case MAP:
                    return calculateMapFieldCount(value, (Types.MapType) icebergType);
                default:
                    return 1; // Struct or Primitive types count as 1 field
            }
        }

        /**
         * Calculates field count for List values by summing element costs.
         */
        private long calculateListFieldCount(Object list, Type elementType) {
            if (list == null) {
                return 0;
            }
            long total = 1;
            if (list instanceof List) {
                for (Object element : (List) list) {
                    total += calculateFieldCount(element, elementType);
                }
            }
            return total;
        }

        /**
         * Calculates field count for Map values by summing key and value costs.
         */
        private long calculateMapFieldCount(Object map, Types.MapType mapType) {
            if (map == null) {
                return 0;
            }

            long total = 1;
            if (map instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
                    total += FieldMetric.count(entry.getKey().toString());
                    total += calculateFieldCount(entry.getValue(), mapType.valueType());
                }
            }
            return total;
        }

        @Override
        public Object getField(String name) {
            Integer position = fieldNameToPosition.get(name);
            return position != null ? get(position) : null;
        }

        @Override
        public Types.StructType struct() {
            return icebergSchema.asStruct();
        }

        @Override
        public int size() {
            return icebergSchema.columns().size();
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(get(pos));
        }

        // Unsupported operations
        @Override
        public void setField(String name, Object value) {
            throw new UnsupportedOperationException("Read-only");
        }
        @Override
        public Record copy() {
            throw new UnsupportedOperationException("Read-only");
        }
        @Override
        public Record copy(Map<String, Object> overwriteValues) {
            throw new UnsupportedOperationException("Read-only");
        }
        @Override
        public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException("Read-only");
        }
    }

    // Field mapping structure
    private static class FieldMapping {
        private final int avroPosition;
        private final String avroKey;
        private final Type icebergType;
        private final Type.TypeID typeId;
        private final Schema avroSchema;
        private final org.apache.iceberg.Schema nestedSchema;
        private final String nestedSchemaId;

        FieldMapping(int avroPosition, String avroKey, Type icebergType, Type.TypeID typeId, Schema avroSchema, org.apache.iceberg.Schema nestedSchema, String nestedSchemaId) {
            this.avroPosition = avroPosition;
            this.avroKey = avroKey;
            this.icebergType = icebergType;
            this.typeId = typeId;
            this.avroSchema = avroSchema;
            this.nestedSchema = nestedSchema;
            this.nestedSchemaId = nestedSchemaId;
        }

        public int avroPosition() {
            return avroPosition;
        }

        public String avroKey() {
            return avroKey;
        }

        public Type icebergType() {
            return icebergType;
        }

        public Type.TypeID typeId() {
            return typeId;
        }

        public Schema avroSchema() {
            return avroSchema;
        }

        public org.apache.iceberg.Schema nestedSchema() {
            return nestedSchema;
        }

        public String nestedSchemaId() {
            return nestedSchemaId;
        }
    }
}
