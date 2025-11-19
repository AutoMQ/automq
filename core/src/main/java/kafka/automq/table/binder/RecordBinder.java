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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.NULL;

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
    private final Map<Schema, RecordBinder> nestedStructBinders;

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
        this.fieldMappings = buildFieldMappings(avroSchema, icebergSchema);
        // Pre-compute nested struct binders
        this.nestedStructBinders = precomputeBindersMap(typeAdapter);
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

    private FieldMapping[] buildFieldMappings(Schema avroSchema, org.apache.iceberg.Schema icebergSchema) {
        Schema recordSchema = avroSchema;
        FieldMapping[] mappings = new FieldMapping[icebergSchema.columns().size()];

        // Unwrap UNION if it contains only one non-NULL type
        recordSchema = resolveUnionElement(recordSchema);

        for (int icebergPos = 0; icebergPos < icebergSchema.columns().size(); icebergPos++) {
            Types.NestedField icebergField = icebergSchema.columns().get(icebergPos);
            String fieldName = icebergField.name();

            Schema.Field avroField = recordSchema.getField(fieldName);
            if (avroField != null) {
                mappings[icebergPos] = buildFieldMapping(
                    avroField.name(),
                    avroField.pos(),
                    icebergField.type(),
                    avroField.schema()
                );
            } else {
                mappings[icebergPos] = null;
            }
        }
        return mappings;
    }

    private FieldMapping buildFieldMapping(String avroFieldName, int avroPosition, Type icebergType, Schema avroType) {
        if (Type.TypeID.TIMESTAMP.equals(icebergType.typeId())
            || Type.TypeID.TIME.equals(icebergType.typeId())
            || Type.TypeID.MAP.equals(icebergType.typeId())
            || Type.TypeID.LIST.equals(icebergType.typeId())
            || Type.TypeID.STRUCT.equals(icebergType.typeId())) {
            avroType = resolveUnionElement(avroType);
        }
        return new FieldMapping(avroPosition, avroFieldName, icebergType, avroType);
    }

    private Schema resolveUnionElement(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }

        // Collect all non-NULL types
        List<Schema> nonNullTypes = new ArrayList<>();
        for (Schema s : schema.getTypes()) {
            if (s.getType() != NULL) {
                nonNullTypes.add(s);
            }
        }

        if (nonNullTypes.isEmpty()) {
            throw new IllegalArgumentException("UNION schema contains only NULL type: " + schema);
        } else if (nonNullTypes.size() == 1) {
            // Only unwrap UNION if it contains exactly one non-NULL type (optional union)
            return nonNullTypes.get(0);
        } else {
            // Multiple non-NULL types: non-optional union not supported
            throw new UnsupportedOperationException(
                "Non-optional UNION with multiple non-NULL types is not supported. " +
                "Found " + nonNullTypes.size() + " non-NULL types in UNION: " + schema);
        }
    }


    /**
     * Pre-computes RecordBinders for nested STRUCT fields.
     */
    private Map<Schema, RecordBinder> precomputeBindersMap(TypeAdapter<Schema> typeAdapter) {
        Map<Schema, RecordBinder> binders = new IdentityHashMap<>();

        for (FieldMapping mapping : fieldMappings) {
            if (mapping != null) {
                precomputeBindersForType(mapping.icebergType(), mapping.avroSchema(), binders, typeAdapter);
            }
        }
        return binders;
    }

    /**
     * Recursively precomputes binders for a given Iceberg type and its corresponding Avro schema.
     */
    private void precomputeBindersForType(Type icebergType, Schema avroSchema,
                                          Map<Schema, RecordBinder> binders,
                                          TypeAdapter<Schema> typeAdapter) {
        if (icebergType.isPrimitiveType()) {
            return; // No binders needed for primitive types
        }

        if (icebergType.isStructType() && !avroSchema.isUnion()) {
            createStructBinder(icebergType.asStructType(), avroSchema, binders, typeAdapter);
        } else if (icebergType.isStructType() && avroSchema.isUnion()) {
            createUnionStructBinders(icebergType.asStructType(), avroSchema, binders, typeAdapter);
        } else if (icebergType.isListType()) {
            createListBinder(icebergType.asListType(), avroSchema, binders, typeAdapter);
        } else if (icebergType.isMapType()) {
            createMapBinder(icebergType.asMapType(), avroSchema, binders, typeAdapter);
        }
    }

    /**
     * Creates binders for STRUCT types represented as Avro UNIONs.
     */
    private void createUnionStructBinders(Types.StructType structType, Schema avroSchema,
                                          Map<Schema, RecordBinder> binders,
                                          TypeAdapter<Schema> typeAdapter) {
        org.apache.iceberg.Schema schema = structType.asSchema();
        SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record(avroSchema.getName()).fields()
            .name("tag").type().intType().noDefault();
        int tag = 0;
        for (Schema unionMember : avroSchema.getTypes()) {
            if (unionMember.getType() != NULL) {
                schemaBuilder.name("field" + tag).type(unionMember).noDefault();
                tag++;
            }
        }
        RecordBinder structBinder = new RecordBinder(schema, schemaBuilder.endRecord(), typeAdapter, batchFieldCount);
        binders.put(avroSchema, structBinder);
    }

    /**
     * Creates a binder for a STRUCT type field.
     */
    private void createStructBinder(Types.StructType structType, Schema avroSchema,
                                    Map<Schema, RecordBinder> binders,
                                    TypeAdapter<Schema> typeAdapter) {
        org.apache.iceberg.Schema schema = structType.asSchema();
        RecordBinder structBinder = new RecordBinder(schema, avroSchema, typeAdapter, batchFieldCount);
        binders.put(avroSchema, structBinder);
    }

    /**
     * Creates binders for LIST type elements (if they are STRUCT types).
     */
    private void createListBinder(Types.ListType listType, Schema avroSchema,
                                  Map<Schema, RecordBinder> binders,
                                  TypeAdapter<Schema> typeAdapter) {
        Type elementType = listType.elementType();
        if (elementType.isStructType()) {
            Schema elementAvroSchema = avroSchema.getElementType();
            createStructBinder(elementType.asStructType(), elementAvroSchema, binders, typeAdapter);
        }
    }

    /**
     * Creates binders for MAP type keys and values (if they are STRUCT types).
     * Handles two Avro representations: ARRAY of key-value records, or native MAP.
     */
    private void createMapBinder(Types.MapType mapType, Schema avroSchema,
                                 Map<Schema, RecordBinder> binders,
                                 TypeAdapter<Schema> typeAdapter) {
        Type keyType = mapType.keyType();
        Type valueType = mapType.valueType();

        if (ARRAY.equals(avroSchema.getType())) {
            // Avro represents MAP as ARRAY of records with "key" and "value" fields
            createMapAsArrayBinder(keyType, valueType, avroSchema, binders, typeAdapter);
        } else {
            // Avro represents MAP as native MAP type
            createMapAsMapBinder(keyType, valueType, avroSchema, binders, typeAdapter);
        }
    }

    /**
     * Handles MAP represented as Avro ARRAY of {key, value} records.
     */
    private void createMapAsArrayBinder(Type keyType, Type valueType, Schema avroSchema,
                                        Map<Schema, RecordBinder> binders,
                                        TypeAdapter<Schema> typeAdapter) {
        Schema elementSchema = avroSchema.getElementType();

        // Process key if it's a STRUCT
        if (keyType.isStructType()) {
            Schema keyAvroSchema = elementSchema.getField("key").schema();
            createStructBinder(keyType.asStructType(), keyAvroSchema, binders, typeAdapter);
        }

        // Process value if it's a STRUCT
        if (valueType.isStructType()) {
            Schema valueAvroSchema = elementSchema.getField("value").schema();
            createStructBinder(valueType.asStructType(), valueAvroSchema, binders, typeAdapter);
        }
    }

    /**
     * Handles MAP represented as Avro native MAP type.
     */
    private void createMapAsMapBinder(Type keyType, Type valueType, Schema avroSchema,
                                      Map<Schema, RecordBinder> binders,
                                      TypeAdapter<Schema> typeAdapter) {
        // Struct keys in native MAP are not supported by Avro
        if (keyType.isStructType()) {
            throw new UnsupportedOperationException("Struct keys in MAP types are not supported");
        }

        // Process value if it's a STRUCT
        if (valueType.isStructType()) {
            Schema valueAvroSchema = avroSchema.getValueType();
            createStructBinder(valueType.asStructType(), valueAvroSchema, binders, typeAdapter);
        }
    }

    private static class AvroRecordView implements Record {
        private final GenericRecord avroRecord;
        private final org.apache.iceberg.Schema icebergSchema;
        private final TypeAdapter<Schema> typeAdapter;
        private final Map<String, Integer> fieldNameToPosition;
        private final FieldMapping[] fieldMappings;
        private final Map<Schema, RecordBinder> nestedStructBinders;
        private final RecordBinder parentBinder;

        AvroRecordView(GenericRecord avroRecord,
                       org.apache.iceberg.Schema icebergSchema,
                       TypeAdapter<Schema> typeAdapter,
                       Map<String, Integer> fieldNameToPosition,
                       FieldMapping[] fieldMappings,
                       Map<Schema, RecordBinder> nestedStructBinders,
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
            if (mapping == null) {
                return null;
            }
            Object avroValue = avroRecord.get(mapping.avroPosition());
            if (avroValue == null) {
                return null;
            }
            Object result = convert(avroValue, mapping.avroSchema(), mapping.icebergType());

            // Calculate and accumulate field count
            long fieldCount = calculateFieldCount(result, mapping.icebergType());
            parentBinder.addFieldCount(fieldCount);

            return result;
        }

        public Object convert(Object sourceValue, Schema sourceSchema, Type targetType) {
            if (targetType.typeId() == Type.TypeID.STRUCT) {
                RecordBinder binder = nestedStructBinders.get(sourceSchema);
                if (binder == null) {
                    throw new IllegalStateException("Missing nested binder for schema: " + sourceSchema);
                }
                return binder.bind((GenericRecord) sourceValue);
            }
            return typeAdapter.convert(sourceValue, (Schema) sourceSchema, targetType, this::convert);
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
                    return FieldMetric.count((CharSequence) value);
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
                Map<?, ?> typedMap = (Map<?, ?>) map;
                if (typedMap.isEmpty()) {
                    return total;
                }
                for (Map.Entry<?, ?> entry : typedMap.entrySet()) {
                    total += calculateFieldCount(entry.getKey(), mapType.keyType());
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
}
