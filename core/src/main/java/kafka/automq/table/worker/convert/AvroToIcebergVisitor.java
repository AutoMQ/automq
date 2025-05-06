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

package kafka.automq.table.worker.convert;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroToIcebergVisitor extends AbstractIcebergRecordVisitor<org.apache.avro.Schema, GenericRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroToIcebergVisitor.class);
    private static final org.apache.avro.Schema STRING_SCHEMA_INSTANCE = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);

    public AvroToIcebergVisitor(Schema icebergSchema) {
        super(icebergSchema);
    }

    @Override
    protected Map<String, FieldValue<org.apache.avro.Schema>> getFieldValues(GenericRecord genericRecord) {
        Map<String, FieldValue<org.apache.avro.Schema>> fieldValues = new HashMap<>();
        genericRecord.getSchema().getFields().forEach(field -> {
            Object value = genericRecord.get(field.name());
            FieldValue<org.apache.avro.Schema> fieldValue = new FieldValue<>(field.schema(), value);
            fieldValues.put(field.name(), fieldValue);
        });
        return fieldValues;
    }

    @Override
    protected FieldValue<org.apache.avro.Schema> getFieldValue(GenericRecord genericRecord, String fieldName) {
        org.apache.avro.Schema fieldSchema = genericRecord.getSchema().getField(fieldName).schema();
        Object value = genericRecord.get(fieldName);
        return new FieldValue<>(fieldSchema, value);
    }

    @Override
    protected String convertString0(FieldValue<org.apache.avro.Schema> fieldValue) {
        Object value = fieldValue.value();
        if (value instanceof Utf8) {
            return value.toString();
        } else if (value instanceof GenericData.EnumSymbol) {
            return value.toString();
        } else {
            return super.convertString0(fieldValue);
        }
    }

    @Override
    protected Object convertUUID0(FieldValue<org.apache.avro.Schema> fieldValue) {
        Object value = fieldValue.value();
        if (value instanceof Utf8) {
            return super.convertUUID0(new FieldValue<>(fieldValue.type(), value.toString()));
        } else {
            return super.convertUUID0(fieldValue);
        }
    }

    @Override
    protected LocalTime convertTime0(FieldValue<org.apache.avro.Schema> fieldValue) {
        Object value = fieldValue.value();
        org.apache.avro.Schema type = fieldValue.type();
        if (type.getLogicalType() != null) {
            LogicalType logicalType = type.getLogicalType();
            if (logicalType instanceof LogicalTypes.TimeMicros) {
                // A time-micros logical type annotates an Avro long,
                // where the long stores the number of microseconds after midnight, 00:00:00.000000.
                long timeMicros = ((Number) value).longValue();
                return DateTimeUtil.timeFromMicros(timeMicros);
            } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                // A time-millis logical type annotates an Avro int,
                // where the int stores the number of milliseconds after midnight, 00:00:00.000.
                long timeMillis = ((Number) value).longValue();
                return DateTimeUtil.timeFromMicros(timeMillis * 1000);
            }
        }
        return super.convertTime0(fieldValue);
    }

    @Override
    protected Temporal convertTimestamp0(FieldValue<org.apache.avro.Schema> fieldValue, Types.TimestampType type) {
        Object value = fieldValue.value();
        org.apache.avro.Schema fieldType = fieldValue.type();
        org.apache.avro.LogicalType logicalType = fieldType.getLogicalType();

        if (logicalType != null && value instanceof Number) {
            if ((logicalType instanceof LogicalTypes.TimestampMillis && type.shouldAdjustToUTC())
                || logicalType instanceof LogicalTypes.LocalTimestampMillis) {
                long timestampMillis = ((Number) value).longValue();
                return DateTimeUtil.timestamptzFromMicros(timestampMillis * 1000);
            } else if ((logicalType instanceof LogicalTypes.TimestampMicros && type.shouldAdjustToUTC())
                || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
                long timestampMicros = ((Number) value).longValue();
                return DateTimeUtil.timestamptzFromMicros(timestampMicros);
            } else if (logicalType instanceof LogicalTypes.TimestampMillis && !type.shouldAdjustToUTC()) {
                long timestampMillis = ((Number) value).longValue();
                return DateTimeUtil.timestampFromMicros(timestampMillis * 1000);
            } else if (logicalType instanceof LogicalTypes.TimestampMicros && !type.shouldAdjustToUTC()) {
                long timestampMicros = ((Number) value).longValue();
                return DateTimeUtil.timestampFromMicros(timestampMicros);
            }
        }
        return super.convertTimestamp0(fieldValue, type);
    }


    @Override
    protected ByteBuffer convertBinary0(FieldValue<org.apache.avro.Schema> fieldValue) {
        Object value = fieldValue.value();
        if (value instanceof GenericData.Fixed) {
            byte[] bytes = ((GenericData.Fixed) value).bytes();
            return super.convertBinary0(new FieldValue<>(fieldValue.type(), bytes));
        } else {
            return super.convertBinary0(fieldValue);
        }
    }

    @Override
    protected Object convertStruct(FieldValue<org.apache.avro.Schema> fieldValue, Types.StructType structType) {
        Object value = fieldValue.value();
        if (value instanceof GenericRecord) {
            return super.convertStruct(fieldValue, structType);
        } else {
            LOGGER.warn("Expected GenericRecord but got {}", value.getClass().getName());
            return null;
        }
    }

    @Override
    protected List<Object> convertList(FieldValue<org.apache.avro.Schema> fieldValue, Types.ListType listType) {
        fieldCount += 1;
        Object value = fieldValue.value();
        org.apache.avro.Schema type = fieldValue.type();
        if (value instanceof List<?>) {
            List<?> valueList = (List<?>) value;
            List<Object> list = new ArrayList<>(valueList.size());
            for (Object item :valueList) {
                Object o = convertValue(new FieldValue<>(type.getElementType(), item), listType.elementType());
                list.add(o);
            }
            return list;
        }
        throw new IllegalArgumentException("Expected List for list type, but received: " + value.getClass());
    }

    @Override
    protected Map<Object, Object> convertMap(FieldValue<org.apache.avro.Schema> fieldValue, Types.MapType mapType) {
        fieldCount += 1;
        Object value = fieldValue.value();
        org.apache.avro.Schema type = fieldValue.type();
        if (value instanceof GenericData.Array<?>) {
            GenericData.Array<?> arrayValue = (GenericData.Array<?>) value;
            org.apache.avro.Schema elementSchema = type.getElementType();
            List<org.apache.avro.Schema.Field> fields = elementSchema.getFields();
            int mapSize = arrayValue.size();
            Map<Object, Object> result = new HashMap<>(mapSize);
            for (Object element : arrayValue) {
                GenericRecord record = (GenericRecord) element;
                Object key = convertValue(new FieldValue<>(fields.get(0).schema(), record.get(0)), mapType.keyType());
                Object val = convertValue(new FieldValue<>(fields.get(1).schema(), record.get(1)), mapType.valueType());
                result.put(key, val);
            }
            return result;
        } else if (value instanceof Map<?, ?>) {
            Map<?, ?> mapValue = (Map<?, ?>) value;
            int mapSize = mapValue.size();
            Map<Object, Object> result = new HashMap<>(mapSize);
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                Object key = convertValue(new FieldValue<>(STRING_SCHEMA_INSTANCE, entry.getKey()), mapType.keyType());
                Object val = convertValue(new FieldValue<>(type.getValueType(), entry.getValue()), mapType.valueType());
                result.put(key, val);
            }
            return result;
        } else {
            throw new IllegalArgumentException("Expected Map for map type, but received: " + value.getClass());
        }
    }
}
