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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A concrete implementation of TypeAdapter that converts values from Avro's
 * data representation to Iceberg's internal Java type representation.
 * <p>
 * This class extends {@link AbstractTypeAdapter} and overrides methods to handle
 * Avro-specific types like Utf8, EnumSymbol, and Fixed, as well as Avro's
 * specific representations for List and Map.
 */
public class AvroValueAdapter extends AbstractTypeAdapter<Schema> {
    private static final org.apache.avro.Schema STRING_SCHEMA_INSTANCE = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);

    @Override
    protected Object convertString(Object sourceValue, Schema sourceSchema, Type targetType) {
        if (sourceValue instanceof Utf8) {
            return sourceValue;
        }
        if (sourceValue instanceof GenericData.EnumSymbol) {
            return sourceValue.toString();
        }
        return super.convertString(sourceValue, sourceSchema, targetType);
    }

    @Override
    protected Object convertBinary(Object sourceValue, Schema sourceSchema, Type targetType) {
        if (sourceValue instanceof GenericData.Fixed) {
            return ByteBuffer.wrap(((GenericData.Fixed) sourceValue).bytes());
        }
        return super.convertBinary(sourceValue, sourceSchema, targetType);
    }

    @Override
    protected Object convertFixed(Object sourceValue, Schema sourceSchema, Type targetType) {
        if (sourceValue instanceof GenericData.Fixed) {
            return ((GenericData.Fixed) sourceValue).bytes();
        }
        return super.convertFixed(sourceValue, sourceSchema, targetType);
    }

    @Override
    protected Object convertUUID(Object sourceValue, Schema sourceSchema, Type targetType) {
        if (sourceValue instanceof Utf8) {
            return super.convertUUID(sourceValue.toString(), sourceSchema, targetType);
        }
        return super.convertUUID(sourceValue, sourceSchema, targetType);
    }

    @Override
    protected Object convertTime(Object sourceValue, Schema sourceSchema, Type targetType) {
        if (sourceValue instanceof Number) {
            LogicalType logicalType = sourceSchema.getLogicalType();
            if (logicalType instanceof LogicalTypes.TimeMicros) {
                return DateTimeUtil.timeFromMicros(((Number) sourceValue).longValue());
            } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                return DateTimeUtil.timeFromMicros(((Number) sourceValue).longValue() * 1000);
            }
        }
        return super.convertTime(sourceValue, sourceSchema, targetType);
    }

    @Override
    protected Object convertTimestamp(Object sourceValue, Schema sourceSchema, Types.TimestampType targetType) {
        if (sourceValue instanceof Number) {
            long value = ((Number) sourceValue).longValue();
            LogicalType logicalType = sourceSchema.getLogicalType();
            if (logicalType instanceof LogicalTypes.TimestampMillis) {
                return targetType.shouldAdjustToUTC()
                        ? DateTimeUtil.timestamptzFromMicros(value * 1000)
                        : DateTimeUtil.timestampFromMicros(value * 1000);
            } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                return targetType.shouldAdjustToUTC()
                        ? DateTimeUtil.timestamptzFromMicros(value)
                        : DateTimeUtil.timestampFromMicros(value);
            } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
                return DateTimeUtil.timestampFromMicros(value * 1000);
            } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
                return DateTimeUtil.timestampFromMicros(value);
            }
        }
        return super.convertTimestamp(sourceValue, sourceSchema, targetType);
    }

    @Override
    protected List<?> convertList(Object sourceValue, Schema sourceSchema, Types.ListType targetType, StructConverter<Schema> structConverter) {
        Schema listSchema = sourceSchema;
        Schema elementSchema = listSchema.getElementType();

        List<?> sourceList;
        if (sourceValue instanceof GenericData.Array) {
            sourceList = (GenericData.Array<?>) sourceValue;
        } else if (sourceValue instanceof List) {
            sourceList = (List<?>) sourceValue;
        } else {
            throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to LIST");
        }

        List<Object> list = new ArrayList<>(sourceList.size());
        for (Object element : sourceList) {
            Object convert = convert(element, elementSchema, targetType.elementType(), structConverter);
            list.add(convert);
        }
        return list;
    }

    @Override
    protected Map<?, ?> convertMap(Object sourceValue, Schema sourceSchema, Types.MapType targetType, StructConverter<Schema> structConverter) {
        if (sourceValue instanceof GenericData.Array) {
            GenericData.Array<?> arrayValue = (GenericData.Array<?>) sourceValue;
            Map<Object, Object> recordMap = new HashMap<>(arrayValue.size());

            Schema kvSchema = sourceSchema.getElementType();

            Schema.Field keyField = kvSchema.getFields().get(0);
            Schema.Field valueField = kvSchema.getFields().get(1);
            if (keyField == null || valueField == null) {
                throw new IllegalStateException("Map entry schema missing key/value fields: " + kvSchema);
            }

            Schema keySchema = keyField.schema();
            Schema valueSchema = valueField.schema();
            Type keyType = targetType.keyType();
            Type valueType = targetType.valueType();

            for (Object element : arrayValue) {
                if (element == null) {
                    continue;
                }
                GenericRecord record = (GenericRecord) element;
                Object key = convert(record.get(keyField.pos()), keySchema, keyType, structConverter);
                Object value = convert(record.get(valueField.pos()), valueSchema, valueType, structConverter);
                recordMap.put(key, value);
            }
            return recordMap;
        }

        Schema mapSchema = sourceSchema;

        Map<?, ?> sourceMap = (Map<?, ?>) sourceValue;
        Map<Object, Object> adaptedMap = new HashMap<>(sourceMap.size());

        Schema valueSchema = mapSchema.getValueType();
        Type keyType = targetType.keyType();
        Type valueType = targetType.valueType();

        for (Map.Entry<?, ?> entry : sourceMap.entrySet()) {
            Object rawKey = entry.getKey();
            Object key = convert(rawKey, STRING_SCHEMA_INSTANCE, keyType, structConverter);
            Object value = convert(entry.getValue(), valueSchema, valueType, structConverter);
            adaptedMap.put(key, value);
        }
        return adaptedMap;
    }

    @Override
    public Object convert(Object sourceValue, Schema sourceSchema, Type targetType) {
        return convert(sourceValue, sourceSchema, targetType, this::convertStruct);
    }

    protected Object convertStruct(Object sourceValue, Schema sourceSchema, Type targetType) {
        org.apache.iceberg.Schema schema = targetType.asStructType().asSchema();
        org.apache.iceberg.data.GenericRecord result = org.apache.iceberg.data.GenericRecord.create(schema);
        for (Types.NestedField f : schema.columns()) {
            // Convert the value to the expected type
            GenericRecord record = (GenericRecord) sourceValue;
            Schema.Field sourceField = sourceSchema.getField(f.name());
            if (sourceField == null) {
                throw new IllegalStateException("Missing field '" + f.name()
                    + "' in source schema: " + sourceSchema.getFullName());
            }
            Object fieldValue = convert(record.get(f.name()), sourceField.schema(), f.type());
            result.setField(f.name(), fieldValue);
        }
        return result;
    }
}
