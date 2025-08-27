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
        if (sourceValue instanceof Utf8 || sourceValue instanceof GenericData.EnumSymbol) {
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
    protected List<?> convertList(Object sourceValue, Schema sourceSchema, Types.ListType targetType) {
        Schema elementSchema = sourceSchema.getElementType();
        List<?> sourceList = (List<?>) sourceValue;
        List<Object> list = new ArrayList<>(sourceList.size());
        for (Object element : sourceList) {
            Object convert = convert(element, elementSchema, targetType.elementType());
            list.add(convert);
        }
        return list;
    }

    @Override
    protected Map<?, ?> convertMap(Object sourceValue, Schema sourceSchema, Types.MapType targetType) {
        if (sourceValue instanceof GenericData.Array) {
            GenericData.Array<?> arrayValue = (GenericData.Array<?>) sourceValue;
            // Handle map represented as an array of key-value records
            Map<Object, Object> recordMap = new HashMap<>(arrayValue.size());
            Schema kvSchema = sourceSchema.getElementType();

            if (kvSchema.getType() == Schema.Type.UNION) {
                kvSchema = kvSchema.getTypes().stream()
                    .filter(s -> s.getType() == Schema.Type.RECORD)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                        "Map element UNION schema does not contain a RECORD type: " + sourceSchema.getElementType()));
            }

            Schema.Field keyField = kvSchema.getFields().get(0);
            Schema.Field valueField = kvSchema.getFields().get(1);

            for (Object element : arrayValue) {
                if (element == null) {
                    continue;
                }
                GenericRecord record = (GenericRecord) element;
                Object key = convert(record.get(keyField.pos()), keyField.schema(), targetType.keyType());
                Object value = convert(record.get(valueField.pos()), valueField.schema(), targetType.valueType());
                recordMap.put(key, value);
            }
            return recordMap;
        } else {
            // Handle standard Java Map
            Map<?, ?> sourceMap = (Map<?, ?>) sourceValue;
            Map<Object, Object> adaptedMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : sourceMap.entrySet()) {
                // Avro map keys are always strings
                Object key = convert(entry.getKey(), STRING_SCHEMA_INSTANCE, targetType.keyType());
                Object value = convert(entry.getValue(), sourceSchema.getValueType(), targetType.valueType());
                adaptedMap.put(key, value);
            }
            return adaptedMap;
        }
    }
}
