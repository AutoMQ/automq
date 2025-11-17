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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Abstract implementation providing common type conversion logic from source formats
 * to Iceberg's internal Java type representation.
 * <p>
 * Handles dispatch logic and provides default conversion implementations for primitive types.
 * Subclasses implement format-specific conversion for complex types (LIST, MAP, STRUCT).
 *
 * @param <S> The type of the source schema (e.g., org.apache.avro.Schema)
 */
public abstract class AbstractTypeAdapter<S> implements TypeAdapter<S> {


    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    @Override
    public Object convert(Object sourceValue, S sourceSchema, Type targetType, StructConverter<S> structConverter) {
        if (sourceValue == null) {
            return null;
        }

        switch (targetType.typeId()) {
            case BOOLEAN:
                return convertBoolean(sourceValue, sourceSchema, targetType);
            case INTEGER:
                return convertInteger(sourceValue, sourceSchema, targetType);
            case LONG:
                return convertLong(sourceValue, sourceSchema, targetType);
            case FLOAT:
                return convertFloat(sourceValue, sourceSchema, targetType);
            case DOUBLE:
                return convertDouble(sourceValue, sourceSchema, targetType);
            case STRING:
                return convertString(sourceValue, sourceSchema, targetType);
            case BINARY:
                return convertBinary(sourceValue, sourceSchema, targetType);
            case FIXED:
                return convertFixed(sourceValue, sourceSchema, targetType);
            case UUID:
                return convertUUID(sourceValue, sourceSchema, targetType);
            case DECIMAL:
                return convertDecimal(sourceValue, sourceSchema, (Types.DecimalType) targetType);
            case DATE:
                return convertDate(sourceValue, sourceSchema, targetType);
            case TIME:
                return convertTime(sourceValue, sourceSchema, targetType);
            case TIMESTAMP:
                return convertTimestamp(sourceValue, sourceSchema, (Types.TimestampType) targetType);
            case LIST:
                return convertList(sourceValue, sourceSchema, (Types.ListType) targetType, structConverter);
            case MAP:
                return convertMap(sourceValue, sourceSchema, (Types.MapType) targetType, structConverter);
            case STRUCT:
                return structConverter.convert(sourceValue, sourceSchema, targetType);
            default:
                return sourceValue;
        }
    }

    protected Object convertBoolean(Object sourceValue, S ignoredSourceSchema, Type targetType) {
        if (sourceValue instanceof Boolean) return sourceValue;
        if (sourceValue instanceof String) return Boolean.parseBoolean((String) sourceValue);
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertInteger(Object sourceValue, S ignoredSourceSchema, Type targetType) {
        if (sourceValue instanceof Integer) return sourceValue;
        if (sourceValue instanceof Number) return ((Number) sourceValue).intValue();
        if (sourceValue instanceof String) return Integer.parseInt((String) sourceValue);
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertLong(Object sourceValue, S ignoredSourceSchema, Type targetType) {
        if (sourceValue instanceof Long) return sourceValue;
        if (sourceValue instanceof Number) return ((Number) sourceValue).longValue();
        if (sourceValue instanceof String) return Long.parseLong((String) sourceValue);
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertFloat(Object sourceValue, S ignoredSourceSchema, Type targetType) {
        if (sourceValue instanceof Float) return  sourceValue;
        if (sourceValue instanceof Number) return ((Number) sourceValue).floatValue();
        if (sourceValue instanceof String) return Float.parseFloat((String) sourceValue);
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertDouble(Object sourceValue, S ignoredSourceSchema, Type targetType) {
        if (sourceValue instanceof Double) return sourceValue;
        if (sourceValue instanceof Number) return ((Number) sourceValue).doubleValue();
        if (sourceValue instanceof String) return Double.parseDouble((String) sourceValue);
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertString(Object sourceValue, S sourceSchema, Type targetType) {
        if (sourceValue instanceof String) {
            return sourceValue;
        }
        // Simple toString conversion - subclasses can override for more complex logic
        return sourceValue.toString();
    }

    protected Object convertBinary(Object sourceValue, S sourceSchema, Type targetType) {
        if (sourceValue instanceof ByteBuffer) return ((ByteBuffer) sourceValue).duplicate();
        if (sourceValue instanceof byte[]) return ByteBuffer.wrap((byte[]) sourceValue);
        if (sourceValue instanceof String) return ByteBuffer.wrap(((String) sourceValue).getBytes(StandardCharsets.UTF_8));
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertFixed(Object sourceValue, S sourceSchema, Type targetType) {
        if (sourceValue instanceof byte[]) return sourceValue;
        if (sourceValue instanceof ByteBuffer) return ByteBuffers.toByteArray((ByteBuffer) sourceValue);
        if (sourceValue instanceof String) return ((String) sourceValue).getBytes(StandardCharsets.UTF_8);
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertUUID(Object sourceValue, S sourceSchema, Type targetType) {
        UUID uuid = null;
        if (sourceValue instanceof String) {
            uuid = UUID.fromString(sourceValue.toString());
        } else if (sourceValue instanceof UUID) {
            uuid = (UUID) sourceValue;
        } else if (sourceValue instanceof ByteBuffer) {
            ByteBuffer bb = ((ByteBuffer) sourceValue).duplicate();
            if (bb.remaining() == 16) {
                uuid = new UUID(bb.getLong(), bb.getLong());
            }
        }
        if (uuid != null) {
            return UUIDUtil.convert(uuid);
        }
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertDecimal(Object sourceValue, S ignoredSourceSchema, Types.DecimalType targetType) {
        if (sourceValue instanceof BigDecimal) return sourceValue;
        if (sourceValue instanceof String) return new BigDecimal((String) sourceValue);
        if (sourceValue instanceof byte[]) return new BigDecimal(new java.math.BigInteger((byte[]) sourceValue), targetType.scale());
        if (sourceValue instanceof ByteBuffer) {
            ByteBuffer bb = ((ByteBuffer) sourceValue).duplicate();
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return new BigDecimal(new java.math.BigInteger(bytes), targetType.scale());
        }
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertDate(Object sourceValue, S ignoredSourceSchema, Type targetType) {
        if (sourceValue instanceof LocalDate) return sourceValue;
        if (sourceValue instanceof Number) return LocalDate.ofEpochDay(((Number) sourceValue).intValue());
        if (sourceValue instanceof Date) return ((Date) sourceValue).toInstant().atZone(ZoneOffset.UTC).toLocalDate();
        if (sourceValue instanceof String) return LocalDate.parse(sourceValue.toString());
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertTime(Object sourceValue, S sourceSchema, Type targetType) {
        if (sourceValue instanceof LocalTime) return sourceValue;
        if (sourceValue instanceof Date) return ((Date) sourceValue).toInstant().atZone(ZoneOffset.UTC).toLocalTime();
        if (sourceValue instanceof String) return LocalTime.parse(sourceValue.toString());
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected Object convertTimestamp(Object sourceValue, S sourceSchema, Types.TimestampType targetType) {
        if (sourceValue instanceof Temporal) return sourceValue;
        if (sourceValue instanceof Date) {
            Instant instant = ((Date) sourceValue).toInstant();
            return DateTimeUtil.timestamptzFromMicros(DateTimeUtil.microsFromInstant(instant));
        }
        if (sourceValue instanceof String) {
            Instant instant = Instant.parse(sourceValue.toString());
            return DateTimeUtil.timestamptzFromMicros(DateTimeUtil.microsFromInstant(instant));
        }
        if (sourceValue instanceof Number) {
            return DateTimeUtil.timestamptzFromMicros(((Number) sourceValue).longValue());
        }
        throw new IllegalArgumentException("Cannot convert " + sourceValue.getClass().getSimpleName() + " to " + targetType.typeId());
    }

    protected abstract List<?> convertList(Object sourceValue, S sourceSchema, Types.ListType targetType, StructConverter<S> structConverter);

    protected abstract Map<?, ?> convertMap(Object sourceValue, S sourceSchema, Types.MapType targetType, StructConverter<S> structConverter);
}
