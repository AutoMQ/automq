package kafka.automq.table.worker.convert;

import kafka.automq.table.transformer.FieldMetric;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

public abstract class AbstractIcebergRecordVisitor<T, R> implements IcebergRecordConverter<R> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Schema icebergSchema;
    protected long fieldCount = 0;

    protected AbstractIcebergRecordVisitor(Schema icebergSchema) {
        this.icebergSchema = icebergSchema;
    }

    protected abstract Map<String, FieldValue<T>> getFieldValues(R genericRecord);

    protected abstract FieldValue<T> getFieldValue(R genericRecord, String fieldName);

    @Override
    public org.apache.iceberg.data.Record convertRecord(R genericRecord) {
        return convertRecord(genericRecord, this.icebergSchema);
    }

    @Override
    public long fieldCount() {
        return fieldCount;
    }

    private org.apache.iceberg.data.Record convertRecord(R genericRecord, Schema schema) {
        GenericRecord result = GenericRecord.create(schema);
        for (Types.NestedField f : schema.columns()) {
            FieldValue<T> value = getFieldValue(genericRecord, f.name());
            // Convert the value to the expected type
            Object fieldValue = convertValue(value, f.type());
            result.setField(f.name(), fieldValue);
        }
        return result;
    }

    // Updated convertValue method with arrow syntax
    protected Object convertValue(FieldValue<T> value, Type type) {
        if (value == null || value.value() == null) {
            return null;
        }
        return switch (type.typeId()) {
            case BOOLEAN -> convertBoolean(value);
            case INTEGER -> convertInt(value);
            case LONG -> convertLong(value);
            case FLOAT -> convertFloat(value);
            case DOUBLE -> convertDouble(value);
            case DATE -> convertDate(value);
            case TIME -> convertTime(value);
            case TIMESTAMP -> convertTimestamp(value, (Types.TimestampType) type);
            case STRING -> convertString(value);
            case UUID -> convertUUID(value);
            case BINARY -> convertBinary(value);
            case FIXED -> convertFixed(value);
            case DECIMAL -> convertDecimal(value, (Types.DecimalType) type);
            case STRUCT -> convertStruct(value, type.asStructType());
            case LIST -> convertList(value, type.asListType());
            case MAP -> convertMap(value, type.asMapType());
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    // Type-specific conversion methods
    private boolean convertBoolean(FieldValue<T> fieldValue) {
        fieldCount++;
        Object value = fieldValue.value();
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new IllegalArgumentException("Expected Boolean or String for boolean type, but received: " + value.getClass());
    }

    private int convertInt(FieldValue<T> fieldValue) {
        fieldCount++;
        Object value = fieldValue.value();
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new IllegalArgumentException("Expected Number or String for int type, but received: " + value.getClass());
    }

    private long convertLong(FieldValue<T> fieldValue) {
        fieldCount++;
        Object value = fieldValue.value();
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Expected Number or String for long type, but received: " + value.getClass());
    }

    private float convertFloat(FieldValue<T> fieldValue) {
        fieldCount++;
        Object value = fieldValue.value();
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        throw new IllegalArgumentException("Expected Number or String for float type, but received: " + value.getClass());
    }

    private double convertDouble(FieldValue<T> fieldValue) {
        fieldCount++;
        Object value = fieldValue.value();
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new IllegalArgumentException("Expected Number or String for double type, but received: " + value.getClass());
    }

    private LocalDate convertDate(FieldValue<T> fieldValue) {
        fieldCount++;
        return convertDate0(fieldValue);
    }

    protected LocalDate convertDate0(FieldValue<T> fieldValue) {
        Object value = fieldValue.value();
        if (value instanceof Integer) {
            return LocalDate.ofEpochDay((Integer) value);
        } else if (value instanceof String) {
            return LocalDate.parse((String) value); // Assuming formatted string
        } else if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value / 1000).atZone(ZoneOffset.UTC).toLocalDate();
        } else if (value instanceof LocalDate) {
            return (LocalDate) value;
        } else if (value instanceof Date) {
            return ((Date) value).toInstant().atZone(ZoneOffset.UTC).toLocalDate();
        }
        throw new IllegalArgumentException("Expected Integer or String for date type, but received: " + value.getClass());
    }

    private LocalTime convertTime(FieldValue<T> fieldValue) {
        fieldCount++;
        return convertTime0(fieldValue);
    }

    protected LocalTime convertTime0(FieldValue<T> fieldValue) {
        Object value = fieldValue.value();
        if (value instanceof Long) {
            return LocalTime.ofNanoOfDay((Long) value * 1000);
        } else if (value instanceof String) {
            return LocalTime.parse((String) value); // Assuming formatted string
        } else if (value instanceof Date date) {
            return date.toInstant().atZone(ZoneOffset.UTC).toLocalTime();
        }
        throw new IllegalArgumentException("Expected Long or String for time type, but received: " + value.getClass());
    }

    private Temporal convertTimestamp(FieldValue<T> fieldValue, Types.TimestampType type) {
        fieldCount++;
        return convertTimestamp0(fieldValue, type);
    }

    protected Temporal convertTimestamp0(FieldValue<T> fieldValue, Types.TimestampType type) {
        Object value = fieldValue.value();
        if (value instanceof Number) {
            return getTemporal(type, (Number) value);
        } else if (value instanceof String) {
            // Note: assuming ISO-8601 formatted string
            Instant instant = Instant.parse((String) value);
            return getTemporal(type, DateTimeUtil.microsFromInstant(instant));
        } else if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            return getTemporal(type, DateTimeUtil.microsFromInstant(instant));
        }
        throw new IllegalArgumentException("Expected Long or String for timestamp type, but received: " + (value != null ? value.getClass() : "null"));
    }

    private static Temporal getTemporal(Types.TimestampType type, Number value) {
        long timestampMicros = value.longValue();
        if (type.shouldAdjustToUTC()) {
            return DateTimeUtil.timestamptzFromMicros(timestampMicros);
        } else {
            return DateTimeUtil.timestampFromMicros(timestampMicros);
        }
    }

    private String convertString(FieldValue<T> fieldValue) {
        String ret = convertString0(fieldValue);
        fieldCount += FieldMetric.count(ret);
        return ret;
    }

    protected String convertString0(FieldValue<T> fieldValue) {
        Object value = fieldValue.value();
        try {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Number || value instanceof Boolean) {
                return value.toString();
            } else if (value instanceof Map || value instanceof List) {
                return MAPPER.writeValueAsString(value);
            } else {
                return value.toString();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Object convertUUID(FieldValue<T> fieldValue) {
        fieldCount++;
        return convertUUID0(fieldValue);
    }

    protected Object convertUUID0(FieldValue<T> fieldValue) {
        Object value = fieldValue.value();
        UUID uuid = null;
        if (value instanceof String) {
            uuid = UUID.fromString((String) value);
        } else if (value instanceof UUID) {
            uuid = (UUID) value;
        }
        if (uuid != null) {
            // only FileFormat.PARQUET
            return UUIDUtil.convert(uuid);
        } else {
            throw new IllegalArgumentException("Expected String or UUID for UUID type, but received: " + value.getClass());
        }
    }

    private ByteBuffer convertBinary(FieldValue<T> fieldValue) {
        ByteBuffer ret = convertBinary0(fieldValue);
        fieldCount += FieldMetric.count(ret);
        return ret;
    }

    protected ByteBuffer convertBinary0(FieldValue<T> fieldValue) {
        Object value = fieldValue.value();
        if (value instanceof String) {
            return ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        }
        throw new IllegalArgumentException("Expected String, byte[] or ByteBuffer for binary type, but received: " + value.getClass());
    }

    private byte[] convertFixed(FieldValue<T> fieldValue) {
        byte[] ret = convertFixed0(fieldValue);
        fieldCount += FieldMetric.count(ret);
        return ret;
    }

    protected byte[] convertFixed0(FieldValue<T> fieldValue) {
        return ByteBuffers.toByteArray(convertBinary0(fieldValue));
    }

    private BigDecimal convertDecimal(FieldValue<T> fieldValue, Types.DecimalType type) {
        fieldCount++;
        Object value = fieldValue.value();
        if (value instanceof ByteBuffer) {
            // Duplicate to avoid altering the original buffer state
            ByteBuffer buf = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return new BigDecimal(new java.math.BigInteger(bytes), type.scale());
        } else if (value instanceof byte[] bytes) {
            return new BigDecimal(new java.math.BigInteger(bytes), type.scale());
        } else if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cannot convert to decimal: " + value, e);
            }
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else {
            throw new IllegalArgumentException("Expected ByteBuffer, byte[] or String for decimal type, but received: " + value.getClass());
        }
    }

    protected Object convertStruct(FieldValue<T> fieldValue, Types.StructType structType) {
        Object value = fieldValue.value();
        @SuppressWarnings("unchecked")
        R nestedGenericRecord = (R) value;
        fieldCount += 1;
        return convertRecord(nestedGenericRecord, structType.asSchema());
    }

    abstract List<Object> convertList(FieldValue<T> fieldValue, Types.ListType listType);

    abstract Map<Object, Object> convertMap(FieldValue<T> fieldValue, Types.MapType mapType);
}
