package kafka.automq.table.transformer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

public class SchemalessConverter implements Converter {
    private Schema tableSchema = null;
    private final Types.StructType type = Types.StructType.of(
        required(0, "timestamp", Types.TimestampType.withZone(), "The record timestamp"),
        required(1, "key", Types.StringType.get(), "The record key"),
        required(2, "value", Types.StringType.get(), "The record value")
    );
    private long fieldCount = 0;

    @Override
    public Record convert(org.apache.kafka.common.record.Record src) {
        Schema currentSchema = currentSchema().schema();

        Record record = GenericRecord.create(currentSchema);
        record.setField("timestamp", Instant.ofEpochMilli(src.timestamp()).atOffset(ZoneOffset.UTC));
        fieldCount += 1;
        String keyStr = buf2string(src.key());
        record.setField("key", keyStr);
        fieldCount += FieldMetric.count(keyStr);
        String valueStr = buf2string(src.value());
        record.setField("value", valueStr);
        fieldCount += FieldMetric.count(valueStr);

        return record;
    }

    @Override
    public void tableSchema(Schema tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public SchemaState currentSchema() {
        Schema currentSchema = tableSchema != null ? tableSchema : type.asSchema();
        return new SchemaState(currentSchema, tableSchema != null);
    }

    @Override
    public long fieldCount() {
        return fieldCount;
    }

    private static String buf2string(ByteBuffer buf) {
        if (buf == null) {
            return "";
        }
        byte[] bytes = new byte[buf.remaining()];
        buf.slice().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
