package kafka.automq.table.binder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class AvroValueAdapterTest {
    @Test
    void timestamp() {
        final var schema = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Evt",
              "namespace": "test",
              "fields": [
                {
                  "name": "Timestamp",
                  "default": null,
                  "type": [
                    "null",
                    {
                      "type": "long",
                      "logicalType": "timestamp-millis"
                    }
                  ]
                }
              ]
            }
            """);
        final var record = new GenericData.Record(schema);
        record.put(0, Instant.EPOCH.plusMillis(10_000).toEpochMilli() /*constant for asserts*/);
        final var iceberg = new org.apache.iceberg.Schema(List.of(
            Types.NestedField.of(1, false, "Timestamp", Types.TimestampType.withZone())
        ));
        final var res = new RecordBinder(iceberg, schema).bind(record);
        final var timestamp = res.get(1);
        assertEquals(10_000L, assertInstanceOf(OffsetDateTime.class, timestamp).toInstant().toEpochMilli());
    }
}
