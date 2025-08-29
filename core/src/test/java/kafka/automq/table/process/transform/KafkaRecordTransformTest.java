package kafka.automq.table.process.transform;

import kafka.automq.table.process.Converter;
import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaRecordTransformTest {

    private KafkaRecordTransform transform;
    private TransformContext context;
    private Record kafkaRecord;

    private static final long KAFKA_TIMESTAMP = 1672531200000L;
    private static final ByteBuffer KAFKA_KEY = ByteBuffer.wrap("test-key".getBytes(StandardCharsets.UTF_8));

    @BeforeEach
    void setUp() {
        transform = new KafkaRecordTransform();
        context = mock(TransformContext.class);
        kafkaRecord = mock(Record.class);
        when(context.getKafkaRecord()).thenReturn(kafkaRecord);
    }


    @Test
    void applyRecord() throws TransformException {
        // Setup: Input schema with multiple fields
        Schema eventSchema = SchemaBuilder.record("Event")
            .fields()
            .name("id").type().longType().noDefault()
            .name("payload").type().stringType().noDefault()
            .endRecord();
        GenericRecord inputRecord = new GenericRecordBuilder(eventSchema)
            .set("id", 999L)
            .set("payload", "some-data")
            .build();
        when(kafkaRecord.timestamp()).thenReturn(KAFKA_TIMESTAMP);
        when(kafkaRecord.key()).thenReturn(KAFKA_KEY);

        // Execution
        GenericRecord outputRecord = transform.apply(inputRecord, context);

        // Assertions
        assertNotNull(outputRecord);
        assertEquals(KAFKA_TIMESTAMP, outputRecord.get("timestamp"));
        assertEquals(KAFKA_KEY, outputRecord.get("key"));

        // Assert the "wrapped" value and its schema
        assertTrue(outputRecord.get("value") instanceof GenericRecord, "Value should be a GenericRecord");
        GenericRecord valueRecord = (GenericRecord) outputRecord.get("value");
        assertEquals(inputRecord, valueRecord);
        assertEquals(eventSchema, valueRecord.getSchema());
    }

    @Test
    void applyWithZeroFieldRecord() throws TransformException {
        // Setup: Input schema with no fields
        Schema emptySchema = SchemaBuilder.record("Empty").fields().endRecord();
        GenericRecord inputRecord = new GenericData.Record(emptySchema);
        when(kafkaRecord.timestamp()).thenReturn(KAFKA_TIMESTAMP);
        when(kafkaRecord.key()).thenReturn(KAFKA_KEY);

        // Execution
        GenericRecord outputRecord = transform.apply(inputRecord, context);

        // Assertions
        assertNotNull(outputRecord);
        assertEquals(KAFKA_TIMESTAMP, outputRecord.get("timestamp"));
        assertEquals(KAFKA_KEY, outputRecord.get("key"));

        // Assert the "wrapped" value
        assertTrue(outputRecord.get("value") instanceof GenericRecord);
        assertEquals(inputRecord, outputRecord.get("value"));
        assertEquals(emptySchema, ((GenericRecord) outputRecord.get("value")).getSchema());
    }

    @Test
    void testWrapAndUnwrapRoundtrip() throws TransformException {
        // Setup: Create an initial record
        Schema eventSchema = SchemaBuilder.record("Event")
            .fields()
            .name("id").type().longType().noDefault()
            .endRecord();
        GenericRecord initialRecord = new GenericRecordBuilder(eventSchema)
            .set("id", 12345L)
            .build();
        when(kafkaRecord.timestamp()).thenReturn(KAFKA_TIMESTAMP);
        when(kafkaRecord.key()).thenReturn(KAFKA_KEY);

        // 1. Wrap the record
        GenericRecord wrappedRecord = Converter.buildValueRecord(initialRecord);

        // 2. Unwrap the record
        ValueUnwrapTransform valueUnwrap = new ValueUnwrapTransform();
        valueUnwrap.configure(Collections.emptyMap());
        GenericRecord unwrappedRecord = valueUnwrap.apply(wrappedRecord, context);

        // 3. Assert the result is the same as the initial record
        assertSame(initialRecord, unwrappedRecord, "Wrap and unwrap should result in the original record.");
    }
}
