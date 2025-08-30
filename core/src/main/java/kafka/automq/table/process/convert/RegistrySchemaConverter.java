package kafka.automq.table.process.convert;

import kafka.automq.table.process.ConversionResult;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class RegistrySchemaConverter implements Converter {

    private static final Schema STRING = Schema.create(Schema.Type.STRING);

    @Override
    public final ConversionResult convert(String topic, Record record) throws ConverterException {
        try {
            if (record.value() == null) {
                throw new InvalidDataException("Kafka record value cannot be null");
            }
            RecordData value = performValueConversion(topic, record);
            if (value != null) {
                int schemaId = getSchemaId(topic, record);
                String key = buf2string(record.key());
                GenericRecord genericRecord = Converter.buildConversionRecord(
                    key, STRING,
                    value.getValue(), value.getSchema(),
                    record.timestamp());
                return new ConversionResult(record, genericRecord, String.valueOf(schemaId));
            } else {
                throw new ConverterException("Conversion returned null record for topic: " + topic);
            }

        } catch (SerializationException e) {
            throw new ConverterException("Conversion failed for topic: " + topic + " due to serialization/schema error", e);
        } catch (Exception e) {
            throw new ConverterException("Conversion failed for topic: " + topic + " with an unexpected error", e);
        }
    }


    public static class RecordData implements GenericContainer {
        private final Schema schema;
        private final Object value;
        public RecordData(Schema schema, Object object) {
            this.schema = schema;
            this.value = object;
        }
        public RecordData(GenericRecord genericRecord) {
            this.schema = genericRecord.getSchema();
            this.value = genericRecord;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        public Object getValue() {
            return value;
        }
    }
    protected abstract RecordData performValueConversion(String topic, Record record);

    protected abstract int getSchemaId(String topic, Record record);

    private static String buf2string(ByteBuffer buf) {
        if (buf == null) {
            return "";
        }
        byte[] bytes = new byte[buf.remaining()];
        buf.slice().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
