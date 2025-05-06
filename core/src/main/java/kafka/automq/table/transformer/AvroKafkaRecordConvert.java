package kafka.automq.table.transformer;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroKafkaRecordConvert implements KafkaRecordConvert<GenericRecord> {
    private final Deserializer<Object> deserializer;

    public AvroKafkaRecordConvert() {
        this.deserializer = new KafkaAvroDeserializer();
    }

    @VisibleForTesting
    public AvroKafkaRecordConvert(Deserializer<Object> deserializer) {
        this.deserializer = deserializer;
    }

    public AvroKafkaRecordConvert(SchemaRegistryClient schemaRegistry) {
        this.deserializer = new KafkaAvroDeserializer(schemaRegistry);
    }

    @Override
    public GenericRecord convert(String topic, Record record, int schemaId) {
        Object value = deserializer.deserialize(topic, null, record.value());
        if (value instanceof GenericRecord) {
            return (GenericRecord) value;
        } else {
            return null;
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);
    }
}
