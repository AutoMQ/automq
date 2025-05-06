package kafka.automq.table.deserializer.proto;

import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomKafkaProtobufDeserializer<T extends Message>
    extends AbstractCustomKafkaProtobufDeserializer<T> implements Deserializer<T> {

    public CustomKafkaProtobufDeserializer() {
    }

    public CustomKafkaProtobufDeserializer(SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        CustomKafkaProtobufDeserializerConfig config = new CustomKafkaProtobufDeserializerConfig(configs);
        configure(config);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return this.deserialize(topic, null, bytes);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return (T) super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (IOException e) {
            throw new RuntimeException("Exception while closing deserializer", e);
        }
    }
}