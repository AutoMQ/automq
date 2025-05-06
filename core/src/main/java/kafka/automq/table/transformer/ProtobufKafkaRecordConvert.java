package kafka.automq.table.transformer;

import kafka.automq.table.deserializer.proto.CustomKafkaProtobufDeserializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.time.Duration;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.protobuf.ProtoConversions;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtobufKafkaRecordConvert implements KafkaRecordConvert<GenericRecord> {
    private final Deserializer<Message> deserializer;

    private final Cache<Integer, Schema> avroSchemaCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(20))
        .maximumSize(10000)
        .build();

    public ProtobufKafkaRecordConvert() {
        this.deserializer = new CustomKafkaProtobufDeserializer<>();
    }

    public ProtobufKafkaRecordConvert(SchemaRegistryClient schemaRegistry) {
        this.deserializer = new CustomKafkaProtobufDeserializer<>(schemaRegistry);
    }

    @VisibleForTesting
    public ProtobufKafkaRecordConvert(Deserializer<Message> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public GenericRecord convert(String topic, Record record, int schemaId) {
        Message protoMessage = deserializer.deserialize(topic, null, record.value());
        Schema schema = avroSchemaCache.getIfPresent(schemaId);
        if (schema == null) {
            ProtobufData protobufData = ProtobufData.get();
            protobufData.addLogicalTypeConversion(new ProtoConversions.TimestampMicrosConversion());
            schema = protobufData.getSchema(protoMessage.getDescriptorForType());
            avroSchemaCache.put(schemaId, schema);
        }
        return ProtoToAvroConverter.convert(protoMessage, schema);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);
    }
}
