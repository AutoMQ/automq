package kafka.automq.table.transformer;

import kafka.automq.table.deserializer.proto.ProtobufSchemaProvider;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.server.record.TableTopicSchemaType;

import static kafka.automq.table.transformer.SchemaFormat.AVRO;

public class ConverterFactory {
    private final String registryUrl;
    private final Map<SchemaFormat, KafkaRecordConvert<GenericRecord>> recordConvertMap = new ConcurrentHashMap<>();
    private SchemaRegistryClient schemaRegistry;

    private final Cache<String, SchemaFormat> topicSchemaFormatCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(20))
        .maximumSize(10000)
        .build();

    private SchemaFormat getSchemaFormat(String topic) throws RestClientException, IOException {
        String subjectName = getSubjectName(topic);
        if (schemaRegistry != null) {
            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subjectName);
            return SchemaFormat.fromString(schemaMetadata.getSchemaType());
        }
        return null;
    }

    public ConverterFactory(String registryUrl) {
        this.registryUrl = registryUrl;
        if (registryUrl != null) {
            schemaRegistry = new CachedSchemaRegistryClient(
                registryUrl,
                AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
                List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider()),
                null
            );
        }
    }

    public Converter converter(TableTopicSchemaType type, String topic) {
        return switch (type) {
            case SCHEMALESS -> new SchemalessConverter();
            case SCHEMA -> new LazyRegistrySchemaConvert(() -> createRegistrySchemaConverter(topic));
            default -> throw new IllegalArgumentException("Unsupported converter type: " + type);
        };
    }

    private Converter createRegistrySchemaConverter(String topic) {
        if (schemaRegistry != null) {
            try {
                SchemaFormat format = topicSchemaFormatCache.getIfPresent(topic);
                if (format == null) {
                    format = getSchemaFormat(topic);
                    if (format == null) {
                        throw new RuntimeException("Failed to get schema metadata for topic: " + topic);
                    }
                    topicSchemaFormatCache.put(topic, format);
                }
                return switch (format) {
                    case AVRO -> createAvroConverter(topic);
                    case PROTOBUF -> createProtobufConverter(topic);
                    default -> throw new IllegalArgumentException("Unsupported schema format: " + format);
                };
            } catch (Exception e) {
                throw new RuntimeException("Failed to get schema metadata for topic: " + topic, e);
            }
        } else {
            throw new RuntimeException("Schema registry is not configured");
        }
    }

    private String getSubjectName(String topic) {
        return topic + "-value";
    }

    private Converter createAvroConverter(String topic) {
        KafkaRecordConvert<GenericRecord> recordConvert = recordConvertMap.computeIfAbsent(AVRO,
                format -> createKafkaAvroRecordConvert(registryUrl));
        return new RegistrySchemaAvroConverter(recordConvert, topic);
    }

    private Converter createProtobufConverter(String topic) {
        KafkaRecordConvert<GenericRecord> recordConvert = recordConvertMap.computeIfAbsent(SchemaFormat.PROTOBUF,
            format -> createKafkaProtobufRecordConvert(registryUrl));
        return new RegistrySchemaAvroConverter(recordConvert, topic);
    }

    @SuppressWarnings("unchecked")
    private KafkaRecordConvert<GenericRecord> createKafkaAvroRecordConvert(String registryUrl) {
        AvroKafkaRecordConvert avroConnectRecordConvert = new AvroKafkaRecordConvert(schemaRegistry);
        avroConnectRecordConvert.configure(Map.of("schema.registry.url", registryUrl), false);
        return avroConnectRecordConvert;
    }

    @SuppressWarnings("unchecked")
    private KafkaRecordConvert<GenericRecord> createKafkaProtobufRecordConvert(String registryUrl) {
        ProtobufKafkaRecordConvert protobufKafkaRecordConvert = new ProtobufKafkaRecordConvert(schemaRegistry);
        protobufKafkaRecordConvert.configure(Map.of("schema.registry.url", registryUrl), false);
        return protobufKafkaRecordConvert;
    }
}
