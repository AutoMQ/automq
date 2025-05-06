package kafka.automq.table.deserializer.proto;

import kafka.automq.table.deserializer.proto.schema.MessageIndexes;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;

public abstract class AbstractCustomKafkaProtobufDeserializer<T extends Message>
    extends AbstractKafkaSchemaSerDe {
    private static final int SCHEMA_ID_SIZE = 4;
    private static final int HEADER_SIZE = SCHEMA_ID_SIZE + 1; // magic byte + schema id

    protected final Map<SchemaKey, ProtobufSchemaWrapper> schemaCache;

    protected record SchemaKey(String subject, int schemaId) {
    }

    public AbstractCustomKafkaProtobufDeserializer() {
        this.schemaCache = new BoundedConcurrentHashMap<>(1000);
    }

    protected void configure(CustomKafkaProtobufDeserializerConfig config) {
        configureClientProperties(config, new ProtobufSchemaProvider());
    }

    /**
     * Deserialize protobuf message from the given byte array.
     * The implementation follows the open-closed principle by breaking down the
     * deserialization process into multiple phases that can be extended by subclasses.
     *
     * @param topic   The Kafka topic
     * @param headers The Kafka record headers
     * @param payload The serialized protobuf payload
     * @return The deserialized object
     */
    protected T deserialize(String topic, Headers headers, byte[] payload)
        throws SerializationException, InvalidConfigurationException {
        // Phase 1: Pre-validation
        if (payload == null) {
            return null;
        }
        if (schemaRegistry == null) {
            throw new InvalidConfigurationException("Schema registry not found, make sure the schema.registry.url is set");
        }

        int schemaId = 0;
        byte[] messageBytes;
        MessageIndexes indexes;
        Message message;

        try {
            // Phase 2: Message Header Parsing
            ByteBuffer buffer = processHeader(payload);
            schemaId = extractSchemaId(buffer);
            indexes = extractMessageIndexes(buffer);
            messageBytes = extractMessageBytes(buffer);

            // Phase 3: Schema Processing
            ProtobufSchemaWrapper protobufSchemaWrapper = processSchema(topic, schemaId, indexes);
            Descriptors.Descriptor targetDescriptor = protobufSchemaWrapper.getDescriptor();

            // Phase 4: Message Deserialization
            message = deserializeMessage(targetDescriptor, messageBytes);

            return (T) message;
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error deserializing Protobuf message for id " + schemaId, e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error deserializing Protobuf message for id " + schemaId, e);
        }
    }

    private Message deserializeMessage(Descriptors.Descriptor descriptor, byte[] messageBytes) throws IOException {
        if (descriptor == null) {
            throw new SerializationException("No Protobuf Descriptor found");
        }
        return DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(messageBytes));
    }

    /**
     * Phase 2a: Process the header of the message
     *
     * @param payload The serialized payload
     * @return ByteBuffer positioned after the magic byte
     */
    protected ByteBuffer processHeader(byte[] payload) {
        return getByteBuffer(payload);
    }

    protected ByteBuffer getByteBuffer(byte[] payload) {
        if (payload == null || payload.length < HEADER_SIZE) {
            throw new SerializationException("Invalid payload size");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        byte magicByte = buffer.get();
        if (magicByte != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte: " + magicByte);
        }
        return buffer;
    }

    /**
     * Phase 2b: Extract the schema ID from the buffer
     *
     * @param buffer The byte buffer positioned after the magic byte
     * @return The schema ID
     */
    protected int extractSchemaId(ByteBuffer buffer) {
        return buffer.getInt();
    }

    /**
     * Phase 2c: Extract message indexes from the buffer
     *
     * @param buffer The byte buffer positioned after the schema ID
     * @return The message indexes
     */
    protected MessageIndexes extractMessageIndexes(ByteBuffer buffer) {
        return MessageIndexes.readFrom(buffer);
    }

    /**
     * Phase 2d: Extract the actual message bytes from the buffer
     *
     * @param buffer The byte buffer positioned after the message indexes
     * @return The message bytes
     */
    protected byte[] extractMessageBytes(ByteBuffer buffer) {
        int messageLength = buffer.remaining();

        byte[] messageBytes = new byte[messageLength];
        buffer.get(messageBytes);
        return messageBytes;
    }

    /**
     * Phase 3: Process and retrieve the schema
     *
     * @param topic    The Kafka topic
     * @param schemaId The schema ID
     * @param indexes  The message indexes
     * @return The protobuf schema wrapper
     */
    protected ProtobufSchemaWrapper processSchema(String topic, int schemaId, MessageIndexes indexes) {
        String subject = getSubjectName(topic, isKey, null, null);
        SchemaKey key = new SchemaKey(subject, schemaId);
        try {
            CustomProtobufSchema schema = (CustomProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, schemaId);
            return schemaCache.computeIfAbsent(key, k -> new ProtobufSchemaWrapper(schema, indexes));
        } catch (IOException | RestClientException e) {
            throw new SerializationException("Error retrieving Protobuf schema for id " + schemaId, e);
        }
    }

}
