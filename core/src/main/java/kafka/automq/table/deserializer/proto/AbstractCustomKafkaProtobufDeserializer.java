/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.table.deserializer.proto;

import kafka.automq.table.deserializer.SchemaResolutionResolver;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class AbstractCustomKafkaProtobufDeserializer<T extends Message>
    extends AbstractKafkaSchemaSerDe {

    protected final Map<SchemaKey, ProtobufSchemaWrapper> schemaCache;
    protected final SchemaResolutionResolver schemaResolutionResolver;

    public AbstractCustomKafkaProtobufDeserializer() {
        this.schemaCache = new BoundedConcurrentHashMap<>(1000);
        this.schemaResolutionResolver = new HeaderBasedSchemaResolutionResolver();
    }

    public AbstractCustomKafkaProtobufDeserializer(SchemaResolutionResolver schemaResolutionResolver) {
        this.schemaCache = new BoundedConcurrentHashMap<>(1000);
        this.schemaResolutionResolver = schemaResolutionResolver;
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

        try {
            // Phase 2: Schema Resolution
            ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
            SchemaResolutionResolver.SchemaResolution resolution = schemaResolutionResolver.resolve(topic, byteBuffer);
            int schemaId = resolution.getSchemaId();
            ByteBuffer messageBytes = resolution.getMessageBytes();

            // Phase 3: Schema Processing
            ProtobufSchemaWrapper protobufSchemaWrapper = processSchema(topic, schemaId, resolution);
            Descriptors.Descriptor targetDescriptor = protobufSchemaWrapper.getDescriptor();

            // Phase 4: Message Deserialization
            Message message = deserializeMessage(targetDescriptor, messageBytes);

            @SuppressWarnings("unchecked")
            T result = (T) message;
            return result;
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error deserializing Protobuf message", e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error deserializing Protobuf message", e);
        }
    }

    private Message deserializeMessage(Descriptors.Descriptor descriptor, ByteBuffer messageBytes) throws IOException {
        if (descriptor == null) {
            throw new SerializationException("No Protobuf Descriptor found");
        }

        // Convert ByteBuffer to byte array for DynamicMessage.parseFrom
        byte[] bytes;
        if (messageBytes.hasArray() && messageBytes.arrayOffset() == 0 && messageBytes.remaining() == messageBytes.array().length) {
            // Use the backing array directly if it's a simple case
            bytes = messageBytes.array();
        } else {
            // Create a new byte array for the remaining bytes
            bytes = new byte[messageBytes.remaining()];
            messageBytes.duplicate().get(bytes);
        }

        return DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(bytes));
    }

    /**
     * Phase 3: Process and retrieve the schema
     */
    protected ProtobufSchemaWrapper processSchema(String topic, int schemaId, SchemaResolutionResolver.SchemaResolution resolution) {
        String subject = resolution.getSubject() == null ?
            getSubjectName(topic, isKey, null, null) : resolution.getSubject();
        SchemaKey key = new SchemaKey(subject, schemaId);
        try {
            CustomProtobufSchema schema = (CustomProtobufSchema) schemaRegistry.getSchemaBySubjectAndId(subject, schemaId);
            return schemaCache.computeIfAbsent(key, k -> {
                if (resolution.getIndexes() != null) {
                    return new ProtobufSchemaWrapper(schema, resolution.getIndexes());
                } else {
                    return new ProtobufSchemaWrapper(schema, resolution.getMessageTypeName());
                }
            });
        } catch (IOException | RestClientException e) {
            throw new SerializationException("Error retrieving Protobuf schema for id " + schemaId, e);
        }
    }
    protected static final class SchemaKey {
        private final String subject;
        private final int schemaId;

        protected SchemaKey(String subject, int schemaId) {
            this.subject = subject;
            this.schemaId = schemaId;
        }

        public String subject() {
            return subject;
        }

        public int schemaId() {
            return schemaId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (SchemaKey) obj;
            return Objects.equals(this.subject, that.subject) &&
                this.schemaId == that.schemaId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subject, schemaId);
        }

        @Override
        public String toString() {
            return "SchemaKey[" +
                "subject=" + subject + ", " +
                "schemaId=" + schemaId + ']';
        }

    }
}
