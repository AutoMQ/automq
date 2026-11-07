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

package kafka.automq.table.process.convert;

import kafka.automq.table.process.ConversionResult;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;

import org.apache.kafka.common.serialization.Deserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Converter for Avro format data with Confluent Schema Registry support.
 *
 * <p>This converter handles Kafka records that contain Avro-serialized data
 * with schema information stored in Confluent Schema Registry. It deserializes
 * the binary Avro data and resolves schemas using the registry, producing
 * standardized Avro GenericRecord objects for the processing pipeline.</p>
 *
 * @see KafkaAvroDeserializer
 */
public class AvroRegistryConverter implements Converter {
    private static final int SCHEMA_ID_SIZE = 4;
    private static final int HEADER_SIZE = SCHEMA_ID_SIZE + 1; // magic byte + schema id
    private static final byte MAGIC_BYTE = 0x0;

    private final SchemaRegistryClient client;
    private final Deserializer<Object> deserializer;

    public AvroRegistryConverter(SchemaRegistryClient client, String registryUrl, boolean isKey) {
        // Initialize deserializer with the provided client
        this.deserializer = new KafkaAvroDeserializer(client);
        this.client = client;
        // Configure the deserializer immediately upon creation
        Map<String, String> configs = Map.of("schema.registry.url", registryUrl);
        deserializer.configure(configs, isKey);
    }


    public AvroRegistryConverter(Deserializer deserializer, SchemaRegistryClient client) {
        this.deserializer = deserializer;
        this.client = client;
    }

    protected int getSchemaId(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            throw new InvalidDataException("Invalid payload size: " + buffer.remaining() + ", expected at least " + HEADER_SIZE);
        }
        ByteBuffer buf = buffer.duplicate();
        byte magicByte = buf.get();
        if (magicByte != MAGIC_BYTE) {
            throw new InvalidDataException("Unknown magic byte: " + magicByte);
        }
        return buf.getInt();
    }

    @Override
    public ConversionResult convert(String topic, ByteBuffer buffer) throws ConverterException {
        if (buffer == null) {
            throw new InvalidDataException("AvroRegistryConverter does not support null data - schema information is required");
        }

        if (buffer.remaining() == 0) {
            throw new InvalidDataException("Invalid empty Avro data - schema information is required");
        }

        Object object = deserializer.deserialize(topic, null, buffer);
        Schema schema;
        if (object instanceof GenericRecord) {
            return new ConversionResult((GenericRecord) object, String.valueOf(getSchemaId(buffer)));
        } else {
            try {
                ParsedSchema schemaById = client.getSchemaById(getSchemaId(buffer));
                schema = (Schema) schemaById.rawSchema();
                return new ConversionResult(object, schema, String.valueOf(getSchemaId(buffer)));
            } catch (RestClientException | IOException e) {
                throw new ConverterException("Failed to retrieve schema from registry for topic: " + topic, e);
            }
        }
    }
}
