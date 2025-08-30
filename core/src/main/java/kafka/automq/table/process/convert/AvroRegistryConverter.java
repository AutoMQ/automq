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

import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;

import org.apache.kafka.common.record.Record;
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
 * @see RegistrySchemaConverter
 * @see KafkaAvroDeserializer
 */
public class AvroRegistryConverter extends RegistrySchemaConverter {
    private static final int SCHEMA_ID_SIZE = 4;
    private static final int HEADER_SIZE = SCHEMA_ID_SIZE + 1; // magic byte + schema id
    private static final byte MAGIC_BYTE = 0x0;

    private final SchemaRegistryClient client;
    private final Deserializer<Object> valueDeserializer;

    public AvroRegistryConverter(SchemaRegistryClient client, String registryUrl) {
        // Initialize deserializer with the provided client
        this.valueDeserializer = new KafkaAvroDeserializer(client);
        this.client = client;
        // Configure the deserializer immediately upon creation
        Map<String, String> configs = Map.of("schema.registry.url", registryUrl);
        // isKey whether this converter is for a key or a value
        // isKey must be false
        valueDeserializer.configure(configs, false);
    }


    public AvroRegistryConverter(Deserializer deserializer, SchemaRegistryClient client) {
        this.valueDeserializer = deserializer;
        this.client = client;
    }

    @Override
    protected RecordData performValueConversion(String topic, Record record) {
        Object object = valueDeserializer.deserialize(topic, null, record.value());
        Schema schema;
        if (object instanceof GenericRecord) {
            return new RecordData((GenericRecord) object);
        } else {
            try {
                ParsedSchema schemaById = client.getSchemaById(getSchemaId(topic, record));
                schema = (Schema) schemaById.rawSchema();
                return new RecordData(schema, object);
            } catch (RestClientException | IOException e) {
                throw new ConverterException("Failed to retrieve schema from registry for topic: " + topic, e);
            }
        }
    }

    @Override
    protected int getSchemaId(String topic, Record record) {
        ByteBuffer buffer = record.value();
        if (buffer.remaining() < HEADER_SIZE) {
            throw new InvalidDataException("Invalid payload size: " + buffer.remaining() + ", expected at least " + HEADER_SIZE);
        }
        byte magicByte = buffer.get();
        if (magicByte != MAGIC_BYTE) {
            throw new InvalidDataException("Unknown magic byte: " + magicByte);
        }
        return buffer.getInt();
    }
}
