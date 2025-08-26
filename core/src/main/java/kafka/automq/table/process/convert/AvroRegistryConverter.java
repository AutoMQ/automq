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

import kafka.automq.table.deserializer.HeaderBasedSchemaResolutionResolver;
import kafka.automq.table.deserializer.SchemaResolutionResolver;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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

    private static final Logger log = LoggerFactory.getLogger(AvroRegistryConverter.class);

    private final Deserializer<Object> deserializer;
    private final SchemaResolutionResolver resolver;
    /**
     * Creates a new Avro converter with schema registry support.
     *
     * @param client the schema registry client for schema operations, must not be null
     * @param registryUrl the schema registry URL, used to configure the deserializer
     */
    public AvroRegistryConverter(SchemaRegistryClient client, String registryUrl) {
        // Initialize deserializer with the provided client
        this.deserializer = new KafkaAvroDeserializer(client);
        this.resolver = new HeaderBasedSchemaResolutionResolver();

        // Configure the deserializer immediately upon creation
        Map<String, String> configs = Map.of("schema.registry.url", registryUrl);
        // isKey whether this converter is for a key or a value
        // isKey must be false
        deserializer.configure(configs, false);
    }

    public AvroRegistryConverter(Deserializer deserializer) {
        this.deserializer = deserializer;
        this.resolver = new HeaderBasedSchemaResolutionResolver();
    }


    @Override
    protected GenericRecord performConversion(String topic, Record record) {
        Object object = deserializer.deserialize(topic, null, record.value());
        if (object instanceof GenericRecord) {
            return (GenericRecord) object;
        } else {
            return null;
        }
    }

    @Override
    protected int getSchemaId(String topic, Record record) {
        return resolver.getSchemaId(topic, record);
    }
}
