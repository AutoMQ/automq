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

import kafka.automq.table.deserializer.SchemaResolutionResolver;
import kafka.automq.table.deserializer.proto.CustomKafkaProtobufDeserializer;
import kafka.automq.table.deserializer.proto.HeaderBasedSchemaResolutionResolver;
import kafka.automq.table.process.ConversionResult;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.protobuf.ProtoConversions;
import org.apache.avro.protobuf.ProtobufData;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class ProtobufRegistryConverter implements Converter {
    private final Deserializer<Message> deserializer;
    private final SchemaResolutionResolver resolver;

    private final Cache<Integer, Schema> avroSchemaCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(20))
        .maximumSize(10000)
        .build();

    public ProtobufRegistryConverter(Deserializer deserializer) {
        this.deserializer = deserializer;
        this.resolver = new HeaderBasedSchemaResolutionResolver();
    }

    public ProtobufRegistryConverter(SchemaRegistryClient client, String registryUrl, boolean isKey) {
        this(client, registryUrl, new HeaderBasedSchemaResolutionResolver(), isKey);
    }

    public ProtobufRegistryConverter(SchemaRegistryClient client, String registryUrl, SchemaResolutionResolver resolver, boolean isKey) {
        this.resolver = resolver;
        this.deserializer = new CustomKafkaProtobufDeserializer<>(client, resolver);
        // Configure the deserializer immediately upon creation
        Map<String, String> configs = Map.of("schema.registry.url", registryUrl);
        deserializer.configure(configs, isKey);
    }

    @Override
    public ConversionResult convert(String topic, ByteBuffer buffer) throws ConverterException {
        if (buffer == null) {
            throw new InvalidDataException("buffer is null");
        }
        int schemaId = resolver.getSchemaId(topic, buffer);
        Message protoMessage = deserializer.deserialize(topic, null, buffer);
        Schema schema = avroSchemaCache.getIfPresent(schemaId);
        if (schema == null) {
            ProtobufData protobufData = ProtobufData.get();
            protobufData.addLogicalTypeConversion(new ProtoConversions.TimestampMicrosConversion());
            schema = protobufData.getSchema(protoMessage.getDescriptorForType());
            avroSchemaCache.put(schemaId, schema);
        }
        GenericRecord convert = ProtoToAvroConverter.convert(protoMessage, schema);
        return new ConversionResult(convert, String.valueOf(schemaId));
    }
}
