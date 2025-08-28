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
import kafka.automq.table.transformer.ProtoToAvroConverter;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.protobuf.ProtoConversions;
import org.apache.avro.protobuf.ProtobufData;

import java.time.Duration;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class ProtobufRegistryConverter extends RegistrySchemaConverter {
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

    public ProtobufRegistryConverter(SchemaRegistryClient client, String registryUrl) {
        this(client, registryUrl, new HeaderBasedSchemaResolutionResolver());
    }


    public ProtobufRegistryConverter(SchemaRegistryClient client, String registryUrl, SchemaResolutionResolver resolver) {
        this.resolver = resolver;
        this.deserializer = new CustomKafkaProtobufDeserializer<>(client, resolver);
        // Configure the deserializer immediately upon creation
        Map<String, String> configs = Map.of("schema.registry.url", registryUrl);
        // isKey whether this converter is for a key or a value
        // isKey must be false
        deserializer.configure(configs, false);
    }

    @Override
    protected GenericRecord performConversion(String topic, Record record) {
        int schemaId = resolver.getSchemaId(topic, record);
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
    protected int getSchemaId(String topic, Record record) {
        return resolver.getSchemaId(topic, record);
    }
}
