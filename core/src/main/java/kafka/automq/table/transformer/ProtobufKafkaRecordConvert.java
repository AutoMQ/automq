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

package kafka.automq.table.transformer;

import kafka.automq.table.deserializer.proto.CustomKafkaProtobufDeserializer;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.annotations.VisibleForTesting;
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
