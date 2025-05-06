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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.Message;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class CustomKafkaProtobufDeserializer<T extends Message>
    extends AbstractCustomKafkaProtobufDeserializer<T> implements Deserializer<T> {

    public CustomKafkaProtobufDeserializer() {
    }

    public CustomKafkaProtobufDeserializer(SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        CustomKafkaProtobufDeserializerConfig config = new CustomKafkaProtobufDeserializerConfig(configs);
        configure(config);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return this.deserialize(topic, null, bytes);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return (T) super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (IOException e) {
            throw new RuntimeException("Exception while closing deserializer", e);
        }
    }
}