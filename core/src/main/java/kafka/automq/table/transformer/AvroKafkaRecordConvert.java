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

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class AvroKafkaRecordConvert implements KafkaRecordConvert<GenericRecord> {
    // Source: io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe#MAGIC_BYTE
    private static final byte MAGIC_BYTE = 0x0;

    private final Deserializer<Object> deserializer;

    public AvroKafkaRecordConvert() {
        this.deserializer = new KafkaAvroDeserializer();
    }

    @VisibleForTesting
    public AvroKafkaRecordConvert(Deserializer<Object> deserializer) {
        this.deserializer = deserializer;
    }

    public AvroKafkaRecordConvert(SchemaRegistryClient schemaRegistry) {
        this.deserializer = new KafkaAvroDeserializer(schemaRegistry);
    }

    @Override
    public GenericRecord convert(String topic, Record record, int schemaId) {
        Object value = deserializer.deserialize(topic, null, record.value());
        if (value instanceof GenericRecord) {
            return (GenericRecord) value;
        } else {
            return null;
        }
    }

    // io.confluent.kafka.serializers.DeserializationContext#constructor
    @Override
    public int getSchemaId(String topic, org.apache.kafka.common.record.Record record) {
        ByteBuffer buffer = record.value().duplicate();
        if (buffer.get() != MAGIC_BYTE) {
            throw new InvalidDataException("Unknown magic byte!");
        }
        return buffer.getInt();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);
    }
}
