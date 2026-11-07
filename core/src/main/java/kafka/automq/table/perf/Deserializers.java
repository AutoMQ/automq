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

package kafka.automq.table.perf;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

class StaticAvroDeserializer implements Deserializer<Object> {
    final Schema schema;
    final DatumReader<GenericRecord> reader;
    final DecoderFactory decoderFactory = DecoderFactory.get();

    public StaticAvroDeserializer(Schema schema) {
        this.schema = schema;
        this.reader = new GenericDatumReader<>(schema);
    }

    public Object deserialize(String topic, byte[] data) {
        try {
            return this.reader.read(null, decoderFactory.binaryDecoder(data, 5, data.length - 5, null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class StaticProtobufDeserializer implements Deserializer<Message> {
    final Descriptors.Descriptor descriptor;

    public StaticProtobufDeserializer(Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public Message deserialize(String s, byte[] bytes) {
        try {
            return DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(bytes, 5, bytes.length - 5));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
