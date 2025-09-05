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
import kafka.automq.table.deserializer.proto.schema.MessageIndexes;
import kafka.automq.table.process.exception.InvalidDataException;

import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;

/**
 * Default implementation of SchemaResolutionResolver that parses schema information from message headers.
 * This implementation handles the standard Confluent Kafka protobuf message format with magic byte,
 * schema ID, message indexes, and message payload.
 */
public class HeaderBasedSchemaResolutionResolver implements SchemaResolutionResolver {

    private static final int SCHEMA_ID_SIZE = 4;
    private static final int HEADER_SIZE = SCHEMA_ID_SIZE + 1; // magic byte + schema id
    private static final byte MAGIC_BYTE = 0x0;

    @Override
    public SchemaResolution resolve(String topic, ByteBuffer payload) {
        if (payload == null) {
            throw new SerializationException("Payload cannot be null");
        }

        ByteBuffer buffer = payload.duplicate();

        int schemaId = readSchemaId(buffer);

        // Extract message indexes
        MessageIndexes indexes = MessageIndexes.readFrom(buffer);

        // Extract message bytes as a slice of the buffer
        ByteBuffer messageBytes = buffer.slice();

        return new SchemaResolution(schemaId, indexes, messageBytes);
    }

    @Override
    public int getSchemaId(String topic, ByteBuffer payload) {
        // io.confluent.kafka.serializers.DeserializationContext#constructor
        return readSchemaId(payload.duplicate());
    }

    private int readSchemaId(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            throw new InvalidDataException("Invalid payload size: " + buffer.remaining() + ", expected at least " + HEADER_SIZE);
        }

        // Extract magic byte
        byte magicByte = buffer.get();
        if (magicByte != MAGIC_BYTE) {
            throw new InvalidDataException("Unknown magic byte: " + magicByte);
        }

        // Extract schema ID
        return buffer.getInt();
    }
}
