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

import kafka.automq.table.deserializer.proto.schema.MessageIndexes;

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
    public SchemaResolution resolve(String topic, byte[] payload) {
        if (payload == null) {
            throw new SerializationException("Payload cannot be null");
        }

        if (payload.length < HEADER_SIZE) {
            throw new SerializationException("Invalid payload size: " + payload.length + ", expected at least " + HEADER_SIZE);
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);

        // Extract magic byte
        byte magicByte = buffer.get();
        if (magicByte != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte: " + magicByte);
        }

        // Extract schema ID
        int schemaId = buffer.getInt();

        // Extract message indexes
        MessageIndexes indexes = MessageIndexes.readFrom(buffer);

        // Extract message bytes
        int messageLength = buffer.remaining();
        byte[] messageBytes = new byte[messageLength];
        buffer.get(messageBytes);

        return new SchemaResolution(schemaId, indexes, messageBytes);
    }
}
