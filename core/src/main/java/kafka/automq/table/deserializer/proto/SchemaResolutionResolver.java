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

import org.apache.kafka.common.record.Record;


/**
 * Interface for resolving schema information for protobuf message deserialization.
 * This interface supports different strategies for obtaining schema ID and message structure:
 * - Parse from message header (standard Confluent format)
 * - Lookup latest schema from registry by subject name
 */
public interface SchemaResolutionResolver {

    /**
     * Resolves schema information for the given message payload and topic.
     *
     * @param topic The Kafka topic name
     * @param payload The serialized protobuf payload
     * @return SchemaResolution containing schema ID, message indexes, and message bytes
     * @throws org.apache.kafka.common.errors.SerializationException if resolution fails
     */
    SchemaResolution resolve(String topic, byte[] payload);


    int getSchemaId(String topic, Record record);

    /**
     * Container class for resolved schema information.
     */
    class SchemaResolution {
        private final int schemaId;
        private final MessageIndexes indexes;
        private final byte[] messageBytes;

        public SchemaResolution(int schemaId, MessageIndexes indexes, byte[] messageBytes) {
            this.schemaId = schemaId;
            this.indexes = indexes;
            this.messageBytes = messageBytes;
        }

        public int getSchemaId() {
            return schemaId;
        }

        public MessageIndexes getIndexes() {
            return indexes;
        }

        public byte[] getMessageBytes() {
            return messageBytes;
        }
    }
}
