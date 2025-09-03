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

package kafka.automq.table.deserializer;

import kafka.automq.table.deserializer.proto.schema.MessageIndexes;

import java.nio.ByteBuffer;


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
     * @param payload The serialized protobuf payload as ByteBuffer
     * @return SchemaResolution containing schema ID, message indexes, and message bytes buffer
     * @throws org.apache.kafka.common.errors.SerializationException if resolution fails
     */
    SchemaResolution resolve(String topic, ByteBuffer payload);


    int getSchemaId(String topic, ByteBuffer payload);

    /**
     * Container class for resolved schema information.
     */
    class SchemaResolution {
        private final int schemaId;
        private final ByteBuffer messageBytes;

        private MessageIndexes indexes;
        private String subject;
        private String messageTypeName;

        public SchemaResolution(int schemaId, MessageIndexes indexes, ByteBuffer messageBytes) {
            this.schemaId = schemaId;
            this.indexes = indexes;
            this.messageBytes = messageBytes;
        }

        public SchemaResolution(int schemaId, String subject, String messageTypeName, ByteBuffer messageBytes) {
            this.schemaId = schemaId;
            this.subject = subject;
            this.messageTypeName = messageTypeName;
            this.messageBytes = messageBytes;
        }

        public int getSchemaId() {
            return schemaId;
        }

        public MessageIndexes getIndexes() {
            return indexes;
        }


        public ByteBuffer getMessageBytes() {
            return messageBytes;
        }

        public String getMessageTypeName() {
            return messageTypeName;
        }

        public String getSubject() {
            return subject;
        }
    }
}
