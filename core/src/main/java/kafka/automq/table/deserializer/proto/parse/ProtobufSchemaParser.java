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

package kafka.automq.table.deserializer.proto.parse;

import kafka.automq.table.deserializer.proto.parse.template.ProtoFileElementTemplate;
import kafka.automq.table.deserializer.proto.parse.template.ProtoSchemaFileDescriptorTemplate;
import kafka.automq.table.deserializer.proto.schema.DynamicSchema;
import kafka.automq.table.deserializer.proto.schema.ProtobufSchema;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants.DEFAULT_LOCATION;


public class ProtobufSchemaParser {

    /**
     * Converts a ProtoFileElement and its dependencies into a DynamicSchema.
     *
     * @param name The name/identifier for the schema
     * @param rootElem The root proto file element to convert
     * @param dependencies Map of dependency file names to their ProtoFileElements
     * @return A DynamicSchema representing the protobuf schema
     */
    public static DynamicSchema toDynamicSchema(String name, ProtoFileElement rootElem, Map<String, ProtoFileElement> dependencies) {
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(rootElem, "rootElem cannot be null");
        Objects.requireNonNull(dependencies, "dependencies cannot be null");

        try {
            return new ProtoFileElementTemplate().convert(name, rootElem, dependencies);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build dynamic schema", e);
        }
    }


    /**
     * Converts a FileDescriptor into a ProtoFileElement.
     * @param file The FileDescriptor to convert
     * @return The converted ProtoFileElement
     */
    public static ProtoFileElement toProtoFileElement(Descriptors.FileDescriptor file) {
        return new ProtoSchemaFileDescriptorTemplate().convert(file.toProto());
    }


    /**
     * Converts a FileDescriptorProto into a ProtoFileElement.
     * @param file The FileDescriptorProto to convert
     * @return The converted ProtoFileElement
     */
    public static ProtoFileElement toProtoFileElement(DescriptorProtos.FileDescriptorProto file) {
        return new ProtoSchemaFileDescriptorTemplate().convert(file);
    }

    /**
     * Exception thrown when schema parsing fails.
     */
    public static class SchemaParsingException extends RuntimeException {
        /**
         * Constructs a new schema parsing exception with the specified detail message and cause.
         *
         * @param message the detail message
         * @param cause the cause of the exception
         */
        public SchemaParsingException(String message, Throwable cause) {
            super(message, cause);
        }

        /**
         * Constructs a new schema parsing exception with the specified detail message.
         *
         * @param message the detail message
         */
        public SchemaParsingException(String message) {
            super(message);
        }
    }

    @VisibleForTesting
    protected static ProtobufSchema parseSchema(String schemaString, Map<String, String> dependencies) {
        ProtoFileElement fileElement = ProtoParser.Companion.parse(
            DEFAULT_LOCATION,
            schemaString
        );
        Map<String, ProtoFileElement> protoDependencies = new HashMap<>();
        dependencies.forEach((importPath, schemaDefinition) -> {
            ProtoFileElement dependencyElement = ProtoParser.Companion.parse(
                DEFAULT_LOCATION,
                schemaDefinition
            );
            protoDependencies.put(importPath, dependencyElement);
        });
        MessageElement firstMessage = null;
        for (TypeElement typeElement : fileElement.getTypes()) {
            if (typeElement instanceof MessageElement) {
                firstMessage = (MessageElement) typeElement;
            }
        }
        if (firstMessage == null) {
            throw new SchemaParsingException("No message found in schema");
        }
        DynamicSchema dynamicSchema = toDynamicSchema(firstMessage.getName(), fileElement, protoDependencies);
        Descriptors.Descriptor messageDescriptor = dynamicSchema.getMessageDescriptor(firstMessage.getName());
        Descriptors.FileDescriptor file = messageDescriptor.getFile();
        return new ProtobufSchema(file, fileElement);
    }
}
