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

import kafka.automq.table.deserializer.proto.parse.ProtobufSchemaParser;
import kafka.automq.table.deserializer.proto.schema.DynamicSchema;
import kafka.automq.table.deserializer.proto.schema.MessageIndexes;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants.DEFAULT_LOCATION;

public class ProtobufSchemaWrapper {
    private static final Logger LOGGER = Logger.getLogger(ProtobufSchemaWrapper.class.getName());

    private final CustomProtobufSchema schema;
    private final MessageIndexes messageIndexes;
    private static final Base64.Decoder DECODER = Base64.getDecoder();
    private ProtoFileElement rootElem;
    private Map<String, ProtoFileElement> dependencies;
    private Descriptors.Descriptor descriptor;

    public ProtobufSchemaWrapper(CustomProtobufSchema schema, MessageIndexes messageIndexes) {
        this.schema = schema;
        this.messageIndexes = messageIndexes;
    }


    private static ProtoFileElement buildProtoFile(String schemaString) {
        // Parse the schema string into a ProtoFileElement
        try {
            return ProtoParser.Companion.parse(
                DEFAULT_LOCATION,
                schemaString
            );
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Failed to parse schema as text format, trying binary format", e);
            // Parse the binary schema into a FileDescriptorProto
            try {
                byte[] bytes = DECODER.decode(schemaString);
                DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.parseFrom(bytes);
                return ProtobufSchemaParser.toProtoFileElement(proto);
            } catch (InvalidProtocolBufferException ex) {
                throw new ProtobufSchemaParser.SchemaParsingException("Failed to parse Protobuf schema in any supported format", e);
            }
        }
    }

    /**
     * Parses schema dependencies into ProtoFileElements.
     *
     * @param resolvedReferences Map of resolved schema dependencies
     * @return Map of import paths to parsed ProtoFileElements
     */
    private static Map<String, ProtoFileElement> parseDependencies(Map<String, String> resolvedReferences) {
        Map<String, ProtoFileElement> dependencies = new HashMap<>();

        resolvedReferences.forEach((importPath, schemaDefinition) -> {
            ProtoFileElement dependencyElement = buildProtoFile(schemaDefinition);
            dependencies.put(importPath, dependencyElement);
        });

        return dependencies;
    }

    /**
     * Gets the descriptor for the Protobuf message.
     * This method builds a descriptor from the schema definition, potentially using
     * cached values for better performance.
     *
     * @return The Protobuf Descriptor for the message
     */
    public Descriptors.Descriptor getDescriptor() {
        if (descriptor == null) {
            descriptor = buildDescriptor();
        }
        return descriptor;
    }

    private Descriptors.Descriptor buildDescriptor() {
        if (schema == null || messageIndexes == null) {
            throw new IllegalArgumentException("Schema and message indexes must be provided");
        }
        String schemaString = schema.canonicalString();
        this.rootElem = buildProtoFile(schemaString);
        this.dependencies = parseDependencies(schema.resolvedReferences());

        String messageName = toMessageName(rootElem, messageIndexes);

        DynamicSchema dynamicSchema = ProtobufSchemaParser.toDynamicSchema(messageName, rootElem, dependencies);
        return dynamicSchema.getMessageDescriptor(messageName);
    }

    private String toMessageName(ProtoFileElement rootElem, MessageIndexes indexes) {
        StringBuilder sb = new StringBuilder();
        List<TypeElement> types = rootElem.getTypes();
        boolean first = true;
        for (Integer index : indexes.indexes()) {
            if (!first) {
                sb.append(".");
            } else {
                first = false;
            }
            MessageElement message = getMessageAtIndex(types, index);
            if (message == null) {
                throw new IllegalArgumentException("Invalid message indexes: " + indexes);
            }
            sb.append(message.getName());
            types = message.getNestedTypes();
        }
        return sb.toString();
    }

    private MessageElement getMessageAtIndex(List<TypeElement> types, int index) {
        int i = 0;
        for (TypeElement type : types) {
            if (type instanceof MessageElement) {
                if (index == i) {
                    return (MessageElement) type;
                }
                i++;
            }
        }
        return null;
    }

}
