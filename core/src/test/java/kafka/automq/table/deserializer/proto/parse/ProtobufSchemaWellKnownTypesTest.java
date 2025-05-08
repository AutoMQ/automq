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

import kafka.automq.table.deserializer.proto.schema.ProtobufSchema;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that verify parsing schema files with Google Well-Known Types and other complex protobuf features.
 *
 * Note: These tests are currently disabled because they require access to the Well-Known Type definition files
 * which are not bundled with the test resources. In a real environment, these would be available
 * through the protobuf library. To make these tests pass, you would need to:  
 * 1. Include the well-known type .proto files in your test resources
 * 2. Make them available to the ProtobufSchemaParser when parsing
 *
 * Note: The current implementation does not support the `extend` feature.
 */
@Tag("S3Unit")
class ProtobufSchemaWellKnownTypesTest {

    private static final String TEST_RESOURCES_DIR = "src/test/resources/proto";

    @Test
    @DisplayName("Should parse schema with google.protobuf.Timestamp")
    void shouldParseSchemaWithTimestamp() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_timestamp.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("EventWithTimestamp");
        assertNotNull(messageDescriptor);
        
        // Verify timestamp fields
        Descriptors.FieldDescriptor createdAtField = messageDescriptor.findFieldByName("created_at");
        Descriptors.FieldDescriptor updatedAtField = messageDescriptor.findFieldByName("updated_at");
        
        assertNotNull(createdAtField);
        assertNotNull(updatedAtField);
        
        assertEquals("google.protobuf.Timestamp", createdAtField.getMessageType().getFullName());
        assertEquals("google.protobuf.Timestamp", updatedAtField.getMessageType().getFullName());
        
        // Verify field types
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, createdAtField.getType());
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, updatedAtField.getType());
    }

    @Test
    @DisplayName("Should parse schema with multiple well-known types")
    void shouldParseSchemaWithWellKnownTypes() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_well_known_types.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("ComplexTypeMessage");
        assertNotNull(messageDescriptor);
        
        // Verify imports are preserved
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptor.toProto();
        assertTrue(fileDescriptorProto.getDependencyList().contains("google/protobuf/timestamp.proto"));
        assertTrue(fileDescriptorProto.getDependencyList().contains("google/protobuf/duration.proto"));
        assertTrue(fileDescriptorProto.getDependencyList().contains("google/protobuf/any.proto"));
        assertTrue(fileDescriptorProto.getDependencyList().contains("google/protobuf/struct.proto"));
        
        // Verify well-known type fields
        Map<String, String> fieldTypeMap = messageDescriptor.getFields().stream()
                .filter(f -> f.getType() == Descriptors.FieldDescriptor.Type.MESSAGE)
                .collect(Collectors.toMap(
                        Descriptors.FieldDescriptor::getName,
                        f -> f.getMessageType().getFullName()
                ));
        
        assertEquals("google.protobuf.Timestamp", fieldTypeMap.get("created_at"));
        assertEquals("google.protobuf.Duration", fieldTypeMap.get("elapsed_time"));
        assertEquals("google.protobuf.Any", fieldTypeMap.get("details"));
        assertEquals("google.protobuf.Struct", fieldTypeMap.get("attributes"));
        assertEquals("google.protobuf.StringValue", fieldTypeMap.get("optional_name"));
        assertEquals("google.protobuf.BoolValue", fieldTypeMap.get("is_active"));
        assertEquals("google.protobuf.Int64Value", fieldTypeMap.get("big_count"));
        assertEquals("google.protobuf.DoubleValue", fieldTypeMap.get("score"));
        assertEquals("google.protobuf.FieldMask", fieldTypeMap.get("update_mask"));
        assertEquals("google.protobuf.Empty", fieldTypeMap.get("nothing"));
    }

    @Test
    @DisplayName("Should parse schema with map fields")
    void shouldParseSchemaWithMapFields() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_map.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("MapMessage");
        assertNotNull(messageDescriptor);
        
        // Verify map fields are correctly identified as repeated messages with key/value fields
        Descriptors.FieldDescriptor stringToStringField = messageDescriptor.findFieldByName("string_to_string");
        Descriptors.FieldDescriptor intToStringField = messageDescriptor.findFieldByName("int_to_string");
        Descriptors.FieldDescriptor stringToNestedField = messageDescriptor.findFieldByName("string_to_nested");
        
        assertNotNull(stringToStringField);
        assertNotNull(intToStringField);
        assertNotNull(stringToNestedField);
        
        // Maps are represented as repeated messages with a special entry type
        assertTrue(stringToStringField.isMapField());
        assertTrue(intToStringField.isMapField());
        assertTrue(stringToNestedField.isMapField());
        
        // Verify the entry types
        Descriptors.Descriptor stringToStringEntryType = stringToStringField.getMessageType();
        assertEquals("string_to_stringentry", stringToStringEntryType.getName().toLowerCase(Locale.ENGLISH));
        assertEquals(Descriptors.FieldDescriptor.Type.STRING, 
                stringToStringEntryType.findFieldByName("key").getType());
        assertEquals(Descriptors.FieldDescriptor.Type.STRING, 
                stringToStringEntryType.findFieldByName("value").getType());
        
        // Verify map with nested message value
        Descriptors.Descriptor stringToNestedEntryType = stringToNestedField.getMessageType();
        Descriptors.FieldDescriptor valueField = stringToNestedEntryType.findFieldByName("value");
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, valueField.getType());
        assertEquals("NestedValue", valueField.getMessageType().getName());
    }

    @Test
    @DisplayName("Should parse schema with repeated fields")
    void shouldParseSchemaWithRepeatedFields() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_repeated_fields.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("RepeatedFieldsMessage");
        assertNotNull(messageDescriptor);
        
        // Verify repeated fields
        Descriptors.FieldDescriptor tagsField = messageDescriptor.findFieldByName("tags");
        Descriptors.FieldDescriptor valuesField = messageDescriptor.findFieldByName("values");
        Descriptors.FieldDescriptor itemsField = messageDescriptor.findFieldByName("items");
        Descriptors.FieldDescriptor eventTimesField = messageDescriptor.findFieldByName("event_times");
        
        assertTrue(tagsField.isRepeated());
        assertTrue(valuesField.isRepeated());
        assertTrue(itemsField.isRepeated());
        assertTrue(eventTimesField.isRepeated());
        
        // Verify repeated of complex message types
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, itemsField.getType());
        assertEquals("Item", itemsField.getMessageType().getName());
        
        // Verify repeated of well-known types
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, eventTimesField.getType());
        assertEquals("google.protobuf.Timestamp", eventTimesField.getMessageType().getFullName());
    }

    @Test
    @DisplayName("Should parse schema with reserved fields")
    void shouldParseSchemaWithReservedFields() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_reserved.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("MessageWithReserved");
        assertNotNull(messageDescriptor);
        
        // Get the proto representation to verify reserved fields
        DescriptorProtos.DescriptorProto descriptorProto = messageDescriptor.toProto();
        
        // Check reserved numbers
        assertTrue(descriptorProto.getReservedRangeList().stream()
                .anyMatch(range -> range.getStart() == 2 && range.getEnd() == 2)); // Single number 2
        assertTrue(descriptorProto.getReservedRangeList().stream()
                .anyMatch(range -> range.getStart() == 15 && range.getEnd() == 15)); // Single number 15
        assertTrue(descriptorProto.getReservedRangeList().stream()
                .anyMatch(range -> range.getStart() == 9 && range.getEnd() == 11)); // Range 9-11
        assertTrue(descriptorProto.getReservedRangeList().stream()
                .anyMatch(range -> range.getStart() == 40 && range.getEnd() == 45)); // Range 40-45
        
        // Check reserved names
        assertTrue(descriptorProto.getReservedNameList().contains("foo"));
        assertTrue(descriptorProto.getReservedNameList().contains("bar"));
        assertTrue(descriptorProto.getReservedNameList().contains("baz"));
    }

    @Test
    @DisplayName("Should parse schema with service definitions")
    @Disabled
    void shouldParseSchemaWithServiceDefinitions() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_service.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptor.toProto();
        
        // Verify service is present
        assertEquals(1, fileDescriptorProto.getServiceCount());
        assertEquals("UserService", fileDescriptorProto.getService(0).getName());
        
        // Verify the service methods
        DescriptorProtos.ServiceDescriptorProto serviceProto = fileDescriptorProto.getService(0);
        assertEquals(8, serviceProto.getMethodCount());
        
        // Verify specific methods and their streaming properties
        Map<String, DescriptorProtos.MethodDescriptorProto> methods = serviceProto.getMethodList().stream()
                .collect(Collectors.toMap(
                        DescriptorProtos.MethodDescriptorProto::getName,
                        method -> method
                ));
        
        // Regular unary method
        assertTrue(methods.containsKey("GetUser"));
        assertFalse(methods.get("GetUser").getClientStreaming());
        assertFalse(methods.get("GetUser").getServerStreaming());
        
        // Server streaming method
        assertTrue(methods.containsKey("StreamUserUpdates"));
        assertFalse(methods.get("StreamUserUpdates").getClientStreaming());
        assertTrue(methods.get("StreamUserUpdates").getServerStreaming());
        
        // Client streaming method
        assertTrue(methods.containsKey("UploadUserData"));
        assertTrue(methods.get("UploadUserData").getClientStreaming());
        assertFalse(methods.get("UploadUserData").getServerStreaming());
        
        // Bidirectional streaming method
        assertTrue(methods.containsKey("ProcessUserBatch"));
        assertTrue(methods.get("ProcessUserBatch").getClientStreaming());
        assertTrue(methods.get("ProcessUserBatch").getServerStreaming());
    }

    @Test
    @DisplayName("Should parse schema with extensions")
    @Disabled
    void shouldParseSchemaWithExtensions() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_extensions.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        
        // Verify the base message
        Descriptors.Descriptor baseMessageDescriptor = fileDescriptor.findMessageTypeByName("ExtendingMessage");
        assertNotNull(baseMessageDescriptor);
        
        // Verify extension ranges in base message
        DescriptorProtos.DescriptorProto baseProto = baseMessageDescriptor.toProto();
        assertFalse(baseProto.getExtensionRangeList().isEmpty());
        
        // Verify extension range values
        assertTrue(baseProto.getExtensionRangeList().stream()
                .anyMatch(range -> range.getStart() == 100 && range.getEnd() == 199));
    }

    @Test
    @DisplayName("Should parse schema with complex nested types")
    void shouldParseSchemaWithNestedTypes() throws Exception {
        // Given
        Path protoPath = Paths.get(TEST_RESOURCES_DIR, "with_nested_types.proto");
        String protoContent = Files.readString(protoPath);
        
        // When
        ProtobufSchema schema = ProtobufSchemaParser.parseSchema(protoContent, Collections.emptyMap());
        
        // Then
        Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
        
        // Verify the main message type
        Descriptors.Descriptor mainMessageDescriptor = fileDescriptor.findMessageTypeByName("ComplexNestedMessage");
        assertNotNull(mainMessageDescriptor);
        
        // Verify nested enum
        Descriptors.EnumDescriptor statusEnum = mainMessageDescriptor.findEnumTypeByName("Status");
        assertNotNull(statusEnum);
        assertEquals(4, statusEnum.getValues().size());
        assertEquals("UNKNOWN", statusEnum.getValues().get(0).getName());
        assertEquals("ACTIVE", statusEnum.getValues().get(1).getName());
        
        // Verify nested Address message
        Descriptors.Descriptor addressDescriptor = mainMessageDescriptor.findNestedTypeByName("Address");
        assertNotNull(addressDescriptor);
        assertEquals(4, addressDescriptor.getFields().size());
        
        // Verify deeply nested GeoLocation message
        Descriptors.Descriptor geoLocationDescriptor = addressDescriptor.findNestedTypeByName("GeoLocation");
        assertNotNull(geoLocationDescriptor);
        assertEquals(3, geoLocationDescriptor.getFields().size());
        
        // Verify deepest nested Accuracy message
        Descriptors.Descriptor accuracyDescriptor = geoLocationDescriptor.findNestedTypeByName("Accuracy");
        assertNotNull(accuracyDescriptor);
        assertEquals(2, accuracyDescriptor.getFields().size());
        
        // Verify recursive TreeNode message
        Descriptors.Descriptor treeNodeDescriptor = mainMessageDescriptor.findNestedTypeByName("TreeNode");
        assertNotNull(treeNodeDescriptor);
        
        // Verify recursive field in TreeNode
        Descriptors.FieldDescriptor childrenField = treeNodeDescriptor.findFieldByName("children");
        assertNotNull(childrenField);
        assertTrue(childrenField.isRepeated());
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, childrenField.getType());
        assertEquals(treeNodeDescriptor, childrenField.getMessageType());
        
        // Verify ContactInfo with oneof
        Descriptors.Descriptor contactInfoDescriptor = mainMessageDescriptor.findNestedTypeByName("ContactInfo");
        assertNotNull(contactInfoDescriptor);
        
        // Verify oneof fields
        Descriptors.OneofDescriptor oneofDescriptor = contactInfoDescriptor.getOneofs().get(0);
        assertEquals("contact", oneofDescriptor.getName());
        assertEquals(3, oneofDescriptor.getFields().size());
        assertTrue(oneofDescriptor.getFields().stream()
                .anyMatch(field -> field.getName().equals("email")));
        assertTrue(oneofDescriptor.getFields().stream()
                .anyMatch(field -> field.getName().equals("phone")));
        assertTrue(oneofDescriptor.getFields().stream()
                .anyMatch(field -> field.getName().equals("physical_address")));
        
        // Verify map fields
        Descriptors.FieldDescriptor labeledAddressesField = mainMessageDescriptor.findFieldByName("labeled_addresses");
        assertNotNull(labeledAddressesField);
        assertTrue(labeledAddressesField.isMapField());
        assertEquals(Descriptors.FieldDescriptor.Type.STRING, labeledAddressesField.getMessageType().findFieldByName("key").getType());
        assertEquals(addressDescriptor, labeledAddressesField.getMessageType().findFieldByName("value").getMessageType());
        
        // Verify repeated fields
        Descriptors.FieldDescriptor secondaryAddressesField = mainMessageDescriptor.findFieldByName("secondary_addresses");
        assertNotNull(secondaryAddressesField);
        assertTrue(secondaryAddressesField.isRepeated());
        assertEquals(addressDescriptor, secondaryAddressesField.getMessageType());
    }
}
