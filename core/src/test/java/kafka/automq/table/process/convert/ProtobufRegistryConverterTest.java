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
package kafka.automq.table.process.convert;

import kafka.automq.table.binder.RecordBinder;
import kafka.automq.table.deserializer.proto.CustomProtobufSchema;
import kafka.automq.table.deserializer.proto.ProtobufSchemaProvider;
import kafka.automq.table.deserializer.proto.parse.ProtobufSchemaParser;
import kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants;
import kafka.automq.table.deserializer.proto.schema.DynamicSchema;
import kafka.automq.table.process.ConversionResult;

import org.apache.kafka.common.utils.ByteUtils;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;

import static kafka.automq.table.binder.AvroRecordBinderTypeTest.createTableWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("S3Unit")
public class ProtobufRegistryConverterTest {

    private static final String ALL_TYPES_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        import \"google/protobuf/timestamp.proto\";

        message Nested {
            string name = 1;
            int32 count = 2;
        }

        enum SampleEnum {
            SAMPLE_ENUM_UNSPECIFIED = 0;
            SAMPLE_ENUM_SECOND = 1;
        }

        message FloatArray {
            repeated double values = 1;
        }

        message StringArray {
            repeated string values = 1;
        }

        message AllTypes {
            // Scalar primitives in order defined by Avro ProtobufData mapping
            bool f_bool = 1;
            double f_double = 2;
            float f_float = 3;
            int32 f_int32 = 4;
            sint32 f_sint32 = 5;
            uint32 f_uint32 = 6;
            fixed32 f_fixed32 = 7;
            sfixed32 f_sfixed32 = 8;
            int64 f_int64 = 9;
            sint64 f_sint64 = 10;
            uint64 f_uint64 = 11;
            fixed64 f_fixed64 = 12;
            sfixed64 f_sfixed64 = 13;
            string f_string = 14;
            bytes f_bytes = 15;
            SampleEnum f_enum = 16;
            Nested f_message = 17;
            // Containers and complex types
            repeated int32 f_repeated_int32 = 18;
            map<string, int32> f_string_int32_map = 19;
            google.protobuf.Timestamp f_timestamp = 20;
            optional string f_optional_string = 21;
            oneof choice {
                string choice_str = 22;
                int32 choice_int = 23;
                FloatArray choice_float_array = 26;
                StringArray choice_string_array = 27;
            }
            repeated Nested f_nested_list = 24;
            map<string, Nested> f_string_nested_map = 25;
        }
        """;

    private static final String MAP_ONLY_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        message MapOnly {
            map<string, int32> attributes = 1;
        }
        """;

    private void testSendRecord(org.apache.iceberg.Schema schema, org.apache.iceberg.data.Record record) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(Namespace.of("default"));
        String tableName = "test";
        Table table = catalog.createTable(TableIdentifier.of(Namespace.of("default"), tableName), schema);
        TaskWriter<org.apache.iceberg.data.Record> writer = createTableWriter(table);
        try {
            writer.write(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testConvertAllPrimitiveAndCollectionTypes() throws Exception {
        String topic = "pb-all-types";
        String subject = topic + "-value";

        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        CustomProtobufSchema schema = new CustomProtobufSchema(
            "AllTypes",
            -1,
            null,
            null,
            ALL_TYPES_PROTO,
            List.of(),
            Map.of()
        );
        int schemaId = registryClient.register(subject, schema);

        ProtoFileElement fileElement = ProtoParser.Companion.parse(ProtoConstants.DEFAULT_LOCATION, ALL_TYPES_PROTO);
        DynamicSchema dynamicSchema = ProtobufSchemaParser.toDynamicSchema("AllTypes", fileElement, Collections.emptyMap());
        Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor("AllTypes");

        DynamicMessage message = buildAllTypesMessage(descriptor);
        // magic byte + schema id + single message index + serialized protobuf payload
        ByteBuffer payload = buildConfluentPayload(schemaId, message.toByteArray(), 3);

        ProtobufRegistryConverter converter = new ProtobufRegistryConverter(registryClient, "http://mock:8081", false);

        ConversionResult result = converter.convert(topic, payload.asReadOnlyBuffer());
        ConversionResult cachedResult = converter.convert(topic, buildConfluentPayload(schemaId, message.toByteArray(), 1));
        assertSame(result.getSchema(), cachedResult.getSchema(), "Schema cache should return the same Avro schema instance");
        assertEquals(String.valueOf(schemaId), result.getSchemaIdentity());

        GenericRecord record = (GenericRecord) result.getValue();
        assertPrimitiveFields(record);
        assertRepeatedAndMapFields(record);
        assertNestedAndTimestamp(record);

        org.apache.iceberg.Schema iceberg = AvroSchemaUtil.toIceberg(record.getSchema());
        RecordBinder recordBinder = new RecordBinder(iceberg, record.getSchema());
        Record bind = recordBinder.bind(record);
        testSendRecord(iceberg, bind);
    }

    private static DynamicMessage buildAllTypesMessage(Descriptors.Descriptor descriptor) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

        builder.setField(descriptor.findFieldByName("f_bool"), true);
        builder.setField(descriptor.findFieldByName("f_double"), 123.456d);
        builder.setField(descriptor.findFieldByName("f_float"), 1.5f);
        builder.setField(descriptor.findFieldByName("f_int32"), -123);
        builder.setField(descriptor.findFieldByName("f_sint32"), -456);
        builder.setField(descriptor.findFieldByName("f_uint32"), 0xFFFFFFFF);
        builder.setField(descriptor.findFieldByName("f_fixed32"), 0x80000000);
        builder.setField(descriptor.findFieldByName("f_sfixed32"), -654_321);
        builder.setField(descriptor.findFieldByName("f_int64"), -9_876_543_210L);
        builder.setField(descriptor.findFieldByName("f_sint64"), -123_456_789_012L);
        builder.setField(descriptor.findFieldByName("f_uint64"), -1L);
        builder.setField(descriptor.findFieldByName("f_fixed64"), Long.MIN_VALUE);
        builder.setField(descriptor.findFieldByName("f_sfixed64"), -9_223_372_036_854_775_000L);
        builder.setField(descriptor.findFieldByName("f_string"), "string-value");
        builder.setField(descriptor.findFieldByName("f_bytes"), ByteString.copyFromUtf8("bytes-value"));
        builder.setField(
            descriptor.findFieldByName("f_enum"),
            descriptor.getFile().findEnumTypeByName("SampleEnum").findValueByName("SAMPLE_ENUM_SECOND")
        );

        // Build FloatArray for oneof choice
        Descriptors.FieldDescriptor floatArrayField = descriptor.findFieldByName("choice_float_array");
        Descriptors.Descriptor floatArrayDescriptor = floatArrayField.getMessageType();
        DynamicMessage.Builder floatArrayBuilder = DynamicMessage.newBuilder(floatArrayDescriptor);
        Descriptors.FieldDescriptor floatValuesField = floatArrayDescriptor.findFieldByName("values");
        floatArrayBuilder.addRepeatedField(floatValuesField, 1.1);
        floatArrayBuilder.addRepeatedField(floatValuesField, 2.2);
        floatArrayBuilder.addRepeatedField(floatValuesField, 3.3);
        builder.setField(floatArrayField, floatArrayBuilder.build());

        Descriptors.FieldDescriptor nestedField = descriptor.findFieldByName("f_message");
        Descriptors.Descriptor nestedDescriptor = nestedField.getMessageType();
        DynamicMessage nestedMessage = DynamicMessage.newBuilder(nestedDescriptor)
            .setField(nestedDescriptor.findFieldByName("name"), "nested-name")
            .setField(nestedDescriptor.findFieldByName("count"), 7)
            .build();
        builder.setField(nestedField, nestedMessage);

        Descriptors.FieldDescriptor repeatedNestedField = descriptor.findFieldByName("f_nested_list");
        builder.addRepeatedField(repeatedNestedField, nestedMessage);
        DynamicMessage nestedMessage2 = DynamicMessage.newBuilder(nestedDescriptor)
            .setField(nestedDescriptor.findFieldByName("name"), "nested-name-2")
            .setField(nestedDescriptor.findFieldByName("count"), 8)
            .build();
        builder.addRepeatedField(repeatedNestedField, nestedMessage2);

        Descriptors.FieldDescriptor repeatedField = descriptor.findFieldByName("f_repeated_int32");
        builder.addRepeatedField(repeatedField, 1);
        builder.addRepeatedField(repeatedField, 2);
        builder.addRepeatedField(repeatedField, 3);

        Descriptors.FieldDescriptor mapField = descriptor.findFieldByName("f_string_int32_map");
        Descriptors.Descriptor entryDescriptor = mapField.getMessageType();
        builder.addRepeatedField(mapField, mapEntry(entryDescriptor, "key1", 11));
        builder.addRepeatedField(mapField, mapEntry(entryDescriptor, "key2", 22));

        Descriptors.FieldDescriptor nestedMapField = descriptor.findFieldByName("f_string_nested_map");
        Descriptors.Descriptor nestedEntryDescriptor = nestedMapField.getMessageType();
        builder.addRepeatedField(nestedMapField, mapEntry(nestedEntryDescriptor, "nk1", nestedMessage));
        builder.addRepeatedField(nestedMapField, mapEntry(nestedEntryDescriptor, "nk2", nestedMessage2));

        Timestamp timestamp = Timestamp.newBuilder().setSeconds(1_234_567_890L).setNanos(987_000_000).build();
        builder.setField(descriptor.findFieldByName("f_timestamp"), timestamp);

        return builder.build();
    }

    @Test
    void testConvertStandaloneMapField() throws Exception {
        String topic = "pb-map-only";
        String subject = topic + "-value";

        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        CustomProtobufSchema schema = new CustomProtobufSchema(
            "MapOnly",
            -1,
            null,
            null,
            MAP_ONLY_PROTO,
            List.of(),
            Map.of()
        );
        int schemaId = registryClient.register(subject, schema);

        ProtoFileElement fileElement = ProtoParser.Companion.parse(ProtoConstants.DEFAULT_LOCATION, MAP_ONLY_PROTO);
        DynamicSchema dynamicSchema = ProtobufSchemaParser.toDynamicSchema("MapOnly", fileElement, Collections.emptyMap());
        Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor("MapOnly");

        DynamicMessage message = buildMapOnlyMessage(descriptor);
        ByteBuffer payload = buildConfluentPayload(schemaId, message.toByteArray(), 0);

        ProtobufRegistryConverter converter = new ProtobufRegistryConverter(registryClient, "http://mock:8081", false);
        ConversionResult result = converter.convert(topic, payload.asReadOnlyBuffer());

        GenericRecord record = (GenericRecord) result.getValue();
        List<?> attributeEntries = (List<?>) record.get("attributes");
        Map<String, Integer> attributes = attributeEntries.stream()
            .map(GenericRecord.class::cast)
            .collect(Collectors.toMap(
                entry -> entry.get("key").toString(),
                entry -> (Integer) entry.get("value")
            ));

        assertEquals(Map.of("env", 1, "tier", 2), attributes);

        Schema.Field attributesField = record.getSchema().getField("attributes");
        Schema mapSchema = attributesField.schema();
        assertNotNull(mapSchema.getLogicalType(), "Map field should have logical type");
        assertEquals("map", mapSchema.getLogicalType().getName());
        assertEquals(GenericData.Array.class, record.get("attributes").getClass());

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(record.getSchema());
        assertEquals(Type.TypeID.MAP, icebergSchema.findField("attributes").type().typeId());
    }

    private static DynamicMessage buildMapOnlyMessage(Descriptors.Descriptor descriptor) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        Descriptors.FieldDescriptor mapField = descriptor.findFieldByName("attributes");
        Descriptors.Descriptor entryDescriptor = mapField.getMessageType();
        builder.addRepeatedField(mapField, mapEntry(entryDescriptor, "env", 1));
        builder.addRepeatedField(mapField, mapEntry(entryDescriptor, "tier", 2));
        return builder.build();
    }

    private static DynamicMessage mapEntry(Descriptors.Descriptor entryDescriptor, Object key, Object value) {
        return DynamicMessage.newBuilder(entryDescriptor)
            .setField(entryDescriptor.findFieldByName("key"), key)
            .setField(entryDescriptor.findFieldByName("value"), value)
            .build();
    }

    private static ByteBuffer buildConfluentPayload(int schemaId, byte[] messageBytes, int... messageIndexes) {
        byte[] indexBytes = encodeMessageIndexes(messageIndexes);
        ByteBuffer buffer = ByteBuffer.allocate(1 + Integer.BYTES + indexBytes.length + messageBytes.length);
        buffer.put((byte) 0);
        buffer.putInt(schemaId);
        buffer.put(indexBytes);
        buffer.put(messageBytes);
        buffer.flip();
        return buffer;
    }

    private static byte[] encodeMessageIndexes(int... indexes) {
        if (indexes == null || indexes.length == 0) {
            return new byte[]{0};
        }
        ByteBuffer buffer = ByteBuffer.allocate(5 * (indexes.length + 1));
        ByteUtils.writeVarint(indexes.length, buffer);
        for (int index : indexes) {
            ByteUtils.writeVarint(index, buffer);
        }
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static void assertPrimitiveFields(GenericRecord record) {
        assertEquals(true, record.get("f_bool"));
        assertEquals(123.456d, (double) record.get("f_double"), 1e-6);
        assertEquals(1.5f, (Float) record.get("f_float"), 1e-6);
        assertEquals(-123, ((Integer) record.get("f_int32")).intValue());
        assertEquals(-456, ((Integer) record.get("f_sint32")).intValue());
        int unsigned32 = ((Integer) record.get("f_uint32")).intValue();
        assertEquals("4294967295", Long.toString(Integer.toUnsignedLong(unsigned32)), "f_uint32 preserves unsigned semantics despite signed storage");
        assertEquals(Integer.MIN_VALUE, ((Integer) record.get("f_fixed32")).intValue());
        assertEquals(-654_321, ((Integer) record.get("f_sfixed32")).intValue());
        assertEquals(-9_876_543_210L, ((Long) record.get("f_int64")).longValue());
        assertEquals(-123_456_789_012L, ((Long) record.get("f_sint64")).longValue());
        long uint64 = ((Long) record.get("f_uint64")).longValue();
        assertEquals("18446744073709551615", Long.toUnsignedString(uint64), "f_uint64 preserves unsigned semantics despite signed storage");
        assertEquals(Long.MIN_VALUE, ((Long) record.get("f_fixed64")).longValue());
        assertEquals(-9_223_372_036_854_775_000L, ((Long) record.get("f_sfixed64")).longValue());
        assertEquals("string-value", record.get("f_string").toString());

        ByteBuffer bytesBuffer = ((ByteBuffer) record.get("f_bytes")).duplicate();
        byte[] bytes = new byte[bytesBuffer.remaining()];
        bytesBuffer.get(bytes);
        assertEquals("bytes-value", new String(bytes, StandardCharsets.UTF_8));

        assertEquals("SAMPLE_ENUM_SECOND", record.get("f_enum").toString());
    }

    private static void assertRepeatedAndMapFields(GenericRecord record) {
        List<Integer> repeated = ((List<?>) record.get("f_repeated_int32")).stream()
            .map(value -> (Integer) value)
            .collect(Collectors.toList());
        assertEquals(List.of(1, 2, 3), repeated);

        List<?> mapEntries = (List<?>) record.get("f_string_int32_map");
        Map<String, Integer> map = mapEntries.stream()
            .map(GenericRecord.class::cast)
            .collect(Collectors.toMap(
                entry -> entry.get("key").toString(),
                entry -> (Integer) entry.get("value")
            ));
        assertEquals(Map.of("key1", 11, "key2", 22), map);

        List<?> nestedList = (List<?>) getField(record, "f_nested_list", "fNestedList");
        List<String> nestedNames = nestedList.stream()
            .map(GenericRecord.class::cast)
            .map(n -> n.get("name").toString())
            .collect(Collectors.toList());
        assertEquals(List.of("nested-name", "nested-name-2"), nestedNames);

        List<?> nestedMapEntries = (List<?>) getField(record, "f_string_nested_map", "fStringNestedMap");
        Map<String, String> nestedMap = nestedMapEntries.stream()
            .map(GenericRecord.class::cast)
            .collect(Collectors.toMap(
                entry -> entry.get("key").toString(),
                entry -> ((GenericRecord) entry.get("value")).get("name").toString()
            ));
        assertEquals(Map.of("nk1", "nested-name", "nk2", "nested-name-2"), nestedMap);
    }

    private static void assertNestedAndTimestamp(GenericRecord record) {
        GenericRecord nested = (GenericRecord) getField(record, "f_message", "fMessage");
        assertEquals("nested-name", nested.get("name").toString());
        assertEquals(7, nested.get("count"));

        long expectedMicros = 1_234_567_890_000_000L + 987_000;
        assertEquals(expectedMicros, ((Long) record.get("f_timestamp")).longValue());

        // Optional field should fall back to proto3 default (empty string)
        assertEquals("", getField(record, "f_optional_string", "fOptionalString").toString());

        // Verify oneof with complex FloatArray type
        GenericRecord floatArrayValue = (GenericRecord) getField(record, "choice_float_array", "floatArray");
        List<?> floatValues = (List<?>) floatArrayValue.get("values");
        List<Double> expectedFloats = List.of(1.1, 2.2, 3.3);
        assertEquals(expectedFloats.size(), floatValues.size());
        for (int i = 0; i < expectedFloats.size(); i++) {
            assertEquals(expectedFloats.get(i), (Double) floatValues.get(i), 1e-6);
        }
    }

    private static Object getField(GenericRecord record, String... candidateNames) {
        for (String name : candidateNames) {
            if (record.getSchema().getField(name) != null) {
                return record.get(name);
            }
        }
        throw new IllegalArgumentException("Field not found in schema: " + String.join(", ", candidateNames));
    }
}
