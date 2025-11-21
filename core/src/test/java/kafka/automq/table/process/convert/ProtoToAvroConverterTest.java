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

import kafka.automq.table.deserializer.proto.parse.ProtobufSchemaParser;
import kafka.automq.table.deserializer.proto.parse.converter.ProtoConstants;
import kafka.automq.table.deserializer.proto.schema.DynamicSchema;
import kafka.automq.table.process.exception.ConverterException;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.protobuf.ProtobufData;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Focused unit tests for {@link ProtoToAvroConverter} exercise converter paths that are
 * hard to reach through the higher-level registry converter integration tests.
 */
class ProtoToAvroConverterTest {

    private static final String SIMPLE_PROTO = """
        syntax = \"proto3\";

        package kafka.automq.table.process.proto;

        message SimpleRecord {
            bool flag = 1;
            Nested nested = 2;
            optional int32 opt_scalar = 3;
        }

        message Nested {
            string note = 1;
        }
        """;

    @Test
    void skipsUnknownAvroFieldsWhenSchemaHasExtraColumns() throws Exception {
        DynamicMessage message = buildSimpleRecord(b ->
            b.setField(b.getDescriptorForType().findFieldByName("flag"), true)
        );

        Schema schema = SchemaBuilder.record("SimpleRecord")
            .fields()
            .name("flag").type().booleanType().noDefault()
            .name("ghost_field").type().stringType().noDefault()
            .endRecord();

        GenericRecord record = ProtoToAvroConverter.convert(message, schema);
        assertEquals(true, record.get("flag"));
        assertNull(record.get("ghost_field"));
    }

    @Test
    void leavesMissingPresenceFieldUnsetWhenAvroSchemaDisallowsNull() throws Exception {
        DynamicMessage message = buildSimpleRecord(b ->
            b.setField(b.getDescriptorForType().findFieldByName("flag"), false)
        );

        Schema nestedSchema = SchemaBuilder.record("Nested")
            .fields()
            .name("note").type().stringType().noDefault()
            .endRecord();

        Schema schema = SchemaBuilder.record("SimpleRecord")
            .fields()
            .name("nested").type(nestedSchema).noDefault()
            .name("opt_scalar").type().intType().noDefault()
            .endRecord();

        GenericRecord record = ProtoToAvroConverter.convert(message, schema);
        assertNull(record.get("nested"));
        assertEquals(0, record.get("opt_scalar"));
    }

    @Test
    void messageSchemaMismatchYieldsNullWhenNonRecordTypeProvided() throws Exception {
        DynamicMessage message = buildSimpleRecord(b -> {
            Descriptors.Descriptor nestedDesc = b.getDescriptorForType().findFieldByName("nested").getMessageType();
            b.setField(b.getDescriptorForType().findFieldByName("nested"),
                DynamicMessage.newBuilder(nestedDesc)
                    .setField(nestedDesc.findFieldByName("note"), "note-value")
                    .build()
            );
        });

        Schema schema = SchemaBuilder.record("SimpleRecord")
            .fields()
            .name("nested").type().longType().noDefault()
            .endRecord();

        GenericRecord record = ProtoToAvroConverter.convert(message, schema);
        assertNull(record.get("nested"));
    }

    @Test
    void convertPrimitiveWrapsByteArrayValues() throws Exception {
        Method method = ProtoToAvroConverter.class.getDeclaredMethod("convertPrimitive", Object.class, Schema.class);
        method.setAccessible(true);
        byte[] source = new byte[]{1, 2, 3};
        ByteBuffer buffer = (ByteBuffer) invoke(method, null, source, Schema.create(Schema.Type.BYTES));
        ByteBuffer copy = buffer.duplicate();
        byte[] actual = new byte[copy.remaining()];
        copy.get(actual);
        assertEquals(List.of((byte) 1, (byte) 2, (byte) 3), List.of(actual[0], actual[1], actual[2]));
    }

    @Test
    void convertSingleValueRejectsRawListsWhenFieldIsNotRepeated() throws Exception {
        Method method = ProtoToAvroConverter.class.getDeclaredMethod("convertSingleValue", Object.class, Schema.class, ProtobufData.class);
        method.setAccessible(true);
        Schema schema = Schema.create(Schema.Type.STRING);
        assertThrows(ConverterException.class, () -> invoke(method, null, List.of("unexpected"), schema, LogicalMapProtobufData.get()));
    }

    private static <T> T invoke(Method method, Object target, Object... args) throws Exception {
        try {
            return (T) method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    private static DynamicMessage buildSimpleRecord(Consumer<DynamicMessage.Builder> configurer) throws Exception {
        Descriptors.Descriptor descriptor = getDescriptor(SIMPLE_PROTO, "SimpleRecord");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        configurer.accept(builder);
        return builder.build();
    }

    private static Descriptors.Descriptor getDescriptor(String proto, String messageName) throws Exception {
        com.squareup.wire.schema.internal.parser.ProtoFileElement fileElement =
            com.squareup.wire.schema.internal.parser.ProtoParser.Companion.parse(ProtoConstants.DEFAULT_LOCATION, proto);
        DynamicSchema dynamicSchema = ProtobufSchemaParser.toDynamicSchema(messageName, fileElement, Collections.emptyMap());
        return dynamicSchema.getMessageDescriptor(messageName);
    }
}
