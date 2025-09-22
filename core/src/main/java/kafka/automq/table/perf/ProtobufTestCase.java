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

import kafka.automq.table.deserializer.proto.schema.DynamicSchema;
import kafka.automq.table.deserializer.proto.schema.MessageDefinition;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.convert.ProtobufRegistryConverter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

class ProtobufTestCase extends PerfTestCase {
    private final Descriptors.Descriptor descriptor;
    private final PayloadManager payloadManager;

    public ProtobufTestCase(DataType dataType, int fieldCount, int payloadCount, PerfConfig config) {
        super(dataType, "proto", fieldCount, payloadCount, config);
        this.descriptor = createDescriptor(dataType, fieldCount);
        this.payloadManager = new PayloadManager(() -> generatePayloadWithDescriptor(descriptor), payloadCount);
    }

    @Override
    PayloadManager getPayloadManager() {
        return payloadManager;
    }

    @Override
    protected byte[] generatePayload() {
        return generatePayloadWithDescriptor(descriptor);
    }

    private byte[] generatePayloadWithDescriptor(Descriptors.Descriptor descriptorToUse) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptorToUse);
        fillBuilder(builder, dataType, fieldCount);
        return createProtoBufValue(builder.build());
    }

    @Override
    protected Converter createConverter() {
        StaticProtobufDeserializer deserializer = new StaticProtobufDeserializer(descriptor);
        return new ProtobufRegistryConverter(deserializer);
    }

    private Descriptors.Descriptor createDescriptor(DataType dataType, int fieldCount) {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        schemaBuilder.setPackage("example");
        schemaBuilder.setName("DynamicTest.proto");

        MessageDefinition.Builder msgDefBuilder = MessageDefinition.newBuilder("TestMessage");

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = "f" + i;
            int fieldNumber = i + 1;
            addFieldToMessage(msgDefBuilder, fieldName, fieldNumber, dataType);
        }

        schemaBuilder.addMessageDefinition(msgDefBuilder.build());

        try {
            DynamicSchema schema = schemaBuilder.build();
            return schema.getMessageDescriptor("TestMessage");
        } catch (Exception e) {
            throw new RuntimeException("Schema build failed", e);
        }
    }

    private void addFieldToMessage(MessageDefinition.Builder msgDefBuilder, String fieldName, int fieldNumber, DataType dataType) {
        switch (dataType) {
            case BOOLEAN -> msgDefBuilder.addField("required", "bool", fieldName, fieldNumber, null);
            case INT -> msgDefBuilder.addField("required", "int32", fieldName, fieldNumber, null);
            case LONG, TIMESTAMP -> msgDefBuilder.addField("required", "int64", fieldName, fieldNumber, null);
            case DOUBLE -> msgDefBuilder.addField("required", "double", fieldName, fieldNumber, null);
            case STRING -> msgDefBuilder.addField("required", "string", fieldName, fieldNumber, null);
            case BINARY -> msgDefBuilder.addField("required", "bytes", fieldName, fieldNumber, null);
            case NESTED -> {
                MessageDefinition nested = MessageDefinition.newBuilder("NestedType" + fieldName)
                    .addField("required", "bool", "nf1", 1, null)
                    .build();
                msgDefBuilder.addMessageDefinition(nested);
                msgDefBuilder.addField("required", "NestedType" + fieldName, fieldName, fieldNumber, null);
            }
            case ARRAY -> msgDefBuilder.addField("repeated", "bool", fieldName, fieldNumber, null);
        }
    }

    private void fillBuilder(DynamicMessage.Builder builder, DataType dataType, int fieldCount) {
        for (int i = 0; i < fieldCount; i++) {
            String fieldName = "f" + i;
            setFieldValue(builder, fieldName, dataType);
        }
    }

    private void setFieldValue(DynamicMessage.Builder builder, String fieldName, DataType dataType) {
        Descriptors.FieldDescriptor field = builder.getDescriptorForType().findFieldByName(fieldName);

        switch (dataType) {
            case BOOLEAN, INT, LONG, DOUBLE, TIMESTAMP, STRING ->
                builder.setField(field, dataType.generateValue());
            case BINARY -> {
                byte[] bytes = new byte[32];
                java.util.concurrent.ThreadLocalRandom.current().nextBytes(bytes);
                builder.setField(field, bytes);
            }
            case NESTED -> {
                Descriptors.Descriptor nestedDescriptor = builder.getDescriptorForType().findNestedTypeByName("NestedType" + fieldName);
                DynamicMessage nestedMessage = DynamicMessage.newBuilder(nestedDescriptor)
                    .setField(nestedDescriptor.findFieldByName("nf1"), DataType.BOOLEAN.generateValue())
                    .build();
                builder.setField(field, nestedMessage);
            }
            case ARRAY ->
                builder.addRepeatedField(field, DataType.BOOLEAN.generateValue());
        }
    }

    private static byte[] createProtoBufValue(Message message) {
        try {
            byte[] protobufBytes = message.toByteArray();

            ByteBuf buf = Unpooled.buffer(1 + 4 + protobufBytes.length);
            buf.writeByte((byte) 0x0);
            buf.writeInt(0);
            buf.writeBytes(protobufBytes);

            return buf.array();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
