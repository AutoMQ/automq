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

import kafka.automq.table.process.Converter;
import kafka.automq.table.process.convert.AvroRegistryConverter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

class AvroTestCase extends PerfTestCase {
    private final Schema schema;
    private final PayloadManager payloadManager;

    public AvroTestCase(DataType dataType, int fieldCount, int payloadCount, PerfConfig config) {
        super(dataType, "avro", fieldCount, payloadCount, config);
        this.schema = createSchema(dataType, fieldCount);
        this.payloadManager = new PayloadManager(() -> generatePayloadWithSchema(schema), payloadCount);
    }

    @Override
    PayloadManager getPayloadManager() {
        return payloadManager;
    }

    @Override
    protected byte[] generatePayload() {
        return generatePayloadWithSchema(schema);
    }

    private byte[] generatePayloadWithSchema(Schema schemaToUse) {
        GenericRecord record = new GenericData.Record(schemaToUse);
        fillRecord(record, dataType, fieldCount);
        return createAvroValue(record);
    }

    @Override
    protected Converter createConverter() {
        StaticAvroDeserializer deserializer = new StaticAvroDeserializer(schema);
        return new AvroRegistryConverter(deserializer, null);
    }

    private Schema createSchema(DataType dataType, int fieldCount) {
        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.builder()
            .record("TestRecord")
            .fields();

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = "f" + i;
            assembler = addFieldToSchema(assembler, fieldName, dataType);
        }

        return assembler.endRecord();
    }

    private SchemaBuilder.FieldAssembler<Schema> addFieldToSchema(SchemaBuilder.FieldAssembler<Schema> assembler, String fieldName, DataType dataType) {
        return switch (dataType) {
            case BOOLEAN -> assembler.name(fieldName).type().booleanType().noDefault();
            case INT -> assembler.name(fieldName).type().intType().noDefault();
            case LONG -> assembler.name(fieldName).type().longType().noDefault();
            case DOUBLE -> assembler.name(fieldName).type().doubleType().noDefault();
            case TIMESTAMP -> assembler.name(fieldName).type(LogicalTypes.timestampMillis()
                .addToSchema(Schema.create(Schema.Type.LONG))).withDefault(0);
            case STRING -> assembler.name(fieldName).type().stringType().noDefault();
            case BINARY -> assembler.name(fieldName).type().bytesType().noDefault();
            case NESTED -> {
                Schema nestedSchema = SchemaBuilder.builder().record("nested").fields()
                    .name("nf1").type().booleanType().noDefault()
                    .endRecord();
                yield assembler.name(fieldName).type(nestedSchema).noDefault();
            }
            case ARRAY -> assembler.name(fieldName).type().array().items(Schema.create(Schema.Type.BOOLEAN)).noDefault();
        };
    }

    private void fillRecord(GenericRecord record, DataType dataType, int fieldCount) {
        for (int i = 0; i < fieldCount; i++) {
            String fieldName = "f" + i;
            Object value = generateFieldValue(dataType);
            record.put(fieldName, value);
        }
    }

    private Object generateFieldValue(DataType dataType) {
        return switch (dataType) {
            case BOOLEAN, INT, LONG, DOUBLE, TIMESTAMP, STRING -> dataType.generateValue();
            case BINARY -> {
                ByteBuffer buffer = (ByteBuffer) dataType.generateValue();
                yield buffer;
            }
            case NESTED -> {
                // Create nested schema inline
                Schema nestedSchema = SchemaBuilder.builder().record("nested").fields()
                    .name("nf1").type().booleanType().noDefault()
                    .endRecord();
                GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
                nestedRecord.put("nf1", DataType.BOOLEAN.generateValue());
                yield nestedRecord;
            }
            case ARRAY -> List.of(DataType.BOOLEAN.generateValue());
        };
    }

    private static byte[] createAvroValue(GenericRecord record) {
        try {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(record, encoder);
            encoder.flush();
            byte[] avroBytes = outputStream.toByteArray();

            ByteBuf buf = Unpooled.buffer(1 + 4 + avroBytes.length);
            buf.writeByte((byte) 0x0);
            buf.writeInt(0);
            buf.writeBytes(avroBytes);

            return buf.array();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
