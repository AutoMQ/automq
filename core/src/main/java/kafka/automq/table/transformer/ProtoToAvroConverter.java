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

package kafka.automq.table.transformer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.protobuf.ProtoConversions;
import org.apache.avro.protobuf.ProtobufData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ProtoToAvroConverter {

    public static GenericRecord convert(Message protoMessage, Schema schema) {
        try {
            ProtobufData protobufData = ProtobufData.get();
            protobufData.addLogicalTypeConversion(new ProtoConversions.TimestampMicrosConversion());
            return convertRecord(protoMessage, schema, protobufData);
        } catch (Exception e) {
            throw new InvalidDataException("Proto to Avro conversion failed", e);
        }
    }

    private static Object convert(Message protoMessage, Schema schema, ProtobufData protobufData) {
        Conversion<?> conversion = getConversion(protoMessage.getDescriptorForType(), protobufData);
        if (conversion != null) {
            if (conversion instanceof ProtoConversions.TimestampMicrosConversion) {
                ProtoConversions.TimestampMicrosConversion timestampConversion = (ProtoConversions.TimestampMicrosConversion) conversion;
                Timestamp.Builder builder = Timestamp.newBuilder();
                Timestamp.getDescriptor().getFields().forEach(field -> {
                    String fieldName = field.getName();
                    Descriptors.FieldDescriptor protoField = protoMessage.getDescriptorForType()
                        .findFieldByName(fieldName);
                    if (protoField != null) {
                        Object value = protoMessage.getField(protoField);
                        if (value != null) {
                            builder.setField(field, value);
                        }
                    }
                });
                Timestamp timestamp = builder.build();
                return timestampConversion.toLong(timestamp, schema, null);
            }
        }
        if (schema.getType() == Schema.Type.RECORD) {
            return convertRecord(protoMessage, schema, protobufData);
        } else if (schema.getType() == Schema.Type.UNION) {
            Schema dataSchema = protobufData.getSchema(protoMessage.getDescriptorForType());
            return convertRecord(protoMessage, dataSchema, protobufData);
        } else {
            return null;
        }
    }

    private static Conversion<?> getConversion(Descriptors.Descriptor descriptor, ProtobufData protobufData) {
        String namespace = protobufData.getNamespace(descriptor.getFile(), descriptor.getContainingType());
        String name = descriptor.getName();

        if (namespace.equals("com.google.protobuf")) {
            if (name.equals("Timestamp")) {
                return new ProtoConversions.TimestampMicrosConversion();
            }
        }
        return null;
    }

    private static GenericRecord convertRecord(Message protoMessage, Schema schema, ProtobufData protobufData) {
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Descriptors.FieldDescriptor protoField = protoMessage.getDescriptorForType()
                .findFieldByName(fieldName);

            if (protoField == null)
                continue;

            Object value = protoMessage.getField(protoField);
            Object convertedValue = convertValue(value, protoField, field.schema(), protobufData);
            record.put(fieldName, convertedValue);
        }
        return record;
    }

    private static Object convertValue(Object value, Descriptors.FieldDescriptor fieldDesc, Schema avroSchema,
        ProtobufData protobufData) {
        if (value == null)
            return null;

        // process repeated fields
        if (fieldDesc.isRepeated() && value instanceof List<?>) {
            List<?> protoList = (List<?>) value;
            List<Object> avroList = new ArrayList<>();
            Schema elementSchema = avroSchema.getElementType();

            for (Object item : protoList) {
                avroList.add(convertSingleValue(item, elementSchema, protobufData));
            }
            return avroList;
        }

        return convertSingleValue(value, avroSchema, protobufData);
    }

    private static Object convertSingleValue(Object value, Schema avroSchema, ProtobufData protobufData) {
        if (value instanceof Message) {
            return convert((Message) value, avroSchema, protobufData);
        } else if (value instanceof ByteString) {
            return ((ByteString) value).asReadOnlyByteBuffer();
        } else if (value instanceof Enum) {
            return value.toString(); // protobuf Enum is represented as string
        } else if (value instanceof List) {
            throw new InvalidDataException("Should be handled in convertValue");
        }

        // primitive types
        return convertPrimitive(value, avroSchema);
    }

    private static Object convertPrimitive(Object value, Schema schema) {
        switch (schema.getType()) {
            case INT: {
                return ((Number) value).intValue();
            }
            case LONG: {
                return ((Number) value).longValue();
            }
            case FLOAT: {
                return ((Number) value).floatValue();
            }
            case DOUBLE: {
                return ((Number) value).doubleValue();
            }
            case BOOLEAN: {
                return (Boolean) value;
            }
            case BYTES: {
                if (value instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) value);
                }
                return value;
            }
            default: {
                return value;
            }
        }
    }
}
