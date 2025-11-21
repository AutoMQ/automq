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

import kafka.automq.table.process.exception.ConverterException;

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
import java.util.List;

public class ProtoToAvroConverter {

    private static final ProtobufData DATA = initProtobufData();

    private static ProtobufData initProtobufData() {
        ProtobufData protobufData = LogicalMapProtobufData.get();
        protobufData.addLogicalTypeConversion(new ProtoConversions.TimestampMicrosConversion());
        return protobufData;
    }

    public static GenericRecord convert(Message protoMessage, Schema schema) {
        try {
            Schema nonNull = resolveNonNullSchema(schema);
            return convertRecord(protoMessage, nonNull, DATA);
        } catch (Exception e) {
            throw new ConverterException("Proto to Avro conversion failed", e);
        }
    }

    private static Object convert(Message protoMessage, Schema schema, ProtobufData protobufData) {
        Conversion<?> conversion = getConversion(protoMessage.getDescriptorForType(), protobufData);
        if (conversion instanceof ProtoConversions.TimestampMicrosConversion) {
            ProtoConversions.TimestampMicrosConversion timestampConversion = (ProtoConversions.TimestampMicrosConversion) conversion;
            Timestamp.Builder builder = Timestamp.newBuilder();
            Timestamp.getDescriptor().getFields().forEach(field -> {
                Descriptors.FieldDescriptor protoField = protoMessage.getDescriptorForType().findFieldByName(field.getName());
                if (protoField != null && protoMessage.hasField(protoField)) {
                    builder.setField(field, protoMessage.getField(protoField));
                }
            });
            return timestampConversion.toLong(builder.build(), schema, null);
        }

        Schema nonNull = resolveNonNullSchema(schema);
        if (nonNull.getType() == Schema.Type.RECORD) {
            return convertRecord(protoMessage, nonNull, protobufData);
        }
        return null;
    }

    private static Conversion<?> getConversion(Descriptors.Descriptor descriptor, ProtobufData protobufData) {
        String namespace = protobufData.getNamespace(descriptor.getFile(), descriptor.getContainingType());
        String name = descriptor.getName();
        if ("com.google.protobuf".equals(namespace) && "Timestamp".equals(name)) {
            return new ProtoConversions.TimestampMicrosConversion();
        }
        return null;
    }

    private static GenericRecord convertRecord(Message protoMessage, Schema recordSchema, ProtobufData protobufData) {
        GenericRecord record = new GenericData.Record(recordSchema);
        Descriptors.Descriptor descriptor = protoMessage.getDescriptorForType();

        for (Schema.Field field : recordSchema.getFields()) {
            String fieldName = field.name();
            Descriptors.FieldDescriptor protoField = descriptor.findFieldByName(fieldName);
            if (protoField == null) {
                continue;
            }

            boolean hasPresence = protoField.hasPresence() || protoField.getContainingOneof() != null;
            if (!protoField.isRepeated() && hasPresence && !protoMessage.hasField(protoField)) {
                if (allowsNull(field.schema())) {
                    record.put(fieldName, null);
                }
                continue;
            }

            Object value = protoMessage.getField(protoField);
            Object convertedValue = convertValue(value, protoField, field.schema(), protobufData);
            record.put(fieldName, convertedValue);
        }
        return record;
    }

    private static Object convertValue(Object value, Descriptors.FieldDescriptor fieldDesc, Schema avroSchema,
        ProtobufData protobufData) {
        if (value == null) {
            return null;
        }

        Schema nonNullSchema = resolveNonNullSchema(avroSchema);

        if (fieldDesc.isRepeated() && value instanceof List<?>) {
            List<?> protoList = (List<?>) value;
            GenericData.Array<Object> avroArray = new GenericData.Array<>(protoList.size(), nonNullSchema);
            Schema elementSchema = nonNullSchema.getElementType();
            for (Object item : protoList) {
                avroArray.add(convertSingleValue(item, elementSchema, protobufData));
            }
            return avroArray;
        }

        return convertSingleValue(value, nonNullSchema, protobufData);
    }

    private static Object convertSingleValue(Object value, Schema avroSchema, ProtobufData protobufData) {
        if (value instanceof Message) {
            return convert((Message) value, avroSchema, protobufData);
        } else if (value instanceof ByteString) {
            return ((ByteString) value).asReadOnlyByteBuffer();
        } else if (value instanceof Enum) {
            return value.toString();
        } else if (value instanceof List) {
            throw new ConverterException("Unexpected list type found; repeated fields should have been handled in convertValue");
        }

        return convertPrimitive(value, avroSchema);
    }

    private static Object convertPrimitive(Object value, Schema schema) {
        Schema.Type type = schema.getType();
        switch (type) {
            case INT:
                return ((Number) value).intValue();
            case LONG:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return (Boolean) value;
            case BYTES:
                if (value instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) value);
                }
                return value;
            default:
                return value;
        }
    }

    private static Schema resolveNonNullSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema type : schema.getTypes()) {
                if (type.getType() != Schema.Type.NULL) {
                    return type;
                }
            }
        }
        return schema;
    }

    private static boolean allowsNull(Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema type : schema.getTypes()) {
                if (type.getType() == Schema.Type.NULL) {
                    return true;
                }
            }
        }
        return false;
    }
}
