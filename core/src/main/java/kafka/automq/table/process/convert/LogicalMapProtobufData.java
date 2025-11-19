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

import com.google.protobuf.Descriptors;

import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.iceberg.avro.CodecSetup;

import java.util.Arrays;

/**
 * ProtobufData extension that annotates protobuf map fields with Iceberg's LogicalMap logical type so that
 * downstream Avro{@literal >}Iceberg conversion keeps them as MAP instead of generic {@literal ARRAY<record<key,value>>}.
 */
public class LogicalMapProtobufData extends ProtobufData {
    private static final LogicalMapProtobufData INSTANCE = new LogicalMapProtobufData();
    private static final Schema NULL = Schema.create(Schema.Type.NULL);

    public static LogicalMapProtobufData get() {
        return INSTANCE;
    }

    @Override
    public Schema getSchema(Descriptors.FieldDescriptor f) {
        Schema schema = super.getSchema(f);
        if (f.isMapField()) {
            Schema nonNull = resolveNonNull(schema);
            // protobuf maps are materialized as ARRAY<entry{key,value}> in Avro
            if (nonNull != null && nonNull.getType() == Schema.Type.ARRAY) {
                // set logicalType property; LogicalTypes is registered in CodecSetup
                CodecSetup.getLogicalMap().addToSchema(nonNull);
            }
        } else if (f.isOptional() && !f.isRepeated() && f.getContainingOneof() == null
            && schema.getType() != Schema.Type.UNION) {
            // Proto3 optional scalars/messages: wrap as union(type, null) so the protobuf default (typically non-null)
            // remains valid (Avro default must match the first branch).
            schema = Schema.createUnion(Arrays.asList(schema, NULL));
        } else if (f.getContainingOneof() != null && !f.isRepeated() && schema.getType() != Schema.Type.UNION) {
            // oneof fields: wrap as union(type, null) so that non-set fields can be represented as null
            schema = Schema.createUnion(Arrays.asList(schema, NULL));
        }
        return schema;
    }

    private Schema resolveNonNull(Schema schema) {
        if (schema == null) {
            return null;
        }
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema member : schema.getTypes()) {
                if (member.getType() != Schema.Type.NULL) {
                    return member;
                }
            }
            return null;
        }
        return schema;
    }
}
