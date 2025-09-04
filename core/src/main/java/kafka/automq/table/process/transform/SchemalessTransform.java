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

package kafka.automq.table.process.transform;

import kafka.automq.table.process.Transform;
import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * SchemalessTransform provides a simplified transformation approach that ignores the structured data
 * from valueConverter. It extracts the raw bytes of key, value, and timestamp from the original Kafka Record
 * and assembles them into a flat GenericRecord.
 * <p>
 * This transform is designed for compatibility with legacy "schemaless" configuration.
 */
public class SchemalessTransform implements Transform {

    private static final String TRANSFORM_NAME = "schemaless";
    private static final String FIELD_KEY = "key";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_TIMESTAMP = "timestamp";

    public static final Schema SCHEMALESS_SCHEMA = SchemaBuilder
            .record("SchemalessRecord")
            .namespace("kafka.automq.table.process.transform")
            .doc("A simple record containing raw key, value, and timestamp.")
            .fields()
                .name(FIELD_KEY).doc("Original record key as string")
                .type().unionOf().nullType().and().stringType().endUnion()
                .nullDefault()
            .name(FIELD_VALUE).doc("Original record value as string")
                .type().unionOf().nullType().and().stringType().endUnion()
                .nullDefault()
            .name(FIELD_TIMESTAMP).doc("Record timestamp")
                .type().longType()
                .longDefault(0L)
            .endRecord();

    @Override
    public GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException {
        // Ignore the input record, we only use the context
        try {
            Record kafkaRecord = context.getKafkaRecord();

            // Create a new record with the predefined schema
            GenericRecord schemalessRecord = new GenericData.Record(SCHEMALESS_SCHEMA);

            // Set fields using constants
            schemalessRecord.put(FIELD_KEY, kafkaRecord.hasKey() ? buf2String(kafkaRecord.key()) : null);
            schemalessRecord.put(FIELD_VALUE, kafkaRecord.hasValue() ? buf2String(kafkaRecord.value()) : null);
            schemalessRecord.put(FIELD_TIMESTAMP, kafkaRecord.timestamp());

            return schemalessRecord;
        } catch (Exception e) {
            throw new TransformException("Failed to process record in SchemalessTransform", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Nothing to configure for this transform
    }

    @Override
    public String getName() {
        return TRANSFORM_NAME;
    }

    private String buf2String(ByteBuffer buffer) {
        if (buffer == null) {
            return "";
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.slice().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
