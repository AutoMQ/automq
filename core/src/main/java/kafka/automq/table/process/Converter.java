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

package kafka.automq.table.process;

import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;

import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 * Interface for a component that converts raw Kafka records into a standardized
 * format, typically Avro GenericRecord.
 *
 * <p>Implementations handle different data formats (e.g., Avro, JSON, Protobuf)
 * and may interact with services like a Schema Registry.</p>
 */
public interface Converter {

    String CONVERSION_RECORD_NAME = "ConversionRecord";
    String KEY_FIELD_NAME = "key";
    String VALUE_FIELD_NAME = "value";
    String TIMESTAMP_FIELD_NAME = "timestamp";
    Schema BYTES = Schema.create(Schema.Type.BYTES);

    /**
     * Converts a raw Kafka record into a structured representation.
     *
     * <p>This method performs the core format conversion operation. It reads the
     * key, value and timestamp from the Kafka record and converts it to an Avro GenericRecord
     * that serves as the standardized internal representation for subsequent processing.</p>
     *
     * @param topic the name of the Kafka topic from which the record originated,
     *              used for topic-specific schema resolution and routing
     * @param record the Kafka record containing the byte[] payload to convert,
     *               must not be null
     * @return a {@code ConversionResult} containing the successfully converted data
     * @throws IllegalArgumentException if topic or record is null
     * @throws InvalidDataException if the input record data is invalid (e.g., null value)
     * @throws ConverterException if a non-data-related error occurs during conversion (e.g., schema resolution failure)
     */
    ConversionResult convert(String topic, Record record) throws ConverterException;

    /**
     * A utility to build a standard record that wraps a key, value and timestamp.
     *
     * @param key the key to wrap
     * @param keySchema the schema for the key
     * @param value the value to wrap
     * @param valueSchema the schema for the value
     * @param timestamp the timestamp of the record
     * @return a new GenericRecord wrapping the value
     */
    static GenericRecord buildConversionRecord(Object key, Schema keySchema, Object value, Schema valueSchema, long timestamp) {
        keySchema = keySchema == null ? BYTES : keySchema;
        valueSchema = valueSchema == null ? BYTES : valueSchema;
        Schema schema = SchemaBuilder.record(CONVERSION_RECORD_NAME)
            .fields()
            .name(KEY_FIELD_NAME).type(Schema.createUnion(Schema.create(Schema.Type.NULL), keySchema)).withDefault(null)
            .name(VALUE_FIELD_NAME).type(Schema.createUnion(Schema.create(Schema.Type.NULL), valueSchema)).withDefault(null)
            .name(TIMESTAMP_FIELD_NAME).type(Schema.create(Schema.Type.LONG)).withDefault(-1L)
            .endRecord();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(KEY_FIELD_NAME, key);
        builder.set(VALUE_FIELD_NAME, value);
        builder.set(TIMESTAMP_FIELD_NAME, timestamp);
        return builder.build();
    }
}
