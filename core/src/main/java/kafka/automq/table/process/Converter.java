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
 *
 * <p>Converters are stateful and must be closed to release resources.</p>
 */
public interface Converter {

    String VALUE_FIELD_NAME = "value";
    String RECORD_NAME = "ValueRecord";

    /**
     * Converts a raw Kafka record into a structured representation.
     *
     * <p>This method performs the core format conversion operation. It reads the
     * byte[] payload from the Kafka record and converts it to an Avro GenericRecord
     * that serves as the standardized internal representation for subsequent processing.</p>
     *
     * @param topic the name of the Kafka topic from which the record originated,
     *              used for topic-specific schema resolution and routing
     * @param record the Kafka record containing the byte[] payload to convert,
     *               must not be null
     * @return a ConversionResult containing either the successfully converted
     *         GenericRecord or detailed error information
     * @throws IllegalArgumentException if topic or record is null
     */
    ConversionResult convert(String topic, Record record) throws ConverterException;


    static GenericRecord buildValueRecord(GenericRecord valueRecord) {
        return buildValueRecord(valueRecord, valueRecord.getSchema());
    }
    /**
     * A utility to build a standard record that wraps a value.
     * The created record will have a single field named "value".
     *
     * @param value the value to wrap
     * @param valueSchema the schema for the value
     * @return a new GenericRecord wrapping the value
     */
    static GenericRecord buildValueRecord(Object value, Schema valueSchema) {
        Schema schema = SchemaBuilder.record(RECORD_NAME)
            .fields()
            .name(VALUE_FIELD_NAME).type(valueSchema).noDefault()
            .endRecord();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(VALUE_FIELD_NAME, value);
        return builder.build();
    }
}
