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

import org.apache.kafka.common.record.Record;

import org.apache.avro.generic.GenericRecord;

import java.util.Objects;

/**
 * Result of format conversion operations.
 * Contains either a converted GenericRecord or error information.
 */
public final class ConversionResult {

    private final Record kafkaRecord;
    private final GenericRecord valueRecord;
    private final String schemaIdentity;

    /**
     * Creates a successful conversion result.
     *
     * @param kafkaRecord the original Kafka record
     * @param value the successfully converted Avro GenericRecord, must not be null
     * @param schemaIdentity unique identifier for the record's schema
     */
    public ConversionResult(Record kafkaRecord, GenericRecord value, String schemaIdentity) {
        this.kafkaRecord = Objects.requireNonNull(kafkaRecord);
        this.valueRecord = Objects.requireNonNull(value);
        this.schemaIdentity = schemaIdentity;
    }


    public Record getKafkaRecord() {
        return kafkaRecord;
    }

    /**
     * Returns the converted Avro GenericRecord.
     *
     * <p>This record represents the input data in standardized Avro format,
     * ready for processing by the Transform chain. The record's schema contains
     * all necessary type information for subsequent operations.</p>
     *
     * @return the converted GenericRecord, or null if conversion failed
     */
    public GenericRecord getValueRecord() {
        return valueRecord;
    }

    /**
     * Returns the schema identity for the converted record.
     *
     * <p>The schema identity is a unique identifier for the record's schema,
     * which can be used for schema evolution tracking, caching, and optimization
     * purposes. The format and meaning of this identifier depends on the specific
     * converter implementation.</p>
     *
     * @return the schema identity string, or null if conversion failed
     */
    public String getSchemaIdentity() {
        return schemaIdentity;
    }
}
