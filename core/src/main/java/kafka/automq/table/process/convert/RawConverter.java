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

import kafka.automq.table.process.ConversionResult;
import kafka.automq.table.process.Converter;
import kafka.automq.table.process.exception.ConverterException;

import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class RawConverter implements Converter {
    private static final Schema SCHEMA = SchemaBuilder.builder().bytesType();
    private static final String SCHEMA_IDENTITY = String.valueOf(SCHEMA.hashCode());

    @Override
    public ConversionResult convert(String topic, Record record) throws ConverterException {
        if (record.value() == null) {
            throw new ConverterException("Kafka record value cannot be null");
        }
        return new ConversionResult(record, Converter.buildValueRecord(record.value(), SCHEMA), SCHEMA_IDENTITY);
    }
}
