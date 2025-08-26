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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.Record;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for schema registry-based converters.
 *
 * <p>This class provides common functionality for converters that rely on
 * external schema registries (such as Confluent Schema Registry) for schema
 * resolution and management. It handles the integration with the schema registry
 * client and provides a framework for format-specific conversion implementations.</p>
 *
 *
 */
public abstract class RegistrySchemaConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(RegistrySchemaConverter.class);


    @Override
    public final ConversionResult convert(String topic, Record record) throws ConverterException {
        try {
            log.debug("Converting record for topic: {}", topic);

            int schemaId = getSchemaId(topic, record);
            GenericRecord convertedRecord = performConversion(topic, record);

            if (convertedRecord != null) {
                log.debug("Successfully converted record with schema: {}",
                         convertedRecord.getSchema().getName());
                return new ConversionResult(record, Converter.buildValueRecord(convertedRecord), String.valueOf(schemaId));
            } else {
                throw new ConverterException("Conversion returned null record for topic: " + topic);
            }

        } catch (SerializationException e) {
            throw new ConverterException("Conversion failed for topic: " + topic + " due to serialization/schema error", e);
        } catch (Exception e) {
            throw new ConverterException("Conversion failed for topic: " + topic + " with an unexpected error", e);
        }
    }

    protected abstract GenericRecord performConversion(String topic, Record record);


    protected abstract int getSchemaId(String topic, Record record);

}
