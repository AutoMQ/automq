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

import kafka.automq.table.worker.convert.AvroToIcebergVisitor;
import kafka.automq.table.worker.convert.IcebergRecordConverter;

import org.apache.kafka.common.errors.SerializationException;

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import software.amazon.awssdk.http.HttpStatusCode;

public class RegistrySchemaAvroConverter implements Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegistrySchemaAvroConverter.class);
    // Source: io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe#MAGIC_BYTE
    private static final byte MAGIC_BYTE = 0x0;
    private final String topic;
    private final KafkaRecordConvert<GenericRecord> recordConvert;

    private IcebergRecordConverter<GenericRecord> icebergRecordConverter;
    private int currentSchemaId = -1;
    private Schema tableSchema = null;
    private Schema sampleSchema = null;

    public RegistrySchemaAvroConverter(KafkaRecordConvert<GenericRecord> recordConvert, String topic) {
        this.topic = topic;
        this.recordConvert = recordConvert;
    }

    @Override
    public Record convert(org.apache.kafka.common.record.Record record) {
        int schemaId = getSchemaId(record);
        GenericRecord value;
        try {
            value = recordConvert.convert(topic, record, schemaId);
        } catch (SerializationException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RestClientException) {
                RestClientException restClientException = (RestClientException) cause;
                if (restClientException.getStatus() == HttpStatusCode.NOT_FOUND
                    // https://docs.confluent.io/platform/current/schema-registry/develop/api.html
                    // Error code 40401 – Subject not found
                    // Error code 40402 – Version not found
                    // Error code 40403 – Schema not found
                    || restClientException.getStatus() / 100 == HttpStatusCode.NOT_FOUND) {
                    throw new InvalidDataException(ex);
                }
            }
            throw ex;
        }
        if (value != null) {
            // initialize the converter if the schema has changed
            initAndCheckSchemaUpdate(schemaId, value);
            return icebergRecordConverter.convertRecord(value);
        } else {
            throw new InvalidDataException("Failed to deserialize record");
        }
    }

    @Override
    public void tableSchema(Schema tableSchema) {
        this.tableSchema = tableSchema;
        // update the schema and recordConverter if the table is configured
        this.icebergRecordConverter = new AvroToIcebergVisitor(tableSchema);
    }

    @Override
    public SchemaState currentSchema() {
        Schema currentSchema = tableSchema != null ? tableSchema : sampleSchema;
        return new SchemaState(currentSchema, tableSchema != null);
    }

    @Override
    public long fieldCount() {
        return this.icebergRecordConverter == null ? 0 : this.icebergRecordConverter.fieldCount();
    }

    private void initAndCheckSchemaUpdate(int schemaId, org.apache.avro.generic.GenericRecord record) {
        // update the schema and recordConverter if the schemaId has changed
        if (currentSchemaId < schemaId) {
            sampleSchema = AvroSchemaUtil.toIceberg(record.getSchema());
            currentSchemaId = schemaId;
            tableSchema = null;
            icebergRecordConverter = new AvroToIcebergVisitor(sampleSchema);
        }
    }

    // io.confluent.kafka.serializers.DeserializationContext#constructor
    private int getSchemaId(org.apache.kafka.common.record.Record record) {
        ByteBuffer buffer = record.value().duplicate();
        if (buffer.get() != MAGIC_BYTE) {
            throw new InvalidDataException("Unknown magic byte!");
        }
        return buffer.getInt();
    }
}
