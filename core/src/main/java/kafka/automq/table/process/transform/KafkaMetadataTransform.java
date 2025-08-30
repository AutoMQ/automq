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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaMetadataTransform implements Transform {

    private static final String KAFKA_METADATA_FIELD = "_kafka_metadata";
    private static final String PARTITION_FIELD = "partition";
    private static final String OFFSET_FIELD = "offset";
    private static final String TIMESTAMP_FIELD = "timestamp";

    private static final Schema KAFKA_METADATA_SCHEMA = SchemaBuilder.record(KAFKA_METADATA_FIELD)
        .fields()
        .name(PARTITION_FIELD).doc("Kafka partition").type().intType().noDefault()
        .name(OFFSET_FIELD).doc("Kafka offset").type().longType().noDefault()
        .name(TIMESTAMP_FIELD).doc("Kafka timestamp").type().longType().noDefault()
        .endRecord();

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed for this transform.
    }

    @Override
    public GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException {
        final Record kafkaRecord = context.getKafkaRecord();
        final Schema originalSchema = record.getSchema();

        List<Schema.Field> originalFields = new ArrayList<>();
        for (Schema.Field field : originalSchema.getFields()) {
            // Create a new Schema.Field instance for each original field
            originalFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()));
        }

        List<Schema.Field> enrichedFields = new ArrayList<>(originalFields);
        enrichedFields.add(new Schema.Field(KAFKA_METADATA_FIELD, KAFKA_METADATA_SCHEMA, "Kafka metadata", null));

        String enrichedName = originalSchema.getName() != null ?
            originalSchema.getName() + "_kafka_enriched" : "enriched_record";

        Schema enrichedSchema = Schema.createRecord(
            enrichedName,
            "Record with Kafka metadata",
            originalSchema.getNamespace(),
            false,
            enrichedFields
        );

        GenericData.Record enrichedRecord = new GenericData.Record(enrichedSchema);

        for (Schema.Field field : originalFields) {
            enrichedRecord.put(field.name(), record.get(field.name()));
        }

        GenericData.Record kafkaMetadataRecord = new GenericData.Record(KAFKA_METADATA_SCHEMA);
        kafkaMetadataRecord.put(PARTITION_FIELD, context.getPartition());
        kafkaMetadataRecord.put(OFFSET_FIELD, kafkaRecord.offset());
        kafkaMetadataRecord.put(TIMESTAMP_FIELD, kafkaRecord.timestamp());

        enrichedRecord.put(KAFKA_METADATA_FIELD, kafkaMetadataRecord);

        return enrichedRecord;
    }
}
