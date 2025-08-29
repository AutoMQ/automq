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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.Map;

public class KafkaRecordTransform implements Transform {

    // Define constants to avoid magic strings and ensure consistency.
    private static final String KAFKA_RECORD_NAMESPACE = "kafka.automq.table.process.transform";
    private static final String KAFKA_RECORD_NAME = "KafkaRecord";

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed for this transform.
    }

    @Override
    public GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException {
        final Record kafkaRecord = context.getKafkaRecord();
        final Schema schema = record.getSchema();

        Schema wrapperSchema = SchemaBuilder.record(KAFKA_RECORD_NAME)
            .namespace(KAFKA_RECORD_NAMESPACE)
            .fields()
            .name("timestamp").doc("Timestamp of the Kafka record").type().longType().noDefault()
            .name("key").doc("Key of the Kafka record").type().unionOf().nullType().and().bytesType().endUnion().nullDefault()
            .name("value").doc("Value of the Kafka record").type(schema).noDefault()
            .endRecord();

        return new GenericRecordBuilder(wrapperSchema)
            .set("timestamp", kafkaRecord.timestamp())
            .set("key", kafkaRecord.key())
            .set("value", record)
            .build();
    }
}
