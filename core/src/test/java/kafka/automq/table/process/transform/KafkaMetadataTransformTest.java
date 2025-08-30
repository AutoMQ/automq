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

import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class KafkaMetadataTransformTest {

    private KafkaMetadataTransform transform;
    private TransformContext context;
    private Record kafkaRecord;

    private static final int NUM_ITERATIONS = 100000;
    private static final int NUM_ROUNDS = 10;

    @BeforeEach
    void setUp() {
        transform = new KafkaMetadataTransform();
        context = mock(TransformContext.class);
        kafkaRecord = mock(Record.class);
        when(context.getKafkaRecord()).thenReturn(kafkaRecord);

        // Mock context and kafka record for performance test
        int partition = 10;
        long offset = 987654321L;
        long timestamp = System.currentTimeMillis();
        when(context.getPartition()).thenReturn(partition);
        when(kafkaRecord.offset()).thenReturn(offset);
        when(kafkaRecord.timestamp()).thenReturn(timestamp);
    }

    @Test
    void applyRecord() throws TransformException {
        // Setup: Input schema with multiple fields
        Schema originalSchema = SchemaBuilder.record("OriginalRecord")
            .namespace("test.namespace")
            .fields()
            .name("id").type().intType().noDefault()
            .name("name").type().stringType().noDefault()
            .endRecord();

        // Create an original record
        GenericRecord originalRecord = new GenericData.Record(originalSchema);
        originalRecord.put("id", 123);
        originalRecord.put("name", "test-name");

        // Mock context and kafka record
        int partition = 1;
        long offset = 456L;
        long timestamp = 789L;
        when(context.getPartition()).thenReturn(partition);
        when(kafkaRecord.offset()).thenReturn(offset);
        when(kafkaRecord.timestamp()).thenReturn(timestamp);

        // Apply the transform
        GenericRecord enrichedRecord = transform.apply(originalRecord, context);

        // Assertions
        assertNotNull(enrichedRecord);

        // Check schema
        Schema enrichedSchema = enrichedRecord.getSchema();
        assertEquals("OriginalRecord_kafka_enriched", enrichedSchema.getName());
        assertEquals("test.namespace", enrichedSchema.getNamespace());
        assertNotNull(enrichedSchema.getField("id"));
        assertNotNull(enrichedSchema.getField("name"));
        assertNotNull(enrichedSchema.getField("_kafka_metadata"));

        // Check original values
        assertEquals(123, enrichedRecord.get("id"));
        assertEquals("test-name", enrichedRecord.get("name").toString());

        // Check kafka metadata
        GenericRecord kafkaMetadata = (GenericRecord) enrichedRecord.get("_kafka_metadata");
        assertNotNull(kafkaMetadata);
        assertEquals(partition, kafkaMetadata.get("partition"));
        assertEquals(offset, kafkaMetadata.get("offset"));
        assertEquals(timestamp, kafkaMetadata.get("timestamp"));
    }

    @Test
    void performanceTest() throws TransformException {
        System.out.println("=== KafkaMetadataTransform Performance Test ===");

        // Setup: Input schema and record for performance test
        Schema originalSchema = SchemaBuilder.record("PerfTestRecord")
            .namespace("perf.test.namespace")
            .fields()
            .name("field1").type().stringType().noDefault()
            .name("field2").type().intType().noDefault()
            .endRecord();

        GenericRecord originalRecord = new GenericData.Record(originalSchema);
        originalRecord.put("field1", "some-string-value");
        originalRecord.put("field2", 12345);

        // Warmup phase
        System.out.println("Starting warmup...");
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            transform.apply(originalRecord, context);
        }
        System.out.println("Warmup finished.");

        // Performance measurement rounds
        long totalTimeNanos = 0;
        for (int round = 0; round < NUM_ROUNDS; round++) {
            long startTime = System.nanoTime();
            for (int i = 0; i < NUM_ITERATIONS; i++) {
                transform.apply(originalRecord, context);
            }
            long endTime = System.nanoTime();
            long roundTimeNanos = endTime - startTime;
            totalTimeNanos += roundTimeNanos;
            System.out.printf("Round %d: %d iterations took %d ns (%f ms)%n",
                round + 1, NUM_ITERATIONS, roundTimeNanos, roundTimeNanos / 1_000_000.0);
        }

        long avgTimePerIterationNanos = totalTimeNanos / (NUM_ITERATIONS * NUM_ROUNDS);
        System.out.printf("Total %d iterations across %d rounds: %d ns (%f ms)%n",
            NUM_ITERATIONS * NUM_ROUNDS, NUM_ROUNDS, totalTimeNanos, totalTimeNanos / 1_000_000.0);
        System.out.printf("Average time per transformation: %d ns%n", avgTimePerIterationNanos);
    }
}
