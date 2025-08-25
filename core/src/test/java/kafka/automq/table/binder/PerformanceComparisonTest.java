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
package kafka.automq.table.binder;

import kafka.automq.table.worker.convert.AvroToIcebergVisitor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performance comparison test between the AvroRecordWrapper
 * and the traditional AvroToIcebergVisitor, focusing on CPU-bound operations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PerformanceComparisonTest {

    private static Schema avroSchema;
    private static org.apache.iceberg.Schema icebergSchema;
    private static List<GenericRecord> testRecords;

    @BeforeAll
    static void setUp() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"TestRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"long\"},\n" +
            "    {\"name\": \"name\", \"type\": \"string\"},\n" +
            "    {\"name\": \"active\", \"type\": \"boolean\"},\n" +
            "    {\"name\": \"score\", \"type\": \"double\"},\n" +
            "    {\"name\": \"birthDate\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},\n" +
            "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n" +
            "    {\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n" +
            "    {\n" +
            "      \"name\": \"address\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"Address\",\n" +
            "        \"fields\": [\n" +
            "          {\"name\": \"street\", \"type\": \"string\"},\n" +
            "          {\"name\": \"city\", \"type\": \"string\"},\n" +
            "          {\"name\": \"zipCode\", \"type\": \"int\"}\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        testRecords = createTestRecords(1000000); // 1,000,000 records for performance testing
    }

    @Test
    @Order(1)
    public void testPerformanceAndReusability() {
        System.out.println("=== Performance Comparison: RecordBinder vs Visitor ===");
        int iterations = 10;
        int recordCount = testRecords.size() * iterations;
        System.out.printf("iterations: %d; each recordCount %d \n", iterations, recordCount);

        // --- 1. Visitor (Traditional Approach) ---
        AvroToIcebergVisitor visitor = new AvroToIcebergVisitor(icebergSchema);
        warmup(visitor);
        long visitorTime = measurePerformance(() -> {
            for (GenericRecord record : testRecords) {
                accessAllFields(visitor.convertRecord(record));
            }
        }, iterations);
        printResults("Visitor", recordCount, visitorTime);

        // --- 2. RecordBinder ---
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);
        warmup(recordBinder);
        long reusableWrapperTime = measurePerformance(() -> {
            for (GenericRecord record : testRecords) {
                accessAllFields(recordBinder.bind(record));
            }
        }, iterations);
        printResults("RecordBinder", recordCount, reusableWrapperTime);


        System.out.println("\nPerformance Improvement (RecordBinder vs. Visitor): " + String.format("%.2fx", (double) visitorTime / reusableWrapperTime));
    }

    // --- Helper Methods ---

    private static List<GenericRecord> createTestRecords(int count) {
        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            GenericRecord record = new GenericData.Record(avroSchema);
            record.put("id", (long) i);
            record.put("name", "User" + i);
            record.put("active", i % 2 == 0);
            record.put("score", i * 1.5);
            LocalDate birthDate = LocalDate.of(1990 + (i % 30), 1 + (i % 12), 1 + (i % 28));
            record.put("birthDate", (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), birthDate));
            record.put("tags", Arrays.asList("tag" + i, "category" + (i % 10)));
            Map<String, String> metadata = new HashMap<>();
            metadata.put("source", "test");
            metadata.put("batch", String.valueOf(i / 100));
            record.put("metadata", metadata);
            GenericRecord address = new GenericData.Record(avroSchema.getField("address").schema());
            address.put("street", i + " Main St");
            address.put("city", "City" + (i % 50));
            address.put("zipCode", 100000 + i);
            record.put("address", address);
            records.add(serializeAndDeserialize(record, avroSchema));
        }
        return records;
    }

    private static GenericRecord serializeAndDeserialize(GenericRecord record, Schema schema) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(record, encoder);
            encoder.flush();
            outputStream.close();
            byte[] serializedBytes = outputStream.toByteArray();
            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedBytes);
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void warmup(RecordBinder recordBinder) {
        for (int i = 0; i < 10000; i++) {
            accessAllFields(recordBinder.bind(testRecords.get(i % testRecords.size())));
        }
    }

    private void warmup(AvroToIcebergVisitor visitor) {
        for (int i = 0; i < 10000; i++) {
            accessAllFields(visitor.convertRecord(testRecords.get(i % testRecords.size())));
        }
    }

    private long measurePerformance(Runnable task, int iterations) {
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            task.run();
        }
        return System.nanoTime() - startTime;
    }

    private void printResults(String testName, int recordCount, long totalTimeNanos) {
        long totalFieldAccesses = recordCount * 10L; // 10 fields accessed per record
        System.out.printf("%-25s | Total Time: %5d ms | Avg per Record: %5d ns | Avg per Field: %4d ns\n",
            testName,
            totalTimeNanos / 1_000_000,
            totalTimeNanos / recordCount,
            totalTimeNanos / totalFieldAccesses);
    }

    private void accessAllFields(Record record) {
        record.getField("id");
        record.getField("name");
        record.getField("active");
        record.getField("score");
        record.getField("birthDate");
        record.getField("tags");
        record.getField("metadata");
        Record address = (Record) record.getField("address");
        if (address != null) {
            address.getField("street");
            address.getField("city");
            address.getField("zipCode");
        }
    }
}
