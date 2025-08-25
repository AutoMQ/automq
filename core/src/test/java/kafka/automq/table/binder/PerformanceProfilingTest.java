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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.ThreadLocalRandom;

/**
 * Detailed performance profiling test to identify the root cause of performance issues.
 * Each test runs for approximately 5 minutes with a large number of records.
 */
public class PerformanceProfilingTest {

    private static final int TEST_DURATION_MINUTES = 1;
    private static final long TEST_DURATION_NANOS = TEST_DURATION_MINUTES * 60L * 1_000_000_000L;

    private Schema avroSchema;
    private org.apache.iceberg.Schema icebergSchema;
    private List<GenericRecord> testRecords;

    @BeforeEach
    void setUp() {
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
            "    },\n" +
            "    {\"name\": \"salary\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
            "    {\"name\": \"department\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
            "  ]\n" +
            "}";

        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        // Create a large dataset for intensive testing (1 million records)
        testRecords = createTestRecords(1_000_000);

        System.out.println("=== Performance Profiling Test Setup ===");
        System.out.println("Test Records: " + testRecords.size());
        System.out.println("Test Duration: " + TEST_DURATION_MINUTES + " minutes");
        System.out.println("Schema Fields: " + avroSchema.getFields().size());
        System.out.println("=========================================");
    }

    @Test
    public void testRecordBinderLongRunning() {
        System.out.println("\n=== Starting RecordBinder Long-Running Test ===");

        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        // Warmup
        System.out.println("Warming up RecordBinder...");
        performWarmup(recordBinder, null);

        System.out.println("Starting 5-minute RecordBinder performance test...");
        long startTime = System.nanoTime();
        long recordsProcessed = 0;
        long fieldAccessesProcessed = 0;
        long totalConversionTime = 0;
        long totalAccessTime = 0;

        while ((System.nanoTime() - startTime) < TEST_DURATION_NANOS) {
            GenericRecord record = getRandomRecord();

            // Measure conversion time
            long conversionStart = System.nanoTime();
            Record icebergRecord = recordBinder.bind(record);
            long conversionEnd = System.nanoTime();
            totalConversionTime += conversionEnd - conversionStart;

            // Measure field access time
            long accessStart = System.nanoTime();
            int fieldsAccessed = accessAllFields(icebergRecord);
            long accessEnd = System.nanoTime();
            totalAccessTime += accessEnd - accessStart;

            recordsProcessed++;
            fieldAccessesProcessed += fieldsAccessed;

            // Print progress every 100,000 records
            if (recordsProcessed % 100_000 == 0) {
                long elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000L;
                System.out.printf("RecordBinder Progress: %d records, %d seconds elapsed\n",
                    recordsProcessed, elapsedSeconds);
            }
        }

        long totalTime = System.nanoTime() - startTime;
        printDetailedResults("RecordBinder", recordsProcessed, fieldAccessesProcessed,
            totalTime, totalConversionTime, totalAccessTime);
    }

    @Test
    public void testVisitorLongRunning() {
        System.out.println("\n=== Starting Visitor Long-Running Test ===");

        AvroToIcebergVisitor visitor = new AvroToIcebergVisitor(icebergSchema);

        // Warmup
        System.out.println("Warming up Visitor...");
        performWarmup(null, visitor);

        System.out.println("Starting 5-minute Visitor performance test...");
        long startTime = System.nanoTime();
        long recordsProcessed = 0;
        long fieldAccessesProcessed = 0;
        long totalConversionTime = 0;
        long totalAccessTime = 0;

        while ((System.nanoTime() - startTime) < TEST_DURATION_NANOS) {
            GenericRecord record = getRandomRecord();

            // Measure conversion time
            long conversionStart = System.nanoTime();
            Record icebergRecord = visitor.convertRecord(record);
            long conversionEnd = System.nanoTime();
            totalConversionTime += conversionEnd - conversionStart;

            // Measure field access time
            long accessStart = System.nanoTime();
            int fieldsAccessed = accessAllFields(icebergRecord);
            long accessEnd = System.nanoTime();
            totalAccessTime += accessEnd - accessStart;

            recordsProcessed++;
            fieldAccessesProcessed += fieldsAccessed;

            // Print progress every 100,000 records
            if (recordsProcessed % 100_000 == 0) {
                long elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000L;
                System.out.printf("Visitor Progress: %d records, %d seconds elapsed\n",
                    recordsProcessed, elapsedSeconds);
            }
        }

        long totalTime = System.nanoTime() - startTime;
        printDetailedResults("Visitor", recordsProcessed, fieldAccessesProcessed,
            totalTime, totalConversionTime, totalAccessTime);
    }

    // --- Helper Methods ---

    private List<GenericRecord> createTestRecords(int count) {
        System.out.println("Creating " + count + " test records...");
        List<GenericRecord> records = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            GenericRecord record = new GenericData.Record(avroSchema);
            record.put("id", (long) i);
            record.put("name", "User" + i);
            record.put("active", i % 2 == 0);
            record.put("score", ThreadLocalRandom.current().nextDouble(50.0, 100.0));

            LocalDate birthDate = LocalDate.of(1970 + (i % 50), 1 + (i % 12), 1 + (i % 28));
            record.put("birthDate", (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), birthDate));

            record.put("tags", Arrays.asList("tag" + (i % 100), "category" + (i % 20), "type" + (i % 10)));

            Map<String, String> metadata = new HashMap<>();
            metadata.put("source", "test-batch-" + (i / 1000));
            metadata.put("batch", String.valueOf(i / 10000));
            metadata.put("region", "region-" + (i % 5));
            record.put("metadata", metadata);

            GenericRecord address = new GenericData.Record(avroSchema.getField("address").schema());
            address.put("street", (i % 9999 + 1) + " Main St");
            address.put("city", "City" + (i % 100));
            address.put("zipCode", 10000 + (i % 90000));
            record.put("address", address);

            // Optional fields
            record.put("salary", i % 3 == 0 ? null : ThreadLocalRandom.current().nextDouble(30000, 150000));
            record.put("department", i % 5 == 0 ? null : "Dept" + (i % 20));

            records.add(serializeAndDeserialize(record, avroSchema));

            if (i > 0 && i % 100_000 == 0) {
                System.out.println("Created " + i + " records...");
            }
        }

        System.out.println("Finished creating " + count + " test records.");
        return records;
    }

    private GenericRecord serializeAndDeserialize(GenericRecord record, Schema schema) {
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
            throw new RuntimeException("Failed to serialize/deserialize record", e);
        }
    }

    private GenericRecord getRandomRecord() {
        int index = ThreadLocalRandom.current().nextInt(testRecords.size());
        return testRecords.get(index);
    }

    private void performWarmup(RecordBinder recordBinder, AvroToIcebergVisitor visitor) {
        int warmupIterations = 50_000;
        System.out.println("Performing " + warmupIterations + " warmup iterations...");

        for (int i = 0; i < warmupIterations; i++) {
            GenericRecord record = getRandomRecord();

            if (recordBinder != null) {
                Record icebergRecord = recordBinder.bind(record);
                accessAllFields(icebergRecord);
            }

            if (visitor != null) {
                Record icebergRecord = visitor.convertRecord(record);
                accessAllFields(icebergRecord);
            }
        }
        System.out.println("Warmup completed.");
    }

    private int accessAllFields(Record record) {
        int fieldsAccessed = 0;

        // Access primitive fields
        record.getField("id");
        fieldsAccessed++;

        record.getField("name");
        fieldsAccessed++;

        record.getField("active");
        fieldsAccessed++;

        record.getField("score");
        fieldsAccessed++;

        record.getField("birthDate");
        fieldsAccessed++;

        // Access collection fields
        record.getField("tags");
        fieldsAccessed++;

        record.getField("metadata");
        fieldsAccessed++;

        // Access nested record
        Record address = (Record) record.getField("address");
        fieldsAccessed++;
        if (address != null) {
            address.getField("street");
            fieldsAccessed++;
            address.getField("city");
            fieldsAccessed++;
            address.getField("zipCode");
            fieldsAccessed++;
        }

        // Access optional fields
        record.getField("salary");
        fieldsAccessed++;

        record.getField("department");
        fieldsAccessed++;

        return fieldsAccessed;
    }

    private void printDetailedResults(String testName, long recordsProcessed, long fieldAccessesProcessed,
                                     long totalTimeNanos, long conversionTimeNanos, long accessTimeNanos) {

        long totalTimeMs = totalTimeNanos / 1_000_000;
        long conversionTimeMs = conversionTimeNanos / 1_000_000;
        long accessTimeMs = accessTimeNanos / 1_000_000;

        double recordsPerSecond = (double) recordsProcessed / (totalTimeNanos / 1_000_000_000.0);
        double fieldAccessesPerSecond = (double) fieldAccessesProcessed / (totalTimeNanos / 1_000_000_000.0);

        long avgConversionTimeNs = conversionTimeNanos / recordsProcessed;
        long avgAccessTimeNs = accessTimeNanos / recordsProcessed;
        long avgFieldAccessTimeNs = accessTimeNanos / fieldAccessesProcessed;

        System.out.println("\n" + "=".repeat(60));
        System.out.println(testName + " Performance Results");
        System.out.println("=".repeat(60));
        System.out.printf("Total Runtime:           %,d ms (%.1f minutes)\n", totalTimeMs, totalTimeMs / 60000.0);
        System.out.printf("Records Processed:       %,d\n", recordsProcessed);
        System.out.printf("Field Accesses:          %,d\n", fieldAccessesProcessed);
        System.out.println();
        System.out.printf("Records/Second:          %,.1f\n", recordsPerSecond);
        System.out.printf("Field Accesses/Second:   %,.1f\n", fieldAccessesPerSecond);
        System.out.println();
        System.out.printf("Avg Conversion Time:     %,d ns/record\n", avgConversionTimeNs);
        System.out.printf("Avg Access Time:         %,d ns/record\n", avgAccessTimeNs);
        System.out.printf("Avg Field Access Time:   %,d ns/field\n", avgFieldAccessTimeNs);
        System.out.println();
        System.out.printf("Time Breakdown:\n");
        System.out.printf("  Conversion: %,d ms (%.1f%%)\n", conversionTimeMs,
            100.0 * conversionTimeNanos / totalTimeNanos);
        System.out.printf("  Access:     %,d ms (%.1f%%)\n", accessTimeMs,
            100.0 * accessTimeNanos / totalTimeNanos);
        System.out.printf("  Overhead:   %,d ms (%.1f%%)\n",
            totalTimeMs - conversionTimeMs - accessTimeMs,
            100.0 * (totalTimeNanos - conversionTimeNanos - accessTimeNanos) / totalTimeNanos);
        System.out.println("=".repeat(60));
    }
}
