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

package kafka.automq.table.perf;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FieldsPerformanceTest {

    public static void main(String[] args) {
        PerfConfig config = new PerfConfig();

        Map<String, List<BenchmarkResult>> results = new HashMap<>();
        results.put("avro", new ArrayList<>());
        results.put("proto", new ArrayList<>());

        System.out.printf("Starting performance tests with %d records per test%n", config.getRecordsCount());
        System.out.printf("Enabled data types: %s%n", config.getEnabledDataTypes());
        System.out.printf("Enabled formats: %s%n", config.getEnabledFormats());

        for (DataType dataType : config.getEnabledDataTypes()) {
            for (SerializationFormat format : config.getEnabledFormats()) {

                PerfTestCase testCase = createTestCase(dataType, format, config);

                try {
                    PerfTestCase.clearInMemoryFiles();

                    System.out.printf("Running benchmark: %s %s%n", format.getName(), dataType.getName());
                    BenchmarkResult result = testCase.runBenchmark(config.getRecordsCount());

                    if (result.isSuccess()) {
                        results.get(format.getName()).add(result);
                        System.out.printf(
                            "Completed: %s %s - %d ms, fieldCount=%d, ns/field=%s%n",
                            format.getName(),
                            dataType.getName(),
                            result.getDurationMs(),
                            result.getFieldCount(),
                            String.format(java.util.Locale.ROOT, "%.2f", result.getNsPerField()));
                    } else {
                        System.err.printf("Failed: %s %s - %s%n",
                            format.getName(), dataType.getName(), result.getErrorMessage());
                    }

                } catch (Exception e) {
                    System.err.printf("Failed: %s %s - %s%n", format.getName(), dataType.getName(), e.getMessage());
                    e.printStackTrace(System.err);
                }
            }
        }

        // Output results in the same format as original
        results.forEach((format, formatResults) -> {
            System.out.printf("type: %s%n", format);
            List<Pair<String, Long>> durations = formatResults.stream()
                .map(r -> Pair.of(r.getDataTypeName(), r.getDurationMs()))
                .collect(Collectors.toList());
            System.out.printf("task cost: %s%n", durations);
            formatResults.forEach(r -> System.out.printf(
                "detail: %s %s -> records=%d, fieldCount=%d, ns/field=%s, ns/record=%s%n",
                format,
                r.getDataTypeName(),
                r.getRecordsProcessed(),
                r.getFieldCount(),
                String.format(java.util.Locale.ROOT, "%.2f", r.getNsPerField()),
                String.format(java.util.Locale.ROOT, "%.2f", r.getNsPerRecord())));
        });
    }

    private static PerfTestCase createTestCase(DataType dataType, SerializationFormat format, PerfConfig config) {
        return switch (format) {
            case AVRO -> dataType.createAvroTestCase(config.getFieldCountPerRecord(), config.getPayloadsCount(), config);
            case PROTOBUF -> dataType.createProtobufTestCase(config.getFieldCountPerRecord(), config.getPayloadsCount(), config);
        };
    }
}
