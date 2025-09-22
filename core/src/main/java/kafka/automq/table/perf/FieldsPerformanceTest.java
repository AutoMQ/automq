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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldsPerformanceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldsPerformanceTest.class);

    public static void main(String[] args) {
        PerfConfig config = new PerfConfig();

        Map<String, List<Pair<String, Long>>> results = new HashMap<>();
        results.put("avro", new ArrayList<>());
        results.put("proto", new ArrayList<>());

        LOGGER.info("Starting performance tests with {} records per test", config.getRecordsCount());
        LOGGER.info("Enabled data types: {}", config.getEnabledDataTypes());
        LOGGER.info("Enabled formats: {}", config.getEnabledFormats());

        for (DataType dataType : config.getEnabledDataTypes()) {
            for (SerializationFormat format : config.getEnabledFormats()) {

                PerfTestCase testCase = createTestCase(dataType, format, config);

                try {
                    PerfTestCase.clearInMemoryFiles();

                    LOGGER.info("Running benchmark: {} {}", format.getName(), dataType.getName());
                    BenchmarkResult result = testCase.runBenchmark(config.getRecordsCount());

                    if (result.isSuccess()) {
                        // Unify metric to duration (ms), consistent with original FieldsPerf "task cost"
                        results.get(format.getName()).add(Pair.of(dataType.getName(), result.getDurationMs()));
                        LOGGER.info("Completed: {} {} - {} ms",
                            format.getName(), dataType.getName(), result.getDurationMs());
                    } else {
                        LOGGER.error("Failed: {} {} - {}",
                            format.getName(), dataType.getName(), result.getErrorMessage());
                    }

                } catch (Exception e) {
                    LOGGER.error("Failed: {} {} - {}", format.getName(), dataType.getName(), e.getMessage(), e);
                }
            }
        }

        // Output results in the same format as original
        results.forEach((format, formatResults) -> {
            LOGGER.info("type: {}", format);
            LOGGER.info("task cost: {}", formatResults);
        });
    }

    private static PerfTestCase createTestCase(DataType dataType, SerializationFormat format, PerfConfig config) {
        return switch (format) {
            case AVRO -> dataType.createAvroTestCase(config.getFieldCountPerRecord(), config.getPayloadsCount(), config);
            case PROTOBUF -> dataType.createProtobufTestCase(config.getFieldCountPerRecord(), config.getPayloadsCount(), config);
        };
    }
}
