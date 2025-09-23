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

import com.automq.stream.utils.Systems;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PerfConfig {
    private final long recordsCount;
    private final int fieldCountPerRecord;
    private final int payloadsCount;
    private final int batchSizeBytes;
    private final Set<DataType> enabledDataTypes;
    private final Set<SerializationFormat> enabledFormats;

    public PerfConfig() {
        this.recordsCount = Systems.getEnvLong("RECORDS_COUNT", 10_000_000L);
        this.fieldCountPerRecord = parseIntEnv("FIELD_COUNT_PER_RECORD", 32);
        this.payloadsCount = parseIntEnv("PAYLOADS_COUNT", 1000);
        this.batchSizeBytes = parseIntEnv("BATCH_SIZE_BYTES", 32 * 1024 * 1024);
        this.enabledDataTypes = parseDataTypes(System.getenv("TASKS"));
        this.enabledFormats = parseFormats(System.getenv("FORMAT_TYPES"));
    }

    private int parseIntEnv(String envName, int defaultValue) {
        String value = System.getenv(envName);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private Set<DataType> parseDataTypes(String tasksStr) {
        if (StringUtils.isBlank(tasksStr)) {
            return EnumSet.allOf(DataType.class);
        }
        return Arrays.stream(tasksStr.split(","))
            .map(String::trim)
            .map(String::toLowerCase)
            .map(DataType::fromString)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    private Set<SerializationFormat> parseFormats(String formatsStr) {
        if (StringUtils.isBlank(formatsStr)) {
            return EnumSet.of(SerializationFormat.AVRO);
        }
        return Arrays.stream(formatsStr.split(","))
            .map(String::trim)
            .map(String::toLowerCase)
            .map(SerializationFormat::fromString)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    public long getRecordsCount() {
        return recordsCount;
    }
    public int getFieldCountPerRecord() {
        return fieldCountPerRecord;
    }
    public int getPayloadsCount() {
        return payloadsCount;
    }
    public int getBatchSizeBytes() {
        return batchSizeBytes;
    }
    public Set<DataType> getEnabledDataTypes() {
        return enabledDataTypes;
    }
    public Set<SerializationFormat> getEnabledFormats() {
        return enabledFormats;
    }
}
