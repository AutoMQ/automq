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

public class BenchmarkResult {
    private final String formatName;
    private final String dataTypeName;
    private final long durationNs;
    private final long recordsProcessed;
    private final long fieldCount;
    private final String errorMessage;

    private BenchmarkResult(String formatName, String dataTypeName, long durationNs, long recordsProcessed, long fieldCount,
                            String errorMessage) {
        this.formatName = formatName;
        this.dataTypeName = dataTypeName;
        this.durationNs = durationNs;
        this.recordsProcessed = recordsProcessed;
        this.fieldCount = fieldCount;
        this.errorMessage = errorMessage;
    }

    public static BenchmarkResult success(String formatName, String dataTypeName, long durationNs,
                                          long recordsProcessed, long fieldCount) {
        return new BenchmarkResult(formatName, dataTypeName, durationNs, recordsProcessed, fieldCount, null);
    }

    public static BenchmarkResult failure(String formatName, String dataTypeName, String errorMessage) {
        return new BenchmarkResult(formatName, dataTypeName, 0, 0, 0, errorMessage);
    }

    public String getFormatName() {
        return formatName;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }

    public long getDurationNs() {
        return durationNs;
    }

    public long getDurationMs() {
        return durationNs / 1_000_000L;
    }

    public long getRecordsProcessed() {
        return recordsProcessed;
    }

    public long getFieldCount() {
        return fieldCount;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isSuccess() {
        return errorMessage == null;
    }

    public long getThroughput() {
        long durationMs = getDurationMs();
        if (durationMs == 0) {
            return 0;
        }
        return (recordsProcessed * 1000L) / durationMs;
    }

    public double getNsPerField() {
        if (fieldCount == 0) {
            return 0.0d;
        }
        return (double) durationNs / (double) fieldCount;
    }

    public double getNsPerRecord() {
        if (recordsProcessed == 0) {
            return 0.0d;
        }
        return (double) durationNs / (double) recordsProcessed;
    }

    @Override
    public String toString() {
        if (isSuccess()) {
            return String.format("%s %s: %d ms, %d records, fieldCount=%d, ns/field=%.2f",
                formatName, dataTypeName, getDurationMs(), recordsProcessed, fieldCount, getNsPerField());
        } else {
            return String.format("%s %s: FAILED - %s", formatName, dataTypeName, errorMessage);
        }
    }
}
