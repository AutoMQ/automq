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

import java.util.concurrent.TimeUnit;

public class BenchmarkResult {
    private final String formatName;
    private final String dataTypeName;
    private final long durationMs;
    private final long recordsProcessed;
    private final String errorMessage;

    private BenchmarkResult(String formatName, String dataTypeName, long durationMs, long recordsProcessed, String errorMessage) {
        this.formatName = formatName;
        this.dataTypeName = dataTypeName;
        this.durationMs = durationMs;
        this.recordsProcessed = recordsProcessed;
        this.errorMessage = errorMessage;
    }

    public static BenchmarkResult success(String formatName, String dataTypeName, long durationMs, long recordsProcessed) {
        return new BenchmarkResult(formatName, dataTypeName, durationMs, recordsProcessed, null);
    }

    public static BenchmarkResult failure(String formatName, String dataTypeName, String errorMessage) {
        return new BenchmarkResult(formatName, dataTypeName, 0, 0, errorMessage);
    }

    public String getFormatName() {
        return formatName;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public long getRecordsProcessed() {
        return recordsProcessed;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isSuccess() {
        return errorMessage == null;
    }

    public long getThroughput() {
        if (durationMs == 0) {
            return 0;
        }
        return TimeUnit.SECONDS.toMillis(recordsProcessed) / durationMs;
    }

    @Override
    public String toString() {
        if (isSuccess()) {
            return String.format("%s %s: %d ms, %d records, %d records/sec",
                formatName, dataTypeName, durationMs, recordsProcessed, getThroughput());
        } else {
            return String.format("%s %s: FAILED - %s", formatName, dataTypeName, errorMessage);
        }
    }
}
