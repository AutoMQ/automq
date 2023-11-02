/*
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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.Counter;
import com.automq.stream.s3.metrics.Histogram;
import com.automq.stream.s3.metrics.S3StreamMetricsRegistry;
import com.automq.stream.s3.metrics.operations.S3Operation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OperationMetricsStats {
    private static final Map<String, OperationMetrics> OPERATION_METRICS_MAP = new ConcurrentHashMap<>();

    public static OperationMetrics getOrCreateOperationMetrics(S3Operation s3Operation) {
        return OPERATION_METRICS_MAP.computeIfAbsent(s3Operation.getUniqueKey(), id ->
                new OperationMetrics(s3Operation));
    }

    public static class OperationMetrics {
        public final Counter operationCount;
        public final Histogram operationTime;

        public OperationMetrics(S3Operation s3Operation) {
            Map<String, String> tags = Map.of(
                    "operation", s3Operation.getName(),
                    "type", s3Operation.getType().getName());
            operationCount = S3StreamMetricsRegistry.getMetricsGroup().newCounter("operation_count" + Counter.SUFFIX, tags);
            operationTime = S3StreamMetricsRegistry.getMetricsGroup().newHistogram("operation_time", tags);
        }

    }
}
