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
    private static final Map<String, Counter> OPERATION_COUNTER_MAP = new ConcurrentHashMap<>();
    private static final Map<String, Histogram> OPERATION_HIST_MAP = new ConcurrentHashMap<>();

    public static Counter getCounter(S3Operation s3Operation) {
        return getOrCreateCounterMetrics(s3Operation);
    }

    public static Histogram getHistogram(S3Operation s3Operation) {
        return getOrCreateHistMetrics(s3Operation);
    }

    private static Counter getOrCreateCounterMetrics(S3Operation s3Operation) {
        return OPERATION_COUNTER_MAP.computeIfAbsent(s3Operation.getUniqueKey(), id -> S3StreamMetricsRegistry.getMetricsGroup()
                .newCounter("operation_count" + Counter.SUFFIX, tags(s3Operation)));
    }

    private static Histogram getOrCreateHistMetrics(S3Operation s3Operation) {
        return OPERATION_HIST_MAP.computeIfAbsent(s3Operation.getUniqueKey(), id -> S3StreamMetricsRegistry.getMetricsGroup()
                .newHistogram("operation_time", tags(s3Operation)));
    }

    private static Map<String, String> tags(S3Operation s3Operation) {
        return Map.of(
                "operation", s3Operation.getName(),
                "op_type", s3Operation.getType().getName());
    }
}
