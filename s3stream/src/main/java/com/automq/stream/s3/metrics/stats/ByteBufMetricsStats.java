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

import com.automq.stream.s3.metrics.Histogram;
import com.automq.stream.s3.metrics.S3StreamMetricsRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ByteBufMetricsStats {
    private static final Map<String, Histogram> SOURCE_TO_HISTOGRAM = new ConcurrentHashMap<>();

    public static Histogram getHistogram(String source) {
        return SOURCE_TO_HISTOGRAM.computeIfAbsent(source, k -> {
            Map<String, String> tags = Map.of("source", k);
            return S3StreamMetricsRegistry.getMetricsGroup().newHistogram("s3_stream_byte_buf_size", tags);
        });
    }
}
