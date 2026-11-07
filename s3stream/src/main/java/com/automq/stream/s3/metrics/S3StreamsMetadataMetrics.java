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

package com.automq.stream.s3.metrics;

import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * Metrics recorded by S3StreamsMetadataImage.
 */
public final class S3StreamsMetadataMetrics {
    private static final AttributeKey<String> LABEL_STATUS = AttributeKey.stringKey("status");
    private static final Metrics.HistogramBundle GET_OBJECTS_TIME = Metrics.instance()
        .histogram("kafka_stream_get_objects_time", "Get objects time", "nanoseconds");
    private static final Metrics.HistogramBundle OBJECTS_SEARCH_COUNT = Metrics.instance()
        .histogram("kafka_stream_objects_search_count", "Number of SSO object to search when get objects", "");
    private static final DeltaHistogram GET_OBJECTS_TIME_SUCCESS = GET_OBJECTS_TIME.histogram(MetricsLevel.INFO,
        statusAttributes(S3StreamMetricsConstant.LABEL_STATUS_SUCCESS));
    private static final DeltaHistogram GET_OBJECTS_TIME_FAILED = GET_OBJECTS_TIME.histogram(MetricsLevel.INFO,
        statusAttributes(S3StreamMetricsConstant.LABEL_STATUS_FAILED));
    private static final DeltaHistogram RANGE_INDEX_SKIPPED_OBJECT_NUM =
        OBJECTS_SEARCH_COUNT.histogram(MetricsLevel.INFO, Attributes.empty());

    private S3StreamsMetadataMetrics() {
    }

    /**
     * Records the latency of a get-objects lookup in S3StreamsMetadataImage.
     */
    public static void recordGetObjectsTime(boolean success, long timeNanos) {
        (success ? GET_OBJECTS_TIME_SUCCESS : GET_OBJECTS_TIME_FAILED).record(timeNanos);
    }

    /**
     * Records how many stream-set objects were skipped by the sparse range index lookup.
     */
    public static void recordRangeIndexSkippedObjectNum(long count) {
        RANGE_INDEX_SKIPPED_OBJECT_NUM.record(count);
    }

    private static Attributes statusAttributes(String status) {
        return Attributes.of(LABEL_STATUS, status);
    }
}
