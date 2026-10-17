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

import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * Metrics recorded by object writers.
 */
public final class S3ObjectMetrics {
    private static final AttributeKey<String> LABEL_STAGE = AttributeKey.stringKey("stage");
    private static final Metrics.LongCounterBundle.LongCounter OBJECT_NUM_IN_TOTAL = Metrics.instance()
        .longCounter("kafka_stream_object_count", "Objects count", "")
        .register(MetricsLevel.DEBUG, Attributes.empty());
    private static final Metrics.HistogramBundle OBJECT_STAGE_COST = Metrics.instance()
        .histogram("kafka_stream_object_stage_cost", "Objects stage cost", "nanoseconds");

    private static final DeltaHistogram OBJECT_STAGE_UPLOAD_PART = OBJECT_STAGE_COST
        .histogram(MetricsLevel.DEBUG, attributes(S3ObjectStage.UPLOAD_PART));
    private static final DeltaHistogram OBJECT_STAGE_READY_CLOSE = OBJECT_STAGE_COST
        .histogram(MetricsLevel.DEBUG, attributes(S3ObjectStage.READY_CLOSE));
    private static final DeltaHistogram OBJECT_STAGE_TOTAL = OBJECT_STAGE_COST
        .histogram(MetricsLevel.DEBUG, attributes(S3ObjectStage.TOTAL));

    private S3ObjectMetrics() {
    }

    /**
     * Records an upload-part stage latency.
     */
    public static void recordUploadPartStage(long timeNanos) {
        OBJECT_STAGE_UPLOAD_PART.record(timeNanos);
    }

    /**
     * Records a ready-close stage latency.
     */
    public static void recordReadyCloseStage(long timeNanos) {
        OBJECT_STAGE_READY_CLOSE.record(timeNanos);
    }

    /**
     * Records a total object writer latency.
     */
    public static void recordTotalStage(long timeNanos) {
        OBJECT_STAGE_TOTAL.record(timeNanos);
    }

    /**
     * Records a completed object.
     */
    public static void recordObject() {
        OBJECT_NUM_IN_TOTAL.add(1);
    }

    private static Attributes attributes(S3ObjectStage objectStage) {
        return Attributes.builder()
            .put(LABEL_STAGE, objectStage.getName())
            .build();
    }
}
