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

package com.automq.stream.s3.wal.metrics;

import com.automq.stream.s3.metrics.NoopLongHistogram;
import com.automq.stream.s3.metrics.NoopObservableLongGauge;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

public class ObjectWALMetricsManager {
    private static ObservableLongGauge inflightuploadCount = new NoopObservableLongGauge();
    private static ObservableLongGauge bufferedDataSizeInBytes = new NoopObservableLongGauge();
    private static ObservableLongGauge objectDataSizeInBytes = new NoopObservableLongGauge();
    private static LongHistogram operationLatencyInMillis = new NoopLongHistogram();
    private static LongHistogram operationDataSizeInBytes = new NoopLongHistogram();

    private static Supplier<Long> inflightUploadCountSupplier = () -> 0L;
    private static Supplier<Long> bufferedDataInBytesSupplier = () -> 0L;
    private static Supplier<Long> objectDataInBytesSupplier = () -> 0L;

    public static void initMetrics(Meter meter) {
        initMetrics(meter, "");
    }

    public static void initMetrics(Meter meter, String prefix) {
        operationLatencyInMillis = meter
            .histogramBuilder(prefix + "operation_latency")
            .ofLongs()
            .setUnit("milliseconds")
            .setExplicitBucketBoundariesAdvice(List.of(10L, 100L, 150L, 200L, 300L, 400L, 500L, 750L, 1000L, 3 * 1000L))
            .setDescription("Operation latency in milliseconds")
            .build();
        operationDataSizeInBytes = meter
            .histogramBuilder(prefix + "data_size")
            .ofLongs()
            .setUnit("bytes")
            .setDescription("Operation size in bytes")
            .setExplicitBucketBoundariesAdvice(List.of(512L, 1024L, 16 * 1024L, 32 * 1024L, 64 * 1024L, 128 * 1024L, 256 * 1024L, 512 * 1024L,
                1024 * 1024L, 4 * 1024L * 1024L, 8 * 1024L * 1024L, 16 * 1024L * 1024L))
            .build();
        inflightuploadCount = meter
            .gaugeBuilder(prefix + "inflight_upload_count")
            .setDescription("Inflight upload count")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(inflightUploadCountSupplier.get(), Attributes.empty()));
        bufferedDataSizeInBytes = meter
            .gaugeBuilder(prefix + "buffered_data_size")
            .setDescription("Buffered data size")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(bufferedDataInBytesSupplier.get(), Attributes.empty()));
        objectDataSizeInBytes = meter
            .gaugeBuilder(prefix + "object_data_size")
            .setDescription("Object data size in S3")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(objectDataInBytesSupplier.get(), Attributes.empty()));
    }

    public static void recordOperationLatency(long latencyInNanos, String operation, boolean success) {
        Attributes attributes = Attributes.builder().put("operation", operation).put("success", success).build();
        operationLatencyInMillis.record(TimeUnit.NANOSECONDS.toMillis(latencyInNanos), attributes);
    }

    public static void recordOperationDataSize(long size, String operation) {
        Attributes attributes = Attributes.builder().put("operation", operation).build();
        operationDataSizeInBytes.record(size, attributes);
    }

    public static void setInflightUploadCountSupplier(Supplier<Long> inflightuploadCountSupplier) {
        ObjectWALMetricsManager.inflightUploadCountSupplier = inflightuploadCountSupplier;
    }

    public static void setBufferedDataInBytesSupplier(Supplier<Long> bufferedDataInBytesSupplier) {
        ObjectWALMetricsManager.bufferedDataInBytesSupplier = bufferedDataInBytesSupplier;
    }

    public static void setObjectDataInBytesSupplier(Supplier<Long> objectDataInBytesSupplier) {
        ObjectWALMetricsManager.objectDataInBytesSupplier = objectDataInBytesSupplier;
    }
}
