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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/** Shares snapshot replay and optional disk-tier metrics. */
final class SnapshotReadCacheMetrics {
    static final Metrics.HistogramBundle OPERATION_LATENCY = Metrics.instance().histogram(
        "kafka_stream_snapshot_read_cache", "Snapshot read cache operation latency", "nanoseconds");
    static final DeltaHistogram DISK_HIT_LATENCY = diskLatency("hit");
    static final DeltaHistogram DISK_MISS_LATENCY = diskLatency("miss");
    private static final Metrics.LongCounterBundle DISK_OVERLAP_REMOVED = Metrics.instance().longCounter(
        "kafka_stream_snapshot_read_cache_disk_overlap_removed_total",
        "Disk index entries removed by overlapping snapshot batch admissions", "count");
    private static final Metrics.LongCounterBundle.LongCounter DISK_OVERLAP_REMOVED_COUNT =
        DISK_OVERLAP_REMOVED.register(MetricsLevel.INFO, Attributes.empty());

    private SnapshotReadCacheMetrics() {
    }

    private static DeltaHistogram diskLatency(String status) {
        return OPERATION_LATENCY.histogram(MetricsLevel.INFO, Attributes.of(
            AttributeKey.stringKey("operation"), "disk_read", AttributeKey.stringKey("status"), status));
    }

    static void recordDiskRead(boolean hit, long startNanos) {
        (hit ? DISK_HIT_LATENCY : DISK_MISS_LATENCY).record(System.nanoTime() - startNanos);
    }

    static void recordDiskOverlapRemoved(int count) {
        DISK_OVERLAP_REMOVED_COUNT.add(count);
    }
}
