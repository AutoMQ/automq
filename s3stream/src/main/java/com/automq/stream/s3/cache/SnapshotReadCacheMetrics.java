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
    private static final Metrics.LongCounterBundle DISK_REJECTED = Metrics.instance().longCounter(
        "kafka_stream_snapshot_read_cache_disk_rejected_total",
        "Disk cache tasks dropped by the pending-byte budget or a full task queue", "count");
    private static final Metrics.LongCounterBundle.LongCounter DISK_WRITE_BUDGET_REJECTED =
        diskRejected("write", "budget");
    private static final Metrics.LongCounterBundle.LongCounter DISK_WRITE_QUEUE_REJECTED =
        diskRejected("write", "queue");
    private static final Metrics.LongCounterBundle.LongCounter DISK_READ_BUDGET_REJECTED =
        diskRejected("read", "budget");
    private static final Metrics.LongCounterBundle.LongCounter DISK_READ_QUEUE_REJECTED =
        diskRejected("read", "queue");

    private SnapshotReadCacheMetrics() {
    }

    private static DeltaHistogram diskLatency(String status) {
        return OPERATION_LATENCY.histogram(MetricsLevel.INFO, Attributes.of(
            AttributeKey.stringKey("operation"), "disk_read", AttributeKey.stringKey("status"), status));
    }

    private static Metrics.LongCounterBundle.LongCounter diskRejected(String operation, String reason) {
        return DISK_REJECTED.register(MetricsLevel.INFO, Attributes.of(
            AttributeKey.stringKey("operation"), operation, AttributeKey.stringKey("reason"), reason));
    }

    static void recordDiskRead(boolean hit, long startNanos) {
        (hit ? DISK_HIT_LATENCY : DISK_MISS_LATENCY).record(System.nanoTime() - startNanos);
    }

    static void recordDiskOverlapRemoved(int count) {
        DISK_OVERLAP_REMOVED_COUNT.add(count);
    }

    static void recordDiskWriteRejected(boolean queueFull) {
        (queueFull ? DISK_WRITE_QUEUE_REJECTED : DISK_WRITE_BUDGET_REJECTED).add(1);
    }

    static void recordDiskReadRejected(boolean queueFull) {
        (queueFull ? DISK_READ_QUEUE_REJECTED : DISK_READ_BUDGET_REJECTED).add(1);
    }
}
