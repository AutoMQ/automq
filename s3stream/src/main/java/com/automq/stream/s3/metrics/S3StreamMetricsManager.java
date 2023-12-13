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

package com.automq.stream.s3.metrics;

import com.automq.stream.s3.metrics.operations.S3MetricsType;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import java.util.function.Supplier;

public class S3StreamMetricsManager {
    private static LongCounter s3DownloadSizeInTotal = new NoopLongCounter();
    private static LongCounter s3UploadSizeInTotal = new NoopLongCounter();
    private static LongCounter operationNumInTotal = new NoopLongCounter();
    private static LongHistogram operationLatency = new NoopLongHistogram();
    private static LongCounter objectNumInTotal = new NoopLongCounter();
    private static LongHistogram objectStageCost = new NoopLongHistogram();
    private static LongHistogram objectUploadSize = new NoopLongHistogram();
    private static LongHistogram objectDownloadSize = new NoopLongHistogram();
    private static LongCounter networkInboundUsageInTotal = new NoopLongCounter();
    private static LongCounter networkOutboundUsageInTotal = new NoopLongCounter();
    private static ObservableLongGauge networkInboundAvailableBandwidth = new NoopObservableLongGauge();
    private static ObservableLongGauge networkOutboundAvailableBandwidth = new NoopObservableLongGauge();
    private static ObservableLongGauge networkInboundLimiterQueueSize = new NoopObservableLongGauge();
    private static ObservableLongGauge networkOutboundLimiterQueueSize = new NoopObservableLongGauge();
    private static LongHistogram allocateByteBufSize = new NoopLongHistogram();
    private static LongHistogram readAheadSize = new NoopLongHistogram();
    private static ObservableLongGauge availableInflightReadAheadSize = new NoopObservableLongGauge();
    private static LongCounter compactionReadSizeInTotal = new NoopLongCounter();
    private static LongCounter compactionWriteSizeInTotal = new NoopLongCounter();
    private static Gauge networkInboundAvailableBandwidthValue = new NoopGauge();
    private static Gauge networkOutboundAvailableBandwidthValue = new NoopGauge();
    private static Gauge networkInboundLimiterQueueSizeValue = new NoopGauge();
    private static Gauge networkOutboundLimiterQueueSizeValue = new NoopGauge();
    private static Gauge availableInflightReadAheadSizeValue = new NoopGauge();
    private static Supplier<AttributesBuilder> attributesBuilderSupplier = null;

    public static void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        S3StreamMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
    }

    public static void initMetrics(Meter meter) {
        initMetrics(meter, "");
    }

    public static void initMetrics(Meter meter, String prefix) {
        s3DownloadSizeInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.DOWNLOAD_SIZE_METRIC_NAME)
                .setDescription("S3 download size")
                .setUnit("bytes")
                .build();
        s3UploadSizeInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.UPLOAD_SIZE_METRIC_NAME)
                .setDescription("S3 upload size")
                .setUnit("bytes")
                .build();
        operationNumInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.OPERATION_COUNT_METRIC_NAME)
                .setDescription("Operations count")
                .build();
        operationLatency = meter.histogramBuilder(prefix + S3StreamMetricsConstant.OPERATION_LATENCY_METRIC_NAME)
                .setDescription("Operations latency")
                .setUnit("nanoseconds")
                .ofLongs()
                .build();
        objectNumInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.OBJECT_COUNT_METRIC_NAME)
                .setDescription("Objects count")
                .build();
        objectStageCost = meter.histogramBuilder(prefix + S3StreamMetricsConstant.OBJECT_STAGE_COST_METRIC_NAME)
                .setDescription("Objects stage cost")
                .setUnit("nanoseconds")
                .ofLongs()
                .build();
        objectUploadSize = meter.histogramBuilder(prefix + S3StreamMetricsConstant.OBJECT_UPLOAD_SIZE_METRIC_NAME)
                .setDescription("Objects upload size")
                .setUnit("bytes")
                .ofLongs()
                .build();
        objectDownloadSize = meter.histogramBuilder(prefix + S3StreamMetricsConstant.OBJECT_DOWNLOAD_SIZE_METRIC_NAME)
                .setDescription("Objects download size")
                .setUnit("bytes")
                .ofLongs()
                .build();
        networkInboundUsageInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.NETWORK_INBOUND_USAGE_METRIC_NAME)
                .setDescription("Network inbound usage")
                .setUnit("bytes")
                .build();
        networkOutboundUsageInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_USAGE_METRIC_NAME)
                .setDescription("Network outbound usage")
                .setUnit("bytes")
                .build();
        networkInboundAvailableBandwidth = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_INBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME)
                .setDescription("Network inbound available bandwidth")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> result.record(networkInboundAvailableBandwidthValue.value(), newAttributesBuilder().build()));
        networkOutboundAvailableBandwidth = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME)
                .setDescription("Network outbound available bandwidth")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> result.record(networkOutboundAvailableBandwidthValue.value(), newAttributesBuilder().build()));
        networkInboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_INBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
                .setDescription("Network inbound limiter queue size")
                .ofLongs()
                .buildWithCallback(result -> result.record(networkInboundLimiterQueueSizeValue.value(), newAttributesBuilder().build()));
        networkOutboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
                .setDescription("Network outbound limiter queue size")
                .ofLongs()
                .buildWithCallback(result -> result.record(networkOutboundLimiterQueueSizeValue.value(), newAttributesBuilder().build()));
        allocateByteBufSize = meter.histogramBuilder(prefix + S3StreamMetricsConstant.ALLOCATE_BYTE_BUF_SIZE_METRIC_NAME)
                .setDescription("Allocate byte buf size")
                .setUnit("bytes")
                .ofLongs()
                .build();
        readAheadSize = meter.histogramBuilder(prefix + S3StreamMetricsConstant.READ_AHEAD_SIZE_METRIC_NAME)
                .setDescription("Read ahead size")
                .setUnit("bytes")
                .ofLongs()
                .build();
        availableInflightReadAheadSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_INFLIGHT_READ_AHEAD_SIZE_METRIC_NAME)
                .setDescription("Available inflight read ahead size")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> result.record(availableInflightReadAheadSizeValue.value(), newAttributesBuilder().build()));
        compactionReadSizeInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.COMPACTION_READ_SIZE_METRIC_NAME)
                .setDescription("Compaction read size")
                .setUnit("bytes")
                .build();
        compactionWriteSizeInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.COMPACTION_WRITE_SIZE_METRIC_NAME)
                .setDescription("Compaction write size")
                .setUnit("bytes")
                .build();
    }

    private static AttributesBuilder newAttributesBuilder() {
        if (attributesBuilderSupplier != null) {
            return attributesBuilderSupplier.get();
        }
        return Attributes.builder();
    }

    public static void registerNetworkLimiterGauge(AsyncNetworkBandwidthLimiter.Type type, Gauge networkAvailableBandwidthValue, Gauge networkLimiterQueueSizeValue) {
        switch (type) {
            case INBOUND -> {
                S3StreamMetricsManager.networkInboundAvailableBandwidthValue = networkAvailableBandwidthValue;
                S3StreamMetricsManager.networkInboundLimiterQueueSizeValue = networkLimiterQueueSizeValue;
            }
            case OUTBOUND -> {
                S3StreamMetricsManager.networkOutboundAvailableBandwidthValue = networkAvailableBandwidthValue;
                S3StreamMetricsManager.networkOutboundLimiterQueueSizeValue = networkLimiterQueueSizeValue;
            }
        }
    }

    public static void registerInflightReadSizeLimiterGauge(Gauge availableInflightReadAheadSizeValue) {
        S3StreamMetricsManager.availableInflightReadAheadSizeValue = availableInflightReadAheadSizeValue;
    }

    public static void recordS3UploadSize(long value) {
        s3UploadSizeInTotal.add(value, newAttributesBuilder().build());
    }

    public static void recordS3DownloadSize(long value) {
        s3DownloadSizeInTotal.add(value, newAttributesBuilder().build());
    }

    public static void recordOperationNum(long value, S3Operation operation) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .build();
        operationNumInTotal.add(value, attributes);
    }

    public static void recordOperationLatency(long value, S3Operation operation) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .build();
        operationLatency.record(value, attributes);
    }

    public static void recordAppendWALLatency(long value, String stage) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, S3MetricsType.S3Storage.getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, S3Operation.APPEND_STORAGE_WAL.getName())
                .put(S3StreamMetricsConstant.LABEL_APPEND_WAL_STAGE, stage)
                .build();
        operationLatency.record(value, attributes);
    }

    public static void recordReadCacheLatency(long value, S3Operation operation, boolean isCacheHit) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .put(S3StreamMetricsConstant.LABEL_CACHE_STATUS, isCacheHit ? "hit" : "miss")
                .build();
        operationLatency.record(value, attributes);
    }

    public static void recordObjectNum(long value) {
        objectNumInTotal.add(value, newAttributesBuilder().build());
    }

    public static void recordObjectStageCost(long value, S3ObjectStage stage) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OBJECT_STAGE, stage.getName())
                .build();
        objectStageCost.record(value, attributes);
    }

    public static void recordObjectUploadSize(long value) {
        objectUploadSize.record(value, newAttributesBuilder().build());
    }

    public static void recordObjectDownloadSize(long value) {
        objectDownloadSize.record(value, newAttributesBuilder().build());
    }

    public static void recordNetworkInboundUsage(long value) {
        networkInboundUsageInTotal.add(value, newAttributesBuilder().build());
    }

    public static void recordNetworkOutboundUsage(long value) {
        networkOutboundUsageInTotal.add(value, newAttributesBuilder().build());
    }

    public static void recordAllocateByteBufSize(long value, String source) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_ALLOCATE_BYTE_BUF_SOURCE, source)
                .build();
        allocateByteBufSize.record(value, attributes);
    }

    public static void recordReadAheadSize(long value) {
        readAheadSize.record(value, newAttributesBuilder().build());
    }

    public static void recordCompactionReadSizeIn(long value) {
        compactionReadSizeInTotal.add(value, newAttributesBuilder().build());
    }

    public static void recordCompactionWriteSize(long value) {
        compactionWriteSizeInTotal.add(value, newAttributesBuilder().build());
    }
}
