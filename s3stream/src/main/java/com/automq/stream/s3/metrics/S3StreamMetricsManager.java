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

import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
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
    private static LongHistogram networkInboundLimiterQueueTime = new NoopLongHistogram();
    private static LongHistogram networkOutboundLimiterQueueTime = new NoopLongHistogram();
    private static LongHistogram allocateByteBufSize = new NoopLongHistogram();
    private static LongHistogram readAheadSize = new NoopLongHistogram();
    private static ObservableLongGauge deltaWalStartOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalTrimmedOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge blockCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightReadAheadSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3ReadQuota = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3WriteQuota = new NoopObservableLongGauge();
    private static ObservableLongGauge inflightWALUploadTasksCount = new NoopObservableLongGauge();
    private static LongCounter compactionReadSizeInTotal = new NoopLongCounter();
    private static LongCounter compactionWriteSizeInTotal = new NoopLongCounter();
    private static Supplier<Long> networkInboundAvailableBandwidthSupplier = () -> 0L;
    private static Supplier<Long> networkOutboundAvailableBandwidthSupplier = () -> 0L;
    private static Supplier<Integer> networkInboundLimiterQueueSizeSupplier = () -> 0;
    private static Supplier<Integer> networkOutboundLimiterQueueSizeSupplier = () -> 0;
    private static Supplier<Integer> availableInflightReadAheadSizeSupplier = () -> 0;
    private static Supplier<Long> deltaWalStartOffsetSupplier = () -> 0L;
    private static Supplier<Long> deltaWalTrimmedOffsetSupplier = () -> 0L;
    private static Supplier<Long> deltaWALCacheSizeSupplier = () -> 0L;
    private static Supplier<Long> blockCacheSizeSupplier = () -> 0L;
    private static Supplier<Integer> availableInflightS3ReadQuotaSupplier = () -> 0;
    private static Supplier<Integer> availableInflightS3WriteQuotaSupplier = () -> 0;
    private static Supplier<Integer> inflightWALUploadTasksCountSupplier = () -> 0;
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
                .setExplicitBucketBoundariesAdvice(S3StreamMetricsConstant.LATENCY_BOUNDARIES)
                .build();
        objectNumInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.OBJECT_COUNT_METRIC_NAME)
                .setDescription("Objects count")
                .build();
        objectStageCost = meter.histogramBuilder(prefix + S3StreamMetricsConstant.OBJECT_STAGE_COST_METRIC_NAME)
                .setDescription("Objects stage cost")
                .setUnit("nanoseconds")
                .ofLongs()
                .setExplicitBucketBoundariesAdvice(S3StreamMetricsConstant.LATENCY_BOUNDARIES)
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
                .buildWithCallback(result -> result.record(networkInboundAvailableBandwidthSupplier.get(), newAttributesBuilder().build()));
        networkOutboundAvailableBandwidth = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME)
                .setDescription("Network outbound available bandwidth")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> result.record(networkOutboundAvailableBandwidthSupplier.get(), newAttributesBuilder().build()));
        networkInboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_INBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
                .setDescription("Network inbound limiter queue size")
                .ofLongs()
                .buildWithCallback(result -> result.record((long) networkInboundLimiterQueueSizeSupplier.get(), newAttributesBuilder().build()));
        networkOutboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
                .setDescription("Network outbound limiter queue size")
                .ofLongs()
                .buildWithCallback(result -> result.record((long) networkOutboundLimiterQueueSizeSupplier.get(), newAttributesBuilder().build()));
        networkInboundLimiterQueueTime = meter.histogramBuilder(prefix + S3StreamMetricsConstant.NETWORK_INBOUND_LIMITER_QUEUE_TIME_METRIC_NAME)
                .setDescription("Network inbound limiter queue time")
                .setUnit("nanoseconds")
                .ofLongs()
                .setExplicitBucketBoundariesAdvice(S3StreamMetricsConstant.LATENCY_BOUNDARIES)
                .build();
        networkOutboundLimiterQueueTime = meter.histogramBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_LIMITER_QUEUE_TIME_METRIC_NAME)
                .setDescription("Network outbound limiter queue time")
                .setUnit("nanoseconds")
                .ofLongs()
                .setExplicitBucketBoundariesAdvice(S3StreamMetricsConstant.LATENCY_BOUNDARIES)
                .build();
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
        deltaWalStartOffset = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.WAL_START_OFFSET)
                .setDescription("Delta WAL start offset")
                .ofLongs()
                .buildWithCallback(result -> result.record(deltaWalStartOffsetSupplier.get(), newAttributesBuilder().build()));
        deltaWalTrimmedOffset = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.WAL_TRIMMED_OFFSET)
                .setDescription("Delta WAL trimmed offset")
                .ofLongs()
                .buildWithCallback(result -> result.record(deltaWalTrimmedOffsetSupplier.get(), newAttributesBuilder().build()));
        deltaWalCacheSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.DELTA_WAL_CACHE_SIZE)
                .setDescription("Delta WAL cache size")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> result.record(deltaWALCacheSizeSupplier.get(), newAttributesBuilder().build()));
        blockCacheSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.BLOCK_CACHE_SIZE)
                .setDescription("Block cache size")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> result.record(blockCacheSizeSupplier.get(), newAttributesBuilder().build()));
        availableInflightReadAheadSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_INFLIGHT_READ_AHEAD_SIZE_METRIC_NAME)
                .setDescription("Available inflight read ahead size")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> result.record((long) availableInflightReadAheadSizeSupplier.get(), newAttributesBuilder().build()));
        availableInflightS3ReadQuota = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_S3_INFLIGHT_READ_QUOTA_METRIC_NAME)
                .setDescription("Available inflight S3 read quota")
                .ofLongs()
                .buildWithCallback(result -> result.record((long) availableInflightS3ReadQuotaSupplier.get(), newAttributesBuilder().build()));
        availableInflightS3WriteQuota = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_S3_INFLIGHT_WRITE_QUOTA_METRIC_NAME)
                .setDescription("Available inflight S3 write quota")
                .ofLongs()
                .buildWithCallback(result -> result.record((long) availableInflightS3WriteQuotaSupplier.get(), newAttributesBuilder().build()));
        inflightWALUploadTasksCount = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.INFLIGHT_WAL_UPLOAD_TASKS_COUNT_METRIC_NAME)
                .setDescription("Inflight upload WAL tasks count")
                .ofLongs()
                .buildWithCallback(result -> result.record((long) inflightWALUploadTasksCountSupplier.get(), newAttributesBuilder().build()));
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

    public static void registerNetworkLimiterSupplier(AsyncNetworkBandwidthLimiter.Type type,
                                                      Supplier<Long> networkAvailableBandwidthSupplier,
                                                      Supplier<Integer> networkLimiterQueueSizeSupplier) {
        switch (type) {
            case INBOUND -> {
                S3StreamMetricsManager.networkInboundAvailableBandwidthSupplier = networkAvailableBandwidthSupplier;
                S3StreamMetricsManager.networkInboundLimiterQueueSizeSupplier = networkLimiterQueueSizeSupplier;
            }
            case OUTBOUND -> {
                S3StreamMetricsManager.networkOutboundAvailableBandwidthSupplier = networkAvailableBandwidthSupplier;
                S3StreamMetricsManager.networkOutboundLimiterQueueSizeSupplier = networkLimiterQueueSizeSupplier;
            }
        }
    }

    //TODO: 各broker当前stream数量、各stream流量？、各broker的s3 object number, size
    public static void registerDeltaWalOffsetSupplier(Supplier<Long> deltaWalStartOffsetSupplier,
                                                      Supplier<Long> deltaWalTrimmedOffsetSupplier) {
        S3StreamMetricsManager.deltaWalStartOffsetSupplier = deltaWalStartOffsetSupplier;
        S3StreamMetricsManager.deltaWalTrimmedOffsetSupplier = deltaWalTrimmedOffsetSupplier;
    }

    public static void registerDeltaWalCacheSizeSupplier(Supplier<Long> deltaWalCacheSizeSupplier) {
        S3StreamMetricsManager.deltaWALCacheSizeSupplier = deltaWalCacheSizeSupplier;
    }

    public static void registerBlockCacheSizeSupplier(Supplier<Long> blockCacheSizeSupplier) {
        S3StreamMetricsManager.blockCacheSizeSupplier = blockCacheSizeSupplier;
    }

    public static void registerInflightS3ReadQuotaSupplier(Supplier<Integer> inflightS3ReadQuotaSupplier) {
        S3StreamMetricsManager.availableInflightS3ReadQuotaSupplier = inflightS3ReadQuotaSupplier;
    }

    public static void registerInflightS3WriteQuotaSupplier(Supplier<Integer> inflightS3WriteQuotaSupplier) {
        S3StreamMetricsManager.availableInflightS3WriteQuotaSupplier = inflightS3WriteQuotaSupplier;
    }

    public static void registerInflightReadSizeLimiterSupplier(Supplier<Integer> availableInflightReadAheadSizeSupplier) {
        S3StreamMetricsManager.availableInflightReadAheadSizeSupplier = availableInflightReadAheadSizeSupplier;
    }

    public static void registerInflightWALUploadTasksCountSupplier(Supplier<Integer> inflightWALUploadTasksCountSupplier) {
        S3StreamMetricsManager.inflightWALUploadTasksCountSupplier = inflightWALUploadTasksCountSupplier;
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
        recordOperationLatency(value, operation, 0, true);
    }

    public static void recordOperationLatency(long value, S3Operation operation, boolean isSuccess) {
        recordOperationLatency(value, operation, 0, isSuccess);
    }

    public static void recordOperationLatency(long value, S3Operation operation, long size) {
        recordOperationLatency(value, operation, size, true);
    }

    public static void recordOperationLatency(long value, S3Operation operation, long size, boolean isSuccess) {
        AttributesBuilder attributesBuilder = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .put(S3StreamMetricsConstant.LABEL_STATUS, isSuccess ? "success" : "failed");
        if (operation == S3Operation.GET_OBJECT || operation == S3Operation.PUT_OBJECT || operation == S3Operation.UPLOAD_PART) {
            attributesBuilder.put(S3StreamMetricsConstant.LABEL_SIZE_NAME, getObjectBucketLabel(size));
        }
        operationLatency.record(value, attributesBuilder.build());
    }

    public static void recordStageLatency(long value, S3Stage stage) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, stage.getOperation().getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, stage.getOperation().getName())
                .put(S3StreamMetricsConstant.LABEL_STAGE, stage.getName())
                .build();
        operationLatency.record(value, attributes);
    }

    public static void recordReadCacheLatency(long value, S3Operation operation, boolean isCacheHit) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .put(S3StreamMetricsConstant.LABEL_STATUS, isCacheHit ? "hit" : "miss")
                .build();
        operationLatency.record(value, attributes);
    }

    public static void recordReadAheadLatency(long value, S3Operation operation, boolean isSync) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_OPERATION_TYPE, operation.getType().getName())
                .put(S3StreamMetricsConstant.LABEL_OPERATION_NAME, operation.getName())
                .put(S3StreamMetricsConstant.LABEL_STATUS, isSync ? "sync" : "async")
                .build();
        operationLatency.record(value, attributes);
    }

    public static String getObjectBucketLabel(long objectSize) {
        int index = (int) Math.ceil(Math.log((double) objectSize / (16 * 1024)) / Math.log(2));
        index = Math.min(S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES.length - 1, Math.max(0, index));
        return S3StreamMetricsConstant.OBJECT_SIZE_BUCKET_NAMES[index];
    }

    public static void recordObjectNum(long value) {
        objectNumInTotal.add(value, newAttributesBuilder().build());
    }

    public static void recordObjectStageCost(long value, S3ObjectStage stage) {
        Attributes attributes = newAttributesBuilder()
                .put(S3StreamMetricsConstant.LABEL_STAGE, stage.getName())
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

    public static void recordNetworkLimiterQueueTime(long value, AsyncNetworkBandwidthLimiter.Type type) {
        switch (type) {
            case INBOUND -> networkInboundLimiterQueueTime.record(value, newAttributesBuilder().build());
            case OUTBOUND -> networkOutboundLimiterQueueTime.record(value, newAttributesBuilder().build());
        }
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
