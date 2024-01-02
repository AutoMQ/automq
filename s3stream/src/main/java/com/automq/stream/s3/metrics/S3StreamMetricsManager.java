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
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.function.Supplier;

public class S3StreamMetricsManager {
    private static LongCounter s3DownloadSizeInTotal = new NoopLongCounter();
    private static LongCounter s3UploadSizeInTotal = new NoopLongCounter();
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
    private static MetricsLevel metricsLevel = MetricsLevel.INFO;

    public static void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        AttributesCache.INSTANCE.setDefaultAttributes(attributesBuilderSupplier.get().build());
    }

    public static void initMetrics(Meter meter) {
        initMetrics(meter, "");
    }

    public static void setMetricsLevel(MetricsLevel level) {
        metricsLevel = level;
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
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsLevel)) {
                    result.record(networkInboundAvailableBandwidthSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        networkOutboundAvailableBandwidth = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME)
            .setDescription("Network outbound available bandwidth")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsLevel)) {
                    result.record(networkOutboundAvailableBandwidthSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        networkInboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_INBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
            .setDescription("Network inbound limiter queue size")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record((long) networkInboundLimiterQueueSizeSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        networkOutboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
            .setDescription("Network outbound limiter queue size")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record((long) networkOutboundLimiterQueueSizeSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
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
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record(deltaWalStartOffsetSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        deltaWalTrimmedOffset = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.WAL_TRIMMED_OFFSET)
            .setDescription("Delta WAL trimmed offset")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record(deltaWalTrimmedOffsetSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        deltaWalCacheSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.DELTA_WAL_CACHE_SIZE)
            .setDescription("Delta WAL cache size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record(deltaWALCacheSizeSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        blockCacheSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.BLOCK_CACHE_SIZE)
            .setDescription("Block cache size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record(blockCacheSizeSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        availableInflightReadAheadSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_INFLIGHT_READ_AHEAD_SIZE_METRIC_NAME)
            .setDescription("Available inflight read ahead size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record((long) availableInflightReadAheadSizeSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        availableInflightS3ReadQuota = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_S3_INFLIGHT_READ_QUOTA_METRIC_NAME)
            .setDescription("Available inflight S3 read quota")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record((long) availableInflightS3ReadQuotaSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        availableInflightS3WriteQuota = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_S3_INFLIGHT_WRITE_QUOTA_METRIC_NAME)
            .setDescription("Available inflight S3 write quota")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record((long) availableInflightS3WriteQuotaSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        inflightWALUploadTasksCount = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.INFLIGHT_WAL_UPLOAD_TASKS_COUNT_METRIC_NAME)
            .setDescription("Inflight upload WAL tasks count")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsLevel)) {
                    result.record((long) inflightWALUploadTasksCountSupplier.get(), AttributesCache.INSTANCE.defaultAttributes());
                }
            });
        compactionReadSizeInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.COMPACTION_READ_SIZE_METRIC_NAME)
            .setDescription("Compaction read size")
            .setUnit("bytes")
            .build();
        compactionWriteSizeInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.COMPACTION_WRITE_SIZE_METRIC_NAME)
            .setDescription("Compaction write size")
            .setUnit("bytes")
            .build();
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

    public static void registerInflightReadSizeLimiterSupplier(
        Supplier<Integer> availableInflightReadAheadSizeSupplier) {
        S3StreamMetricsManager.availableInflightReadAheadSizeSupplier = availableInflightReadAheadSizeSupplier;
    }

    public static void registerInflightWALUploadTasksCountSupplier(
        Supplier<Integer> inflightWALUploadTasksCountSupplier) {
        S3StreamMetricsManager.inflightWALUploadTasksCountSupplier = inflightWALUploadTasksCountSupplier;
    }

    public static void recordS3UploadSize(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            s3UploadSizeInTotal.add(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordS3DownloadSize(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            s3DownloadSizeInTotal.add(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordOperationLatency(MetricsLevel level, long value, S3Operation operation) {
        recordOperationLatency(level, value, operation, 0, true);
    }

    public static void recordOperationLatency(MetricsLevel level, long value, S3Operation operation,
        boolean isSuccess) {
        recordOperationLatency(level, value, operation, 0, isSuccess);
    }

    public static void recordOperationLatency(MetricsLevel level, long value, S3Operation operation, long size) {
        recordOperationLatency(level, value, operation, size, true);
    }

    public static void recordOperationLatency(MetricsLevel level, long value, S3Operation operation, long size,
        boolean isSuccess) {
        if (level.isWithin(metricsLevel)) {
            operationLatency.record(value, AttributesCache.INSTANCE.getAttributes(operation, size, isSuccess));
        }
    }

    public static void recordStageLatency(MetricsLevel level, long value, S3Stage stage) {
        if (level.isWithin(metricsLevel)) {
            operationLatency.record(value, AttributesCache.INSTANCE.getAttributes(stage));
        }
    }

    public static void recordReadCacheLatency(MetricsLevel level, long value, S3Operation operation,
        boolean isCacheHit) {
        if (level.isWithin(metricsLevel)) {
            operationLatency.record(value, AttributesCache.INSTANCE.getAttributes(operation, isCacheHit ? "hit" : "miss"));
        }
    }

    public static void recordReadAheadLatency(MetricsLevel level, long value, S3Operation operation, boolean isSync) {
        if (level.isWithin(metricsLevel)) {
            operationLatency.record(value, AttributesCache.INSTANCE.getAttributes(operation, isSync ? "sync" : "async"));
        }
    }

    public static void recordObjectNum(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            objectNumInTotal.add(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordObjectStageCost(MetricsLevel level, long value, S3ObjectStage stage) {
        if (level.isWithin(metricsLevel)) {
            objectStageCost.record(value, AttributesCache.INSTANCE.getAttributes(stage));
        }
    }

    public static void recordObjectUploadSize(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            objectUploadSize.record(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordObjectDownloadSize(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            objectDownloadSize.record(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordNetworkInboundUsage(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            networkInboundUsageInTotal.add(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordNetworkOutboundUsage(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            networkOutboundUsageInTotal.add(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordNetworkLimiterQueueTime(MetricsLevel level, long value,
        AsyncNetworkBandwidthLimiter.Type type) {
        if (level.isWithin(metricsLevel)) {
            switch (type) {
                case INBOUND ->
                    networkInboundLimiterQueueTime.record(value, AttributesCache.INSTANCE.defaultAttributes());
                case OUTBOUND ->
                    networkOutboundLimiterQueueTime.record(value, AttributesCache.INSTANCE.defaultAttributes());
            }
        }
    }

    public static void recordAllocateByteBufSize(MetricsLevel level, long value, String source) {
        if (level.isWithin(metricsLevel)) {
            allocateByteBufSize.record(value, AttributesCache.INSTANCE.getAttributes(source));
        }
    }

    public static void recordReadAheadSize(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            readAheadSize.record(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordCompactionReadSizeIn(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            compactionReadSizeInTotal.add(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }

    public static void recordCompactionWriteSize(MetricsLevel level, long value) {
        if (level.isWithin(metricsLevel)) {
            compactionWriteSizeInTotal.add(value, AttributesCache.INSTANCE.defaultAttributes());
        }
    }
}
