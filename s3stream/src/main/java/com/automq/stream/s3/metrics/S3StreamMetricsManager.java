/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metrics;

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class S3StreamMetricsManager {
    private static final List<ConfigListener> BASE_ATTRIBUTES_LISTENERS = new ArrayList<>();
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
    private static LongHistogram readAheadSize = new NoopLongHistogram();
    private static LongHistogram readAheadLimierQueueTime = new NoopLongHistogram();
    private static ObservableLongGauge deltaWalStartOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalTrimmedOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge blockCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightReadAheadSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3ReadQuota = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3WriteQuota = new NoopObservableLongGauge();
    private static ObservableLongGauge inflightWALUploadTasksCount = new NoopObservableLongGauge();
    private static ObservableLongGauge allocatedDirectMemorySize = new NoopObservableLongGauge();
    private static ObservableLongGauge usedDirectMemorySize = new NoopObservableLongGauge();
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
    private static MetricsConfig metricsConfig = new MetricsConfig(MetricsLevel.INFO, Attributes.empty());
    private static final MultiAttributes<String> ALLOC_TYPE_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
        S3StreamMetricsConstant.LABEL_ALLOC_TYPE);

    static {
        BASE_ATTRIBUTES_LISTENERS.add(ALLOC_TYPE_ATTRIBUTES);
    }

    public static void configure(MetricsConfig metricsConfig) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            S3StreamMetricsManager.metricsConfig = metricsConfig;
            for (ConfigListener listener : BASE_ATTRIBUTES_LISTENERS) {
                listener.onConfigChange(metricsConfig);
            }
        }
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
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(networkInboundAvailableBandwidthSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        networkOutboundAvailableBandwidth = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME)
            .setDescription("Network outbound available bandwidth")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(networkOutboundAvailableBandwidthSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        networkInboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_INBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
            .setDescription("Network inbound limiter queue size")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record((long) networkInboundLimiterQueueSizeSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        networkOutboundLimiterQueueSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME)
            .setDescription("Network outbound limiter queue size")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record((long) networkOutboundLimiterQueueSizeSupplier.get(), metricsConfig.getBaseAttributes());
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
        readAheadSize = meter.histogramBuilder(prefix + S3StreamMetricsConstant.READ_AHEAD_SIZE_METRIC_NAME)
            .setDescription("Read ahead size")
            .setUnit("bytes")
            .ofLongs()
            .build();
        readAheadLimierQueueTime = meter.histogramBuilder(prefix + S3StreamMetricsConstant.READ_AHEAD_QUEUE_TIME_METRIC_NAME)
            .setDescription("Read ahead limiter queue time")
            .setUnit("nanoseconds")
            .ofLongs()
            .setExplicitBucketBoundariesAdvice(S3StreamMetricsConstant.LATENCY_BOUNDARIES)
            .build();
        deltaWalStartOffset = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.WAL_START_OFFSET)
            .setDescription("Delta WAL start offset")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(deltaWalStartOffsetSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        deltaWalTrimmedOffset = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.WAL_TRIMMED_OFFSET)
            .setDescription("Delta WAL trimmed offset")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(deltaWalTrimmedOffsetSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        deltaWalCacheSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.DELTA_WAL_CACHE_SIZE)
            .setDescription("Delta WAL cache size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(deltaWALCacheSizeSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        blockCacheSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.BLOCK_CACHE_SIZE)
            .setDescription("Block cache size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(blockCacheSizeSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        availableInflightReadAheadSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_INFLIGHT_READ_AHEAD_SIZE_METRIC_NAME)
            .setDescription("Available inflight read ahead size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record((long) availableInflightReadAheadSizeSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        availableInflightS3ReadQuota = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_S3_INFLIGHT_READ_QUOTA_METRIC_NAME)
            .setDescription("Available inflight S3 read quota")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record((long) availableInflightS3ReadQuotaSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        availableInflightS3WriteQuota = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_S3_INFLIGHT_WRITE_QUOTA_METRIC_NAME)
            .setDescription("Available inflight S3 write quota")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record((long) availableInflightS3WriteQuotaSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        inflightWALUploadTasksCount = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.INFLIGHT_WAL_UPLOAD_TASKS_COUNT_METRIC_NAME)
            .setDescription("Inflight upload WAL tasks count")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record((long) inflightWALUploadTasksCountSupplier.get(), metricsConfig.getBaseAttributes());
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
        allocatedDirectMemorySize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.ALLOCATED_DIRECT_MEMORY_SIZE_METRIC_NAME)
            .setDescription("Allocated direct memory size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && DirectByteBufAlloc.directByteBufAllocMetric != null) {
                    Map<String, Long> allocateSizeMap = DirectByteBufAlloc.directByteBufAllocMetric.getDetailedMap();
                    for (Map.Entry<String, Long> entry : allocateSizeMap.entrySet()) {
                        result.record(entry.getValue(), ALLOC_TYPE_ATTRIBUTES.get(entry.getKey()));
                    }
                }
            });
        usedDirectMemorySize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.USED_DIRECT_MEMORY_SIZE_METRIC_NAME)
            .setDescription("Used direct memory size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel()) && DirectByteBufAlloc.directByteBufAllocMetric != null) {
                    result.record(DirectByteBufAlloc.directByteBufAllocMetric.getUsedDirectMemory(), metricsConfig.getBaseAttributes());
                }
            });
    }

    public static void registerNetworkLimiterSupplier(AsyncNetworkBandwidthLimiter.Type type,
        Supplier<Long> networkAvailableBandwidthSupplier,
        Supplier<Integer> networkLimiterQueueSizeSupplier) {
        switch (type) {
            case INBOUND:
                S3StreamMetricsManager.networkInboundAvailableBandwidthSupplier = networkAvailableBandwidthSupplier;
                S3StreamMetricsManager.networkInboundLimiterQueueSizeSupplier = networkLimiterQueueSizeSupplier;
                break;
            case OUTBOUND:
                S3StreamMetricsManager.networkOutboundAvailableBandwidthSupplier = networkAvailableBandwidthSupplier;
                S3StreamMetricsManager.networkOutboundLimiterQueueSizeSupplier = networkLimiterQueueSizeSupplier;
                break;
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

    public static CounterMetric buildS3UploadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, s3UploadSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildS3DownloadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, s3DownloadSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildStageOperationMetric(S3Stage stage) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, AttributesUtils.buildAttributes(stage), operationLatency);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildOperationMetric(S3Operation operation) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, AttributesUtils.buildAttributes(operation), operationLatency);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildOperationMetric(S3Operation operation, String status, String sizeLabelName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, AttributesUtils.buildAttributes(operation,
                status, sizeLabelName), operationLatency);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildOperationMetric(S3Operation operation, String status) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, AttributesUtils.buildAttributes(operation, status), operationLatency);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildObjectNumMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, objectNumInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildObjectStageCostMetric(S3ObjectStage stage) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, AttributesUtils.buildAttributes(stage), objectStageCost);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildObjectUploadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, objectUploadSize);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildObjectDownloadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, objectDownloadSize);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildNetworkInboundUsageMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, networkInboundUsageInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildNetworkOutboundUsageMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, networkOutboundUsageInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildNetworkInboundLimiterQueueTimeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, networkInboundLimiterQueueTime);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildNetworkOutboundLimiterQueueTimeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, networkOutboundLimiterQueueTime);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildReadAheadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, readAheadSize);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }

    }

    public static HistogramMetric buildReadAheadLimiterQueueTimeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsConfig, readAheadLimierQueueTime);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildCompactionReadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, compactionReadSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildCompactionWriteSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, compactionWriteSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }
}
