/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metrics;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramInstrument;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

public class S3StreamMetricsManager {
    private static final List<ConfigListener> BASE_ATTRIBUTES_LISTENERS = new ArrayList<>();
    public static final List<HistogramMetric> OPERATION_LATENCY_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> OBJECT_STAGE_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> NETWORK_INBOUND_LIMITER_QUEUE_TIME_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> NETWORK_OUTBOUND_LIMITER_QUEUE_TIME_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> READ_AHEAD_SIZE_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> READ_AHEAD_STAGE_TIME_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> READ_S3_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> WRITE_S3_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> GET_INDEX_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> READ_BLOCK_CACHE_TIME_METRICS = new CopyOnWriteArrayList<>();
    private static LongCounter s3DownloadSizeInTotal = new NoopLongCounter();
    private static LongCounter s3UploadSizeInTotal = new NoopLongCounter();
    private static HistogramInstrument operationLatency;
    private static LongCounter objectNumInTotal = new NoopLongCounter();
    private static HistogramInstrument objectStageCost;
    private static LongCounter networkInboundUsageInTotal = new NoopLongCounter();
    private static LongCounter networkOutboundUsageInTotal = new NoopLongCounter();
    private static ObservableLongGauge networkInboundAvailableBandwidth = new NoopObservableLongGauge();
    private static ObservableLongGauge networkOutboundAvailableBandwidth = new NoopObservableLongGauge();
    private static ObservableLongGauge networkInboundLimiterQueueSize = new NoopObservableLongGauge();
    private static ObservableLongGauge networkOutboundLimiterQueueSize = new NoopObservableLongGauge();
    private static HistogramInstrument networkInboundLimiterQueueTime;
    private static HistogramInstrument networkOutboundLimiterQueueTime;
    private static HistogramInstrument readAheadSize;
    private static HistogramInstrument readAheadStageTime;
    private static HistogramInstrument readS3LimiterTime;
    private static HistogramInstrument writeS3LimiterTime;
    private static HistogramInstrument getIndexTime;
    private static HistogramInstrument readBlockCacheTime;
    private static ObservableLongGauge deltaWalStartOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge pendUploadWalBytes = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalTrimmedOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge blockCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightReadAheadSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3ReadQuota = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3WriteQuota = new NoopObservableLongGauge();
    private static ObservableLongGauge inflightWALUploadTasksCount = new NoopObservableLongGauge();
    private static ObservableLongGauge allocatedMemorySize = new NoopObservableLongGauge();
    private static ObservableLongGauge usedMemorySize = new NoopObservableLongGauge();
    private static ObservableLongGauge pendingStreamAppendLatencyMetrics = new NoopObservableLongGauge();
    private static ObservableLongGauge pendingStreamFetchLatencyMetrics = new NoopObservableLongGauge();
    private static ObservableLongGauge compactionDelayTimeMetrics = new NoopObservableLongGauge();
    private static LongCounter compactionReadSizeInTotal = new NoopLongCounter();
    private static LongCounter compactionWriteSizeInTotal = new NoopLongCounter();
    private static Supplier<Long> networkInboundAvailableBandwidthSupplier = () -> 0L;
    private static Supplier<Long> networkOutboundAvailableBandwidthSupplier = () -> 0L;
    private static Supplier<Integer> networkInboundLimiterQueueSizeSupplier = () -> 0;
    private static Supplier<Integer> networkOutboundLimiterQueueSizeSupplier = () -> 0;
    private static Supplier<Integer> availableInflightReadAheadSizeSupplier = () -> 0;
    private static Supplier<Long> deltaWalStartOffsetSupplier = () -> 0L;
    private static Supplier<Long> deltaWalPendingUploadBytesSupplier = () -> 0L;
    private static Supplier<Long> deltaWalTrimmedOffsetSupplier = () -> 0L;
    private static Supplier<Long> deltaWALCacheSizeSupplier = () -> 0L;
    private static Supplier<Long> blockCacheSizeSupplier = () -> 0L;
    private static Supplier<Long> compactionDelayTimeSupplier = () -> 0L;
    private static LongCounter blockCacheOpsThroughput = new NoopLongCounter();

    private static Map<Integer, Supplier<Integer>> availableInflightS3ReadQuotaSupplier = new ConcurrentHashMap<>();
    private static Map<Integer, Supplier<Integer>> availableInflightS3WriteQuotaSupplier = new ConcurrentHashMap<>();
    private static Supplier<Integer> inflightWALUploadTasksCountSupplier = () -> 0;
    private static Map<Long, Supplier<Long>> pendingStreamAppendLatencySupplier = new ConcurrentHashMap<>();
    private static Map<Long, Supplier<Long>> pendingStreamFetchLatencySupplier = new ConcurrentHashMap<>();
    private static MetricsConfig metricsConfig = new MetricsConfig(MetricsLevel.INFO, Attributes.empty());
    private static final MultiAttributes<String> ALLOC_TYPE_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
        S3StreamMetricsConstant.LABEL_TYPE);
    private static final MultiAttributes<String> OPERATOR_INDEX_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamMetricsConstant.LABEL_INDEX);


    static {
        BASE_ATTRIBUTES_LISTENERS.add(ALLOC_TYPE_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(OPERATOR_INDEX_ATTRIBUTES);
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
        operationLatency = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.OPERATION_LATENCY_METRIC_NAME,
            "Operation latency", "nanoseconds", () -> OPERATION_LATENCY_METRICS);
        objectNumInTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.OBJECT_COUNT_METRIC_NAME)
            .setDescription("Objects count")
            .build();
        objectStageCost = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.OBJECT_STAGE_COST_METRIC_NAME,
            "Objects stage cost", "nanoseconds", () -> OBJECT_STAGE_METRICS);
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
        networkInboundLimiterQueueTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.NETWORK_INBOUND_LIMITER_QUEUE_TIME_METRIC_NAME,
            "Network inbound limiter queue time", "nanoseconds", () -> NETWORK_INBOUND_LIMITER_QUEUE_TIME_METRICS);
        networkOutboundLimiterQueueTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.NETWORK_OUTBOUND_LIMITER_QUEUE_TIME_METRIC_NAME,
            "Network outbound limiter queue time", "nanoseconds", () -> NETWORK_OUTBOUND_LIMITER_QUEUE_TIME_METRICS);
        readAheadSize = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.READ_AHEAD_SIZE_METRIC_NAME,
            "Read ahead size", "bytes", () -> READ_AHEAD_SIZE_METRICS);
        readAheadStageTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.READ_AHEAD_STAGE_TIME_METRIC_NAME,
                "Read ahead stage time", "nanoseconds", () -> READ_AHEAD_STAGE_TIME_METRICS);
        readS3LimiterTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.READ_S3_LIMITER_TIME_METRIC_NAME,
                "Time blocked on waiting for inflight read quota", "nanoseconds", () -> READ_S3_METRICS);
        writeS3LimiterTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.WRITE_S3_LIMITER_TIME_METRIC_NAME,
                "Time blocked on waiting for inflight write quota", "nanoseconds", () -> WRITE_S3_METRICS);
        getIndexTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.GET_INDEX_TIME_METRIC_NAME,
                "Get index time", "nanoseconds", () -> GET_INDEX_METRICS);
        readBlockCacheTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.READ_BLOCK_CACHE_METRIC_NAME,
                "Read block cache time", "nanoseconds", () -> READ_BLOCK_CACHE_TIME_METRICS);
        deltaWalStartOffset = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.WAL_START_OFFSET)
            .setDescription("Delta WAL start offset")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(deltaWalStartOffsetSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });

        pendUploadWalBytes = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.WAL_PENDING_UPLOAD_BYTES)
            .setDescription("Delta WAL pending upload bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                result.record(deltaWalPendingUploadBytesSupplier.get(), metricsConfig.getBaseAttributes());
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
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(deltaWALCacheSizeSupplier.get(), metricsConfig.getBaseAttributes());
                }
            });
        blockCacheSize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.BLOCK_CACHE_SIZE)
            .setDescription("Block cache size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
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
                    for (Map.Entry<Integer, Supplier<Integer>> entry : availableInflightS3ReadQuotaSupplier.entrySet()) {
                        result.record((long) entry.getValue().get(), OPERATOR_INDEX_ATTRIBUTES.get(String.valueOf(entry.getKey())));
                    }
                }
            });
        availableInflightS3WriteQuota = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.AVAILABLE_S3_INFLIGHT_WRITE_QUOTA_METRIC_NAME)
            .setDescription("Available inflight S3 write quota")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    for (Map.Entry<Integer, Supplier<Integer>> entry : availableInflightS3WriteQuotaSupplier.entrySet()) {
                        result.record((long) entry.getValue().get(), OPERATOR_INDEX_ATTRIBUTES.get(String.valueOf(entry.getKey())));
                    }
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
        allocatedMemorySize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.BUFFER_ALLOCATED_MEMORY_SIZE_METRIC_NAME)
            .setDescription("Buffer allocated memory size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && ByteBufAlloc.byteBufAllocMetric != null) {
                    Map<String, Long> allocateSizeMap = ByteBufAlloc.byteBufAllocMetric.getDetailedMap();
                    for (Map.Entry<String, Long> entry : allocateSizeMap.entrySet()) {
                        result.record(entry.getValue(), ALLOC_TYPE_ATTRIBUTES.get(entry.getKey()));
                    }
                }
            });
        usedMemorySize = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.BUFFER_USED_MEMORY_SIZE_METRIC_NAME)
            .setDescription("Buffer used memory size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel()) && ByteBufAlloc.byteBufAllocMetric != null) {
                    result.record(ByteBufAlloc.byteBufAllocMetric.getUsedMemory(), metricsConfig.getBaseAttributes());
                }
            });
        pendingStreamAppendLatencyMetrics = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.PENDING_STREAM_APPEND_LATENCY_METRIC_NAME)
                .setDescription("The maximum latency of pending stream append requests. NOTE: the minimum measurable " +
                        "latency depends on the reporting interval of this metrics.")
                .ofLongs()
                .setUnit("nanoseconds")
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        result.record(maxPendingStreamAppendLatency(), metricsConfig.getBaseAttributes());
                    }
                });
        pendingStreamFetchLatencyMetrics = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.PENDING_STREAM_FETCH_LATENCY_METRIC_NAME)
                .setDescription("The maximum latency of pending stream append requests. NOTE: the minimum measurable " +
                        "latency depends on the reporting interval of this metrics.")
                .ofLongs()
                .setUnit("nanoseconds")
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        result.record(maxPendingStreamFetchLatency(), metricsConfig.getBaseAttributes());
                    }
                });
        blockCacheOpsThroughput = meter.counterBuilder(prefix + S3StreamMetricsConstant.READ_BLOCK_CACHE_THROUGHPUT_METRIC_NAME)
            .setDescription("Block cache operation throughput")
            .setUnit("bytes")
            .build();
        compactionDelayTimeMetrics = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.COMPACTION_DELAY_TIME_METRIC_NAME)
            .setDescription("Compaction delay time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(compactionDelayTimeSupplier.get(), metricsConfig.getBaseAttributes());
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

    public static void registerDeltaWalPendingUploadBytesSupplier(Supplier<Long> deltaWalPendingUploadBytesSupplier) {
        S3StreamMetricsManager.deltaWalPendingUploadBytesSupplier = deltaWalPendingUploadBytesSupplier;
    }

    public static void registerDeltaWalCacheSizeSupplier(Supplier<Long> deltaWalCacheSizeSupplier) {
        S3StreamMetricsManager.deltaWALCacheSizeSupplier = deltaWalCacheSizeSupplier;
    }

    public static void registerBlockCacheSizeSupplier(Supplier<Long> blockCacheSizeSupplier) {
        S3StreamMetricsManager.blockCacheSizeSupplier = blockCacheSizeSupplier;
    }

    public static void registerInflightS3ReadQuotaSupplier(Supplier<Integer> inflightS3ReadQuotaSupplier, int index) {
        S3StreamMetricsManager.availableInflightS3ReadQuotaSupplier.putIfAbsent(index, inflightS3ReadQuotaSupplier);
    }

    public static void registerInflightS3WriteQuotaSupplier(Supplier<Integer> inflightS3WriteQuotaSupplier, int index) {
        S3StreamMetricsManager.availableInflightS3WriteQuotaSupplier.putIfAbsent(index, inflightS3WriteQuotaSupplier);
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
            CounterMetric metric = new CounterMetric(metricsConfig, () -> s3UploadSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildS3DownloadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, () -> s3DownloadSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildStageOperationMetric(MetricsLevel metricsLevel, S3Stage stage) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildAttributes(stage));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            OPERATION_LATENCY_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildOperationMetric(MetricsLevel metricsLevel, String operationType, String operationName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildOperationAttributes(operationType, operationName));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            OPERATION_LATENCY_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildOperationMetric(MetricsLevel metricsLevel, String operationType,
        String operationName, String status, String sizeLabelName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildOperationAttributesWithStatusAndSize(operationType, operationName, status, sizeLabelName));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            OPERATION_LATENCY_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildOperationMetric(MetricsLevel metricsLevel, String operationType, String operationName, String status) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildOperationAttributesWithStatus(operationType, operationName, status));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            OPERATION_LATENCY_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildReadAheadStageTimeMetric(MetricsLevel metricsLevel, String stage) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildAttributesStage(stage));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            READ_AHEAD_STAGE_TIME_METRICS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildObjectNumMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, () -> objectNumInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildObjectStageCostMetric(MetricsLevel metricsLevel, S3ObjectStage stage) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                AttributesUtils.buildAttributes(stage));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            OBJECT_STAGE_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildObjectUploadSizeMetric(MetricsLevel metricsLevel) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            OBJECT_STAGE_METRICS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildNetworkInboundUsageMetric(ThrottleStrategy strategy) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, AttributesUtils.buildAttributes(strategy), () -> networkInboundUsageInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildNetworkOutboundUsageMetric(ThrottleStrategy strategy) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, AttributesUtils.buildAttributes(strategy), () -> networkOutboundUsageInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildNetworkInboundLimiterQueueTimeMetric(MetricsLevel metricsLevel,
                                                                            ThrottleStrategy strategy) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildAttributes(strategy));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            NETWORK_INBOUND_LIMITER_QUEUE_TIME_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildNetworkOutboundLimiterQueueTimeMetric(MetricsLevel metricsLevel,
                                                                             ThrottleStrategy strategy) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildAttributes(strategy));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            NETWORK_OUTBOUND_LIMITER_QUEUE_TIME_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildReadAheadSizeMetric(MetricsLevel metricsLevel, String status) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildAttributes(status));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            READ_AHEAD_SIZE_METRICS.add(metric);
            return metric;
        }

    }

    public static HistogramMetric buildReadBlockCacheStageTime(MetricsLevel metricsLevel, String status, String stage) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    AttributesUtils.buildStatusStageAttributes(status, stage));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            READ_BLOCK_CACHE_TIME_METRICS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildBlockCacheOpsThroughputMetric(String ops) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, Attributes.builder()
                .put(AttributeKey.stringKey("ops"), ops)
                .build(), () -> blockCacheOpsThroughput);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildReadS3LimiterTimeMetric(MetricsLevel metricsLevel, int index) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    Attributes.of(S3StreamMetricsConstant.LABEL_INDEX, String.valueOf(index)));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            READ_S3_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildWriteS3LimiterTimeMetric(MetricsLevel metricsLevel, int index) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    Attributes.of(S3StreamMetricsConstant.LABEL_INDEX, String.valueOf(index)));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            WRITE_S3_METRICS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildGetIndexTimeMetric(MetricsLevel metricsLevel, String stage) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig,
                    Attributes.of(S3StreamMetricsConstant.LABEL_STAGE, stage));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            GET_INDEX_METRICS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildCompactionReadSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, () -> compactionReadSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildCompactionWriteSizeMetric() {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, () -> compactionWriteSizeInTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static void registerPendingStreamAppendLatencySupplier(long streamId, Supplier<Long> pendingStreamAppendLatencySupplier) {
        S3StreamMetricsManager.pendingStreamAppendLatencySupplier.put(streamId, pendingStreamAppendLatencySupplier);
    }

    public static void registerPendingStreamFetchLatencySupplier(long streamId, Supplier<Long> pendingStreamFetchLatencySupplier) {
        S3StreamMetricsManager.pendingStreamFetchLatencySupplier.put(streamId, pendingStreamFetchLatencySupplier);
    }

    public static void removePendingStreamAppendLatencySupplier(long streamId) {
        S3StreamMetricsManager.pendingStreamAppendLatencySupplier.remove(streamId);
    }

    public static void removePendingStreamFetchLatencySupplier(long streamId) {
        S3StreamMetricsManager.pendingStreamFetchLatencySupplier.remove(streamId);
    }

    public static long maxPendingStreamAppendLatency() {
        return pendingStreamAppendLatencySupplier.values().stream().map(Supplier::get).max(Long::compareTo).orElse(0L);
    }

    public static long maxPendingStreamFetchLatency() {
        return pendingStreamFetchLatencySupplier.values().stream().map(Supplier::get).max(Long::compareTo).orElse(0L);
    }

    public static void registerCompactionDelayTimeSuppler(Supplier<Long> compactionDelayTimeSupplier) {
        S3StreamMetricsManager.compactionDelayTimeSupplier = compactionDelayTimeSupplier;
    }
}
