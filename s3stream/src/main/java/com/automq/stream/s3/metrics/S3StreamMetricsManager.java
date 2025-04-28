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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramInstrument;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import static com.automq.stream.s3.metrics.S3StreamMetricsConstant.LABEL_CACHE_NAME;

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
    public static final List<HistogramMetric> GET_OBJECTS_TIME_METRICS = new CopyOnWriteArrayList<>();
    public static final List<HistogramMetric> OBJECTS_TO_SEARCH_COUNT_METRICS = new CopyOnWriteArrayList<>();
    private static LongCounter s3DownloadSizeInTotal = new NoopLongCounter();
    private static LongCounter s3UploadSizeInTotal = new NoopLongCounter();
    private static HistogramInstrument operationLatency;
    private static LongCounter objectNumInTotal = new NoopLongCounter();
    private static HistogramInstrument objectStageCost;
    private static LongCounter networkInboundUsageInTotal = new NoopLongCounter();
    private static LongCounter networkOutboundUsageInTotal = new NoopLongCounter();
    private static LongCounter nodeRangeIndexCacheOperationCountTotal = new NoopLongCounter();
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
    private static HistogramInstrument getObjectsTime;
    private static HistogramInstrument objectsToSearchCount;
    private static ObservableLongGauge deltaWalStartOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge pendUploadWalBytes = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalTrimmedOffset = new NoopObservableLongGauge();
    private static ObservableLongGauge deltaWalCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge blockCacheSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightReadAheadSize = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3ReadQuota = new NoopObservableLongGauge();
    private static ObservableLongGauge availableInflightS3WriteQuota = new NoopObservableLongGauge();

    private static ObservableLongGauge asyncCacheItemNumber = new NoopObservableLongGauge();
    private static ObservableLongGauge asyncCacheSizeNumber = new NoopObservableLongGauge();
    private static ObservableLongGauge asyncCacheMaxSizeNumber = new NoopObservableLongGauge();

    private static ObservableLongGauge inflightWALUploadTasksCount = new NoopObservableLongGauge();
    private static ObservableLongGauge allocatedMemorySize = new NoopObservableLongGauge();
    private static ObservableLongGauge usedMemorySize = new NoopObservableLongGauge();
    private static ObservableLongGauge pendingStreamAppendLatencyMetrics = new NoopObservableLongGauge();
    private static ObservableLongGauge pendingStreamFetchLatencyMetrics = new NoopObservableLongGauge();
    private static ObservableLongGauge compactionDelayTimeMetrics = new NoopObservableLongGauge();
    private static ObservableLongGauge localStreamRangeIndexCacheSizeMetrics = new NoopObservableLongGauge();
    private static ObservableLongGauge localStreamRangeIndexCacheStreamNumMetrics = new NoopObservableLongGauge();

    private static LongCounter asyncCacheEvictCount;
    private static LongCounter asyncCacheHitCount;
    private static LongCounter asyncCacheMissCount;
    private static LongCounter asyncCachePutCount;
    private static LongCounter asyncCachePopCount;
    private static LongCounter asyncCacheOverWriteCount;
    private static LongCounter asyncCacheRemoveNotCompletedCount;
    private static LongCounter asyncCacheRemoveCompletedCount;
    private static LongCounter asyncCacheItemCompleteExceptionallyCount;

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
    private static Supplier<Integer> localStreamRangeIndexCacheSize = () -> 0;
    private static Supplier<Integer> localStreamRangeIndexCacheStreamNum = () -> 0;
    private static LongCounter blockCacheOpsThroughput = new NoopLongCounter();

    private static Map<Integer, Supplier<Integer>> availableInflightS3ReadQuotaSupplier = new ConcurrentHashMap<>();
    private static Map<Integer, Supplier<Integer>> availableInflightS3WriteQuotaSupplier = new ConcurrentHashMap<>();

    private static Map<String, LongSupplier> asyncCacheItemNumberSupplier = new ConcurrentHashMap<>();
    private static Map<String, LongSupplier> asyncCacheSizeSupplier = new ConcurrentHashMap<>();
    private static Map<String, LongSupplier> asyncCacheMaxSizeSupplier = new ConcurrentHashMap<>();

    private static Supplier<Integer> inflightWALUploadTasksCountSupplier = () -> 0;
    private static Map<Long, Supplier<Long>> pendingStreamAppendLatencySupplier = new ConcurrentHashMap<>();
    private static Map<Long, Supplier<Long>> pendingStreamFetchLatencySupplier = new ConcurrentHashMap<>();
    private static MetricsConfig metricsConfig = new MetricsConfig(MetricsLevel.INFO, Attributes.empty());
    private static final MultiAttributes<String> ALLOC_TYPE_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
        S3StreamMetricsConstant.LABEL_TYPE);
    private static final MultiAttributes<String> OPERATOR_INDEX_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamMetricsConstant.LABEL_INDEX);

    // Broker Quota
    private static final MultiAttributes<String> BROKER_QUOTA_TYPE_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamMetricsConstant.LABEL_BROKER_QUOTA_TYPE);
    private static ObservableDoubleGauge brokerQuotaLimit = new NoopObservableDoubleGauge();
    private static Supplier<Map<String, Double>> brokerQuotaLimitSupplier = () -> new ConcurrentHashMap<>();

    static {
        BASE_ATTRIBUTES_LISTENERS.add(ALLOC_TYPE_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(OPERATOR_INDEX_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(BROKER_QUOTA_TYPE_ATTRIBUTES);
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
        nodeRangeIndexCacheOperationCountTotal = meter.counterBuilder(prefix + S3StreamMetricsConstant.NODE_RANGE_INDEX_CACHE_OPERATION_COUNT_METRIC_NAME)
            .setDescription("Range index cache operation count")
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
        getObjectsTime = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.GET_OBJECTS_TIME_METRIC_NAME,
                "Get objects time", "nanoseconds", () -> GET_OBJECTS_TIME_METRICS);
        objectsToSearchCount = new HistogramInstrument(meter, prefix + S3StreamMetricsConstant.OBJECTS_SEARCH_COUNT_METRIC_NAME,
                "Number of SSO object to search when get objects", "count", () -> OBJECTS_TO_SEARCH_COUNT_METRICS);
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
        localStreamRangeIndexCacheSizeMetrics = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.LOCAL_STREAM_RANGE_INDEX_CACHE_SIZE_METRIC_NAME)
            .setDescription("Local stream range index cache size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(localStreamRangeIndexCacheSize.get(), metricsConfig.getBaseAttributes());
                }
            });
        localStreamRangeIndexCacheStreamNumMetrics = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.LOCAL_STREAM_RANGE_INDEX_CACHE_STREAM_NUM_METRIC_NAME)
            .setDescription("Local stream range index cache stream number")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    result.record(localStreamRangeIndexCacheStreamNum.get(), metricsConfig.getBaseAttributes());
                }
            });

        initAsyncCacheMetrics(meter, prefix);
        initBrokerQuotaMetrics(meter, prefix);
    }

    private static void initAsyncCacheMetrics(Meter meter, String prefix) {
        asyncCacheEvictCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_EVICT_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache evict count")
            .setUnit("count")
            .build();

        asyncCacheHitCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_HIT_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache hit count")
            .setUnit("count")
            .build();

        asyncCacheMissCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_MISS_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache miss count")
            .setUnit("count")
            .build();

        asyncCachePutCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_PUT_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache put item count")
            .setUnit("count")
            .build();
        asyncCachePopCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_POP_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache pop item count")
            .setUnit("count")
            .build();
        asyncCacheOverWriteCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_OVERWRITE_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache overwrite item count")
            .setUnit("count")
            .build();
        asyncCacheRemoveNotCompletedCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_REMOVE_NOT_COMPLETE_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache remove not completed item count")
            .setUnit("count")
            .build();
        asyncCacheRemoveCompletedCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_REMOVE_COMPLETE_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache remove completed item count")
            .setUnit("count")
            .build();
        asyncCacheItemCompleteExceptionallyCount = meter.counterBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_ITEM_COMPLETE_EXCEPTIONALLY_COUNT_METRIC_NAME)
            .setDescription("AsyncLRU cache item complete exceptionally count")
            .setUnit("count")
            .build();

        asyncCacheItemNumber = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_ITEM_NUMBER_METRIC_NAME)
            .setDescription("AsyncLRU cache item number")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    for (Map.Entry<String, LongSupplier> entry : asyncCacheItemNumberSupplier.entrySet()) {
                        result.record(entry.getValue().getAsLong(), Attributes.of(LABEL_CACHE_NAME, entry.getKey()));
                    }
                }
            });
        asyncCacheSizeNumber = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_ITEM_SIZE_NAME)
            .setDescription("AsyncLRU cache size")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    for (Map.Entry<String, LongSupplier> entry : asyncCacheSizeSupplier.entrySet()) {
                        result.record(entry.getValue().getAsLong(), Attributes.of(LABEL_CACHE_NAME, entry.getKey()));
                    }
                }
            });
        asyncCacheMaxSizeNumber = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.ASYNC_CACHE_ITEM_MAX_SIZE_NAME)
            .setDescription("AsyncLRU cache max size")
            .ofLongs()
            .buildWithCallback(result -> {
                if (MetricsLevel.DEBUG.isWithin(metricsConfig.getMetricsLevel())) {
                    for (Map.Entry<String, LongSupplier> entry : asyncCacheMaxSizeSupplier.entrySet()) {
                        result.record(entry.getValue().getAsLong(), Attributes.of(LABEL_CACHE_NAME, entry.getKey()));
                    }
                }
            });
    }

    private static void initBrokerQuotaMetrics(Meter meter, String prefix) {
        brokerQuotaLimit = meter.gaugeBuilder(prefix + S3StreamMetricsConstant.BROKER_QUOTA_LIMIT_METRIC_NAME)
            .setDescription("Broker quota limit")
            .buildWithCallback(result -> {
                if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                    Map<String, Double> brokerQuotaLimitMap = brokerQuotaLimitSupplier.get();
                    for (Map.Entry<String, Double> entry : brokerQuotaLimitMap.entrySet()) {
                        String quotaType = entry.getKey();
                        Double quotaLimit = entry.getValue();
                        // drop too large values
                        if (quotaLimit > 1e15) {
                            continue;
                        }
                        result.record(quotaLimit, BROKER_QUOTA_TYPE_ATTRIBUTES.get(quotaType));
                    }
                }
            });
    }

    public static void registerNetworkLimiterQueueSizeSupplier(AsyncNetworkBandwidthLimiter.Type type,
        Supplier<Integer> networkLimiterQueueSizeSupplier) {
        switch (type) {
            case INBOUND:
                S3StreamMetricsManager.networkInboundLimiterQueueSizeSupplier = networkLimiterQueueSizeSupplier;
                break;
            case OUTBOUND:
                S3StreamMetricsManager.networkOutboundLimiterQueueSizeSupplier = networkLimiterQueueSizeSupplier;
                break;
        }
    }

    public static void registerNetworkAvailableBandwidthSupplier(AsyncNetworkBandwidthLimiter.Type type, Supplier<Long> networkAvailableBandwidthSupplier) {
        switch (type) {
            case INBOUND:
                S3StreamMetricsManager.networkInboundAvailableBandwidthSupplier = networkAvailableBandwidthSupplier;
                break;
            case OUTBOUND:
                S3StreamMetricsManager.networkOutboundAvailableBandwidthSupplier = networkAvailableBandwidthSupplier;
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

    public static void registerAsyncCacheSizeSupplier(LongSupplier supplier, String cacheName) {
        S3StreamMetricsManager.asyncCacheSizeSupplier.put(cacheName, supplier);
    }

    public static void registerAsyncCacheItemNumberSupplier(LongSupplier supplier, String cacheName) {
        S3StreamMetricsManager.asyncCacheItemNumberSupplier.put(cacheName, supplier);
    }

    public static void registerAsyncCacheMaxSizeSupplier(LongSupplier supplier, String cacheName) {
        S3StreamMetricsManager.asyncCacheMaxSizeSupplier.put(cacheName, supplier);
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

    public static CounterMetric buildNetworkInboundUsageMetric(ThrottleStrategy strategy, Consumer<Long> callback) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, AttributesUtils.buildAttributes(strategy), () -> networkInboundUsageInTotal, callback);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildNetworkOutboundUsageMetric(ThrottleStrategy strategy, Consumer<Long> callback) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, AttributesUtils.buildAttributes(strategy), () -> networkOutboundUsageInTotal, callback);
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

    public static CounterMetric buildAsyncCacheEvictMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCacheEvictCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCacheHitMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCacheHitCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCacheMissMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCacheMissCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCachePutMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCachePutCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCachePopMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCachePopCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCacheOverwriteMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCacheOverWriteCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCacheRemoveNotCompleteMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCacheRemoveNotCompletedCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCacheRemoveCompleteMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCacheRemoveCompletedCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildAsyncCacheItemCompleteExceptionallyMetric(String cacheName) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig,
                Attributes.of(LABEL_CACHE_NAME, cacheName),
                () -> asyncCacheItemCompleteExceptionallyCount);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
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

    public static HistogramMetric buildGetObjectsTimeMetric(MetricsLevel metricsLevel, String status) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig, AttributesUtils.buildAttributes(status));
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            GET_OBJECTS_TIME_METRICS.add(metric);
            return metric;
        }
    }

    public static CounterMetric buildRangeIndexCacheOperationMetric(String type) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            CounterMetric metric = new CounterMetric(metricsConfig, Attributes.builder()
                .put(S3StreamMetricsConstant.LABEL_TYPE, type).build(), () -> nodeRangeIndexCacheOperationCountTotal);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            return metric;
        }
    }

    public static HistogramMetric buildObjectsToSearchMetric(MetricsLevel metricsLevel) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            HistogramMetric metric = new HistogramMetric(metricsLevel, metricsConfig);
            BASE_ATTRIBUTES_LISTENERS.add(metric);
            OBJECTS_TO_SEARCH_COUNT_METRICS.add(metric);
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

    public static void registerLocalStreamRangeIndexCacheSizeSupplier(Supplier<Integer> localStreamRangeIndexCacheSize) {
        S3StreamMetricsManager.localStreamRangeIndexCacheSize = localStreamRangeIndexCacheSize;
    }

    public static void registerLocalStreamRangeIndexCacheStreamNumSupplier(Supplier<Integer> localStreamRangeIndexCacheStreamNum) {
        S3StreamMetricsManager.localStreamRangeIndexCacheStreamNum = localStreamRangeIndexCacheStreamNum;
    }

    public static void registerBrokerQuotaLimitSupplier(Supplier<Map<String, Double>> brokerQuotaLimitSupplier) {
        S3StreamMetricsManager.brokerQuotaLimitSupplier = brokerQuotaLimitSupplier;
    }
}
