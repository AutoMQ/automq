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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StorageOperationStats {
    private volatile static StorageOperationStats instance = null;

    public final YammerHistogramMetric appendStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE);
    public final YammerHistogramMetric appendWALBeforeStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.APPEND_WAL_BEFORE);
    public final YammerHistogramMetric appendWALBlockPolledStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.APPEND_WAL_BLOCK_POLLED);
    public final YammerHistogramMetric appendWALAwaitStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.APPEND_WAL_AWAIT);
    public final YammerHistogramMetric appendWALWriteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.APPEND_WAL_WRITE);
    public final YammerHistogramMetric appendWALAfterStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.APPEND_WAL_AFTER);
    public final YammerHistogramMetric appendWALCompleteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.APPEND_WAL_COMPLETE);
    public final YammerHistogramMetric appendCallbackStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.DEBUG, S3Operation.APPEND_STORAGE_APPEND_CALLBACK);
    public final YammerHistogramMetric appendWALFullStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE_WAL_FULL);
    public final YammerHistogramMetric appendLogCacheStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE_LOG_CACHE);
    public final YammerHistogramMetric appendLogCacheFullStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE_LOG_CACHE_FULL);
    public final YammerHistogramMetric uploadWALPrepareStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.UPLOAD_WAL_PREPARE);
    public final YammerHistogramMetric uploadWALUploadStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.UPLOAD_WAL_UPLOAD);
    public final YammerHistogramMetric uploadWALCommitStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.UPLOAD_WAL_COMMIT);
    public final YammerHistogramMetric uploadWALCompleteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.UPLOAD_WAL_COMPLETE);
    public final YammerHistogramMetric forceUploadWALAwaitStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.FORCE_UPLOAD_WAL_AWAIT);
    public final YammerHistogramMetric forceUploadWALCompleteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.FORCE_UPLOAD_WAL_COMPLETE);
    public final YammerHistogramMetric readStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE);
    private final YammerHistogramMetric readLogCacheHitStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_LOG_CACHE, S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final YammerHistogramMetric readLogCacheMissStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_LOG_CACHE, S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final YammerHistogramMetric readBlockCacheHitStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_BLOCK_CACHE, S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final YammerHistogramMetric readBlockCacheMissStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_BLOCK_CACHE, S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final YammerHistogramMetric readAheadSyncTimeStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.BLOCK_CACHE_READ_AHEAD, S3StreamMetricsConstant.LABEL_STATUS_SYNC);
    private final YammerHistogramMetric readAheadAsyncTimeStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.BLOCK_CACHE_READ_AHEAD, S3StreamMetricsConstant.LABEL_STATUS_ASYNC);
    public final YammerHistogramMetric readAheadGetIndicesTimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_GET_INDICES);
    public final YammerHistogramMetric readAheadThrottleTimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_THROTTLE);
    public final YammerHistogramMetric readAheadReadS3TimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_READ_S3);
    public final YammerHistogramMetric readAheadPutBlockCacheTimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_PUT_BLOCK_CACHE);

    public final YammerHistogramMetric getIndicesTimeGetObjectStats = S3StreamMetricsManager
            .buildGetIndexTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_GET_OBJECTS);
    public final YammerHistogramMetric getIndicesTimeFindIndexStats = S3StreamMetricsManager
            .buildGetIndexTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_FIND_INDEX);
    public final YammerHistogramMetric getIndicesTimeComputeStats = S3StreamMetricsManager
            .buildGetIndexTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_COMPUTE);

    private final Map<Integer, YammerHistogramMetric> readS3LimiterStatsMap = new ConcurrentHashMap<>();
    private final Map<Integer, YammerHistogramMetric> writeS3LimiterStatsMap = new ConcurrentHashMap<>();
    public final YammerHistogramMetric readAheadSyncSizeStats = S3StreamMetricsManager
            .buildReadAheadSizeMetric(MetricsLevel.INFO, S3StreamMetricsConstant.LABEL_STATUS_SYNC);
    public final YammerHistogramMetric readAheadAsyncSizeStats = S3StreamMetricsManager
            .buildReadAheadSizeMetric(MetricsLevel.INFO, S3StreamMetricsConstant.LABEL_STATUS_ASYNC);

    public final YammerHistogramMetric readBlockCacheStageMissWaitInflightTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_WAIT_INFLIGHT);
    public final YammerHistogramMetric readBlockCacheStageMissReadCacheTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_READ_CACHE);
    public final YammerHistogramMetric readBlockCacheStageMissReadAheadTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_READ_AHEAD);
    public final YammerHistogramMetric readBlockCacheStageMissReadS3TimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_READ_S3);
    public final YammerHistogramMetric readBlockCacheStageHitWaitInflightTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_WAIT_INFLIGHT);
    public final YammerHistogramMetric readBlockCacheStageHitReadCacheTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_READ_CACHE);
    public final YammerHistogramMetric readBlockCacheStageHitReadAheadTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_READ_AHEAD);
    public final YammerHistogramMetric readBlockCacheStageHitReadS3TimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_READ_S3);

    public final CounterMetric blockCacheReadS3Throughput = S3StreamMetricsManager.buildBlockCacheOpsThroughputMetric("read_s3");
    public final CounterMetric blockCacheBlockHitThroughput = S3StreamMetricsManager.buildBlockCacheOpsThroughputMetric("block_hit");
    public final CounterMetric blockCacheBlockMissThroughput = S3StreamMetricsManager.buildBlockCacheOpsThroughputMetric("block_miss");
    public final CounterMetric blockCacheBlockEvictThroughput = S3StreamMetricsManager.buildBlockCacheOpsThroughputMetric("block_evict");
    public final CounterMetric blockCacheReadStreamThroughput = S3StreamMetricsManager.buildBlockCacheOpsThroughputMetric("read_stream");
    public final CounterMetric blockCacheReadaheadThroughput = S3StreamMetricsManager.buildBlockCacheOpsThroughputMetric("readahead");

    private StorageOperationStats() {
    }

    public static StorageOperationStats getInstance() {
        if (instance == null) {
            synchronized (StorageOperationStats.class) {
                if (instance == null) {
                    instance = new StorageOperationStats();
                }
            }
        }
        return instance;
    }

    public YammerHistogramMetric readLogCacheStats(boolean isCacheHit) {
        return isCacheHit ? readLogCacheHitStats : readLogCacheMissStats;
    }

    public YammerHistogramMetric readBlockCacheStats(boolean isCacheHit) {
        return isCacheHit ? readBlockCacheHitStats : readBlockCacheMissStats;
    }

    public YammerHistogramMetric readAheadTimeStats(boolean isSync) {
        return isSync ? readAheadSyncTimeStats : readAheadAsyncTimeStats;
    }

    public YammerHistogramMetric readS3LimiterStats(int index) {
        return this.readS3LimiterStatsMap.computeIfAbsent(index, k -> S3StreamMetricsManager.buildReadS3LimiterTimeMetric(MetricsLevel.DEBUG, index));
    }

    public YammerHistogramMetric writeS3LimiterStats(int index) {
        return this.writeS3LimiterStatsMap.computeIfAbsent(index, k -> S3StreamMetricsManager.buildWriteS3LimiterTimeMetric(MetricsLevel.DEBUG, index));
    }

    public YammerHistogramMetric readAheadSizeStats(boolean isSync) {
        return isSync ? readAheadSyncSizeStats : readAheadAsyncSizeStats;
    }
}
