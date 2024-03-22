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
import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;
import com.yammer.metrics.core.MetricName;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StorageOperationStats {
    private volatile static StorageOperationStats instance = null;

    public final YammerHistogramMetric appendStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.APPEND_STORAGE.getUniqueKey()), MetricsLevel.INFO, S3Operation.APPEND_STORAGE);
    public final YammerHistogramMetric appendWALBeforeStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.APPEND_WAL_BEFORE.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.APPEND_WAL_BEFORE);
    public final YammerHistogramMetric appendWALBlockPolledStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.APPEND_WAL_BLOCK_POLLED.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.APPEND_WAL_BLOCK_POLLED);
    public final YammerHistogramMetric appendWALAwaitStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.APPEND_WAL_AWAIT.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.APPEND_WAL_AWAIT);
    public final YammerHistogramMetric appendWALWriteStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.APPEND_WAL_WRITE.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.APPEND_WAL_WRITE);
    public final YammerHistogramMetric appendWALAfterStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.APPEND_WAL_AFTER.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.APPEND_WAL_AFTER);
    public final YammerHistogramMetric appendWALCompleteStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.APPEND_WAL_COMPLETE.getUniqueKey()), MetricsLevel.INFO, S3Stage.APPEND_WAL_COMPLETE);
    public final YammerHistogramMetric appendCallbackStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.APPEND_STORAGE_APPEND_CALLBACK.getUniqueKey()), MetricsLevel.DEBUG, S3Operation.APPEND_STORAGE_APPEND_CALLBACK);
    public final YammerHistogramMetric appendWALFullStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.APPEND_STORAGE_WAL_FULL.getUniqueKey()), MetricsLevel.INFO, S3Operation.APPEND_STORAGE_WAL_FULL);
    public final YammerHistogramMetric appendLogCacheStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.APPEND_STORAGE_LOG_CACHE.getUniqueKey()), MetricsLevel.INFO, S3Operation.APPEND_STORAGE_LOG_CACHE);
    public final YammerHistogramMetric appendLogCacheFullStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.APPEND_STORAGE_LOG_CACHE_FULL.getUniqueKey()), MetricsLevel.INFO, S3Operation.APPEND_STORAGE_LOG_CACHE_FULL);
    public final YammerHistogramMetric uploadWALPrepareStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.UPLOAD_WAL_PREPARE.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.UPLOAD_WAL_PREPARE);
    public final YammerHistogramMetric uploadWALUploadStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.UPLOAD_WAL_UPLOAD.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.UPLOAD_WAL_UPLOAD);
    public final YammerHistogramMetric uploadWALCommitStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.UPLOAD_WAL_COMMIT.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.UPLOAD_WAL_COMMIT);
    public final YammerHistogramMetric uploadWALCompleteStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.UPLOAD_WAL_COMPLETE.getUniqueKey()), MetricsLevel.INFO, S3Stage.UPLOAD_WAL_COMPLETE);
    public final YammerHistogramMetric forceUploadWALAwaitStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.FORCE_UPLOAD_WAL_AWAIT.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.FORCE_UPLOAD_WAL_AWAIT);
    public final YammerHistogramMetric forceUploadWALCompleteStats = S3StreamMetricsManager.buildStageOperationMetric(
        new MetricName(StorageOperationStats.class, S3Stage.FORCE_UPLOAD_WAL_COMPLETE.getUniqueKey()), MetricsLevel.DEBUG, S3Stage.FORCE_UPLOAD_WAL_COMPLETE);
    public final YammerHistogramMetric readStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.READ_STORAGE.getUniqueKey()), MetricsLevel.INFO, S3Operation.READ_STORAGE);
    private final YammerHistogramMetric readLogCacheHitStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.READ_STORAGE_LOG_CACHE.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_HIT),
        MetricsLevel.INFO, S3Operation.READ_STORAGE_LOG_CACHE, S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final YammerHistogramMetric readLogCacheMissStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.READ_STORAGE_LOG_CACHE.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_MISS),
        MetricsLevel.INFO, S3Operation.READ_STORAGE_LOG_CACHE, S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final YammerHistogramMetric readBlockCacheHitStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.READ_STORAGE_BLOCK_CACHE.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_HIT),
        MetricsLevel.INFO, S3Operation.READ_STORAGE_BLOCK_CACHE, S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final YammerHistogramMetric readBlockCacheMissStats = S3StreamMetricsManager.buildOperationMetric(
        new MetricName(StorageOperationStats.class, S3Operation.READ_STORAGE_BLOCK_CACHE.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_MISS),
        MetricsLevel.INFO, S3Operation.READ_STORAGE_BLOCK_CACHE, S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final YammerHistogramMetric readAheadSyncTimeStats = S3StreamMetricsManager.buildOperationMetric(
            new MetricName(StorageOperationStats.class, S3Operation.BLOCK_CACHE_READ_AHEAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_SYNC),
            MetricsLevel.INFO, S3Operation.BLOCK_CACHE_READ_AHEAD, S3StreamMetricsConstant.LABEL_STATUS_SYNC);
    private final YammerHistogramMetric readAheadAsyncTimeStats = S3StreamMetricsManager.buildOperationMetric(
            new MetricName(StorageOperationStats.class, S3Operation.BLOCK_CACHE_READ_AHEAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STATUS_ASYNC),
            MetricsLevel.INFO, S3Operation.BLOCK_CACHE_READ_AHEAD, S3StreamMetricsConstant.LABEL_STATUS_ASYNC);
    private final YammerHistogramMetric readAheadGetIndicesTimeStats = S3StreamMetricsManager.buildReadAheadStageTimeMetric(
        new MetricName(StorageOperationStats.class, S3Operation.BLOCK_CACHE_READ_AHEAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STAGE_GET_INDICES),
        MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_GET_INDICES);
    private final YammerHistogramMetric readAheadThrottleTimeStats = S3StreamMetricsManager.buildReadAheadStageTimeMetric(
            new MetricName(StorageOperationStats.class, S3Operation.BLOCK_CACHE_READ_AHEAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STAGE_THROTTLE),
            MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_THROTTLE);
    private final YammerHistogramMetric readAheadReadS3TimeStats = S3StreamMetricsManager.buildReadAheadStageTimeMetric(
            new MetricName(StorageOperationStats.class, S3Operation.BLOCK_CACHE_READ_AHEAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STAGE_READ_S3),
            MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_READ_S3);
    private final YammerHistogramMetric readAheadPutBlockCacheTimeStats = S3StreamMetricsManager.buildReadAheadStageTimeMetric(
            new MetricName(StorageOperationStats.class, S3Operation.BLOCK_CACHE_READ_AHEAD.getUniqueKey() + S3StreamMetricsConstant.LABEL_STAGE_PUT_BLOCK_CACHE),
            MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_PUT_BLOCK_CACHE);

    private final Map<String, YammerHistogramMetric> getIndexTimeStatsMap = new ConcurrentHashMap<>();
    private final Map<Integer, YammerHistogramMetric> readS3LimiterStatsMap = new ConcurrentHashMap<>();
    private final Map<Integer, YammerHistogramMetric> writeS3LimiterStatsMap = new ConcurrentHashMap<>();
    public final YammerHistogramMetric readAheadSyncSizeStats = S3StreamMetricsManager.buildReadAheadSizeMetric(
        new MetricName(StorageOperationStats.class, "ReadAheadSize-" + S3StreamMetricsConstant.LABEL_STATUS_SYNC),
            MetricsLevel.INFO, S3StreamMetricsConstant.LABEL_STATUS_SYNC);
    public final YammerHistogramMetric readAheadAsyncSizeStats = S3StreamMetricsManager.buildReadAheadSizeMetric(
            new MetricName(StorageOperationStats.class, "ReadAheadSize-" + S3StreamMetricsConstant.LABEL_STATUS_ASYNC),
            MetricsLevel.INFO, S3StreamMetricsConstant.LABEL_STATUS_ASYNC);

    public final YammerHistogramMetric readBlockCacheTimeStats = S3StreamMetricsManager.buildReadBlockCacheTime(
            new MetricName(StorageOperationStats.class, "ReadBlockCacheTime"), MetricsLevel.INFO);

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

    public YammerHistogramMetric readAheadStageTimeStats(String stage) {
        switch (stage) {
            case S3StreamMetricsConstant.LABEL_STAGE_GET_INDICES:
                return readAheadGetIndicesTimeStats;
            case S3StreamMetricsConstant.LABEL_STAGE_THROTTLE:
                return readAheadThrottleTimeStats;
            case S3StreamMetricsConstant.LABEL_STAGE_READ_S3:
                return readAheadReadS3TimeStats;
            default:
                return readAheadPutBlockCacheTimeStats;
        }
    }

    public YammerHistogramMetric getIndexTimeStats(String stage) {
        return this.getIndexTimeStatsMap.computeIfAbsent(stage, k -> S3StreamMetricsManager.buildGetIndexTimeMetric(
                new MetricName(StorageOperationStats.class, "GetIndexTime-" + stage), MetricsLevel.DEBUG, stage));
    }

    public YammerHistogramMetric readS3LimiterStats(int index) {
        return this.readS3LimiterStatsMap.computeIfAbsent(index, k -> S3StreamMetricsManager.buildReadS3LimiterTimeMetric(
                new MetricName(StorageOperationStats.class, "ReadS3Limiter-" + index), MetricsLevel.DEBUG, index));
    }

    public YammerHistogramMetric writeS3LimiterStats(int index) {
        return this.writeS3LimiterStatsMap.computeIfAbsent(index, k -> S3StreamMetricsManager.buildWriteS3LimiterTimeMetric(
                new MetricName(StorageOperationStats.class, "WriteS3Limiter-" + index), MetricsLevel.DEBUG, index));
    }

    public YammerHistogramMetric readAheadSizeStats(boolean isSync) {
        return isSync ? readAheadSyncSizeStats : readAheadAsyncSizeStats;
    }
}
