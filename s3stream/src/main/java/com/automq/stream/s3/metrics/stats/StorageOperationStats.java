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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StorageOperationStats {
    private static volatile StorageOperationStats instance = null;

    public final HistogramMetric appendStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE.getType().getName(),
                S3Operation.APPEND_STORAGE.getName());
    public final HistogramMetric appendWALBeforeStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.APPEND_WAL_BEFORE);
    public final HistogramMetric appendWALBlockPolledStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.APPEND_WAL_BLOCK_POLLED);
    public final HistogramMetric appendWALAwaitStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.APPEND_WAL_AWAIT);
    public final HistogramMetric appendWALWriteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.APPEND_WAL_WRITE);
    public final HistogramMetric appendWALAfterStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.DEBUG, S3Stage.APPEND_WAL_AFTER);
    public final HistogramMetric appendWALCompleteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.APPEND_WAL_COMPLETE);
    public final HistogramMetric appendCallbackStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.DEBUG, S3Operation.APPEND_STORAGE_APPEND_CALLBACK.getType().getName(),
                S3Operation.APPEND_STORAGE_APPEND_CALLBACK.getName());
    public final HistogramMetric appendWALFullStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE_WAL_FULL.getType().getName(),
                S3Operation.APPEND_STORAGE_WAL_FULL.getName());
    public final HistogramMetric appendLogCacheStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE_LOG_CACHE.getType().getName(),
                S3Operation.APPEND_STORAGE_LOG_CACHE.getName());
    public final HistogramMetric appendLogCacheFullStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.APPEND_STORAGE_LOG_CACHE_FULL.getType().getName(),
                S3Operation.APPEND_STORAGE_LOG_CACHE_FULL.getName());
    public final HistogramMetric uploadWALPrepareStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.UPLOAD_WAL_PREPARE);
    public final HistogramMetric uploadWALUploadStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.UPLOAD_WAL_UPLOAD);
    public final HistogramMetric uploadWALCommitStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.UPLOAD_WAL_COMMIT);
    public final HistogramMetric uploadWALCompleteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.UPLOAD_WAL_COMPLETE);
    public final HistogramMetric forceUploadWALAwaitStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.FORCE_UPLOAD_WAL_AWAIT);
    public final HistogramMetric forceUploadWALCompleteStats = S3StreamMetricsManager
            .buildStageOperationMetric(MetricsLevel.INFO, S3Stage.FORCE_UPLOAD_WAL_COMPLETE);
    public final HistogramMetric readStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE.getType().getName(), S3Operation.READ_STORAGE.getName());
    private final HistogramMetric readLogCacheHitStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_LOG_CACHE.getType().getName(),
                S3Operation.READ_STORAGE_LOG_CACHE.getName(), S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final HistogramMetric readLogCacheMissStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_LOG_CACHE.getType().getName(),
                S3Operation.READ_STORAGE_LOG_CACHE.getName(), S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final HistogramMetric readBlockCacheHitStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_BLOCK_CACHE.getType().getName(),
                S3Operation.READ_STORAGE_BLOCK_CACHE.getName(), S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final HistogramMetric readBlockCacheMissStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.READ_STORAGE_BLOCK_CACHE.getType().getName(),
                S3Operation.READ_STORAGE_BLOCK_CACHE.getName(), S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final HistogramMetric readAheadSyncTimeStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.BLOCK_CACHE_READ_AHEAD.getType().getName(),
                S3Operation.BLOCK_CACHE_READ_AHEAD.getName(), S3StreamMetricsConstant.LABEL_STATUS_SYNC);
    private final HistogramMetric readAheadAsyncTimeStats = S3StreamMetricsManager
            .buildOperationMetric(MetricsLevel.INFO, S3Operation.BLOCK_CACHE_READ_AHEAD.getType().getName(),
                S3Operation.BLOCK_CACHE_READ_AHEAD.getName(), S3StreamMetricsConstant.LABEL_STATUS_ASYNC);
    public final HistogramMetric readAheadGetIndicesTimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_GET_INDICES);
    public final HistogramMetric readAheadThrottleTimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_THROTTLE);
    public final HistogramMetric readAheadReadS3TimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_READ_S3);
    public final HistogramMetric readAheadPutBlockCacheTimeStats = S3StreamMetricsManager
            .buildReadAheadStageTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_PUT_BLOCK_CACHE);

    public final HistogramMetric getIndicesTimeGetObjectStats = S3StreamMetricsManager
            .buildGetIndexTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_GET_OBJECTS);
    public final HistogramMetric getIndicesTimeFindIndexStats = S3StreamMetricsManager
            .buildGetIndexTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_FIND_INDEX);
    public final HistogramMetric getIndicesTimeComputeStats = S3StreamMetricsManager
            .buildGetIndexTimeMetric(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STAGE_COMPUTE);

    private final Map<Integer, HistogramMetric> readS3LimiterStatsMap = new ConcurrentHashMap<>();
    private final Map<Integer, HistogramMetric> writeS3LimiterStatsMap = new ConcurrentHashMap<>();
    public final HistogramMetric readAheadSyncSizeStats = S3StreamMetricsManager
            .buildReadAheadSizeMetric(MetricsLevel.INFO, S3StreamMetricsConstant.LABEL_STATUS_SYNC);
    public final HistogramMetric readAheadAsyncSizeStats = S3StreamMetricsManager
            .buildReadAheadSizeMetric(MetricsLevel.INFO, S3StreamMetricsConstant.LABEL_STATUS_ASYNC);

    public final HistogramMetric readBlockCacheStageMissWaitInflightTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_WAIT_INFLIGHT);
    public final HistogramMetric readBlockCacheStageMissReadCacheTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_READ_CACHE);
    public final HistogramMetric readBlockCacheStageMissReadAheadTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_READ_AHEAD);
    public final HistogramMetric readBlockCacheStageMissReadS3TimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_MISS, S3StreamMetricsConstant.LABEL_STAGE_READ_S3);
    public final HistogramMetric readBlockCacheStageHitWaitInflightTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_WAIT_INFLIGHT);
    public final HistogramMetric readBlockCacheStageHitReadCacheTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_READ_CACHE);
    public final HistogramMetric readBlockCacheStageHitReadAheadTimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_READ_AHEAD);
    public final HistogramMetric readBlockCacheStageHitReadS3TimeStats = S3StreamMetricsManager
            .buildReadBlockCacheStageTime(MetricsLevel.DEBUG, S3StreamMetricsConstant.LABEL_STATUS_HIT, S3StreamMetricsConstant.LABEL_STAGE_READ_S3);


    public final CounterMetric blockCacheReadS3Throughput = S3StreamMetricsManager.buildBlockCacheOpsThroughputMetric("read_s3");
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

    public HistogramMetric readLogCacheStats(boolean isCacheHit) {
        return isCacheHit ? readLogCacheHitStats : readLogCacheMissStats;
    }

    public HistogramMetric readBlockCacheStats(boolean isCacheHit) {
        return isCacheHit ? readBlockCacheHitStats : readBlockCacheMissStats;
    }

    public HistogramMetric readAheadTimeStats(boolean isSync) {
        return isSync ? readAheadSyncTimeStats : readAheadAsyncTimeStats;
    }

    public HistogramMetric readS3LimiterStats(int index) {
        return this.readS3LimiterStatsMap.computeIfAbsent(index, k -> S3StreamMetricsManager.buildReadS3LimiterTimeMetric(MetricsLevel.DEBUG, index));
    }

    public HistogramMetric writeS3LimiterStats(int index) {
        return this.writeS3LimiterStatsMap.computeIfAbsent(index, k -> S3StreamMetricsManager.buildWriteS3LimiterTimeMetric(MetricsLevel.DEBUG, index));
    }

    public HistogramMetric readAheadSizeStats(boolean isSync) {
        return isSync ? readAheadSyncSizeStats : readAheadAsyncSizeStats;
    }
}
