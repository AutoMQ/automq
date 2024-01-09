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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3Stage;
import com.automq.stream.s3.metrics.wrapper.HistogramMetric;

public class StorageOperationStats {
    private volatile static StorageOperationStats instance = null;

    public final HistogramMetric appendStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.APPEND_STORAGE);
    public final HistogramMetric appendWALBeforeStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.APPEND_WAL_BEFORE);
    public final HistogramMetric appendWALBlockPolledStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.APPEND_WAL_BLOCK_POLLED);
    public final HistogramMetric appendWALAwaitStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.APPEND_WAL_AWAIT);
    public final HistogramMetric appendWALWriteStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.APPEND_WAL_WRITE);
    public final HistogramMetric appendWALAfterStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.APPEND_WAL_AFTER);
    public final HistogramMetric appendWALCompleteStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.APPEND_WAL_COMPLETE);
    public final HistogramMetric appendCallbackStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.APPEND_STORAGE_APPEND_CALLBACK);
    public final HistogramMetric appendWALFullStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.APPEND_STORAGE_WAL_FULL);
    public final HistogramMetric appendLogCacheStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.APPEND_STORAGE_LOG_CACHE);
    public final HistogramMetric appendLogCacheFullStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.APPEND_STORAGE_LOG_CACHE_FULL);
    public final HistogramMetric uploadWALPrepareStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.UPLOAD_WAL_PREPARE);
    public final HistogramMetric uploadWALUploadStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.UPLOAD_WAL_UPLOAD);
    public final HistogramMetric uploadWALCommitStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.UPLOAD_WAL_COMMIT);
    public final HistogramMetric uploadWALCompleteStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.UPLOAD_WAL_COMPLETE);
    public final HistogramMetric forceUploadWALAwaitStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.FORCE_UPLOAD_WAL_AWAIT);
    public final HistogramMetric forceUploadWALCompleteStats = S3StreamMetricsManager.buildStageOperationMetric(S3Stage.FORCE_UPLOAD_WAL_COMPLETE);
    public final HistogramMetric readStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.READ_STORAGE);
    private final HistogramMetric readLogCacheHitStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.READ_STORAGE_LOG_CACHE, S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final HistogramMetric readLogCacheMissStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.READ_STORAGE_LOG_CACHE, S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final HistogramMetric readBlockCacheHitStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.READ_STORAGE_BLOCK_CACHE, S3StreamMetricsConstant.LABEL_STATUS_HIT);
    private final HistogramMetric readBlockCacheMissStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.READ_STORAGE_BLOCK_CACHE, S3StreamMetricsConstant.LABEL_STATUS_MISS);
    private final HistogramMetric blockCacheReadAheadSyncStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.BLOCK_CACHE_READ_AHEAD, S3StreamMetricsConstant.LABEL_STATUS_SYNC);
    private final HistogramMetric blockCacheReadAheadAsyncStats = S3StreamMetricsManager.buildOperationMetric(S3Operation.BLOCK_CACHE_READ_AHEAD, S3StreamMetricsConstant.LABEL_STATUS_ASYNC);
    public final HistogramMetric readAheadSizeStats = S3StreamMetricsManager.buildReadAheadSizeMetric();

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

    public HistogramMetric blockCacheReadAheadStats(boolean isSync) {
        return isSync ? blockCacheReadAheadSyncStats : blockCacheReadAheadAsyncStats;
    }
}
