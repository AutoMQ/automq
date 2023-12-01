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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.OperationMetricsStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OFFSET;

public class DefaultS3BlockCache implements S3BlockCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3BlockCache.class);
    private final Map<ReadAheadTaskKey, CompletableFuture<Void>> inflightReadAheadTasks = new ConcurrentHashMap<>();
    private final BlockCache cache;
    private final ExecutorService mainExecutor;
    private final ReadAheadManager readAheadManager;
    private final StreamReader streamReader;
    private final InflightReadThrottle inflightReadThrottle;

    public DefaultS3BlockCache(Config config, ObjectManager objectManager, S3Operator s3Operator) {
        int blockSize = config.objectBlockSize();

        this.cache = new BlockCache(config.blockCacheSize());
        this.readAheadManager = new ReadAheadManager(blockSize, this.cache);
        this.mainExecutor = Threads.newFixedThreadPoolWithMonitor(
                2,
                "s3-block-cache-main",
                false,
                LOGGER);
        this.inflightReadThrottle = new InflightReadThrottle();
        this.streamReader = new StreamReader(s3Operator, objectManager, cache, inflightReadAheadTasks, inflightReadThrottle);
    }

    public void shutdown() {
        this.mainExecutor.shutdown();
        this.streamReader.shutdown();
        this.inflightReadThrottle.shutdown();

    }

    @Override
    public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] read data, stream={}, {}-{}, total bytes: {} ", streamId, startOffset, endOffset, maxBytes);
        }
        this.readAheadManager.updateReadProgress(streamId, startOffset);
        TimerUtil timerUtil = new TimerUtil();
        CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
        ReadAheadAgent agent = this.readAheadManager.getOrCreateReadAheadAgent(streamId, startOffset);
        UUID uuid = UUID.randomUUID();
        // submit read task to mainExecutor to avoid read slower the caller thread.
        mainExecutor.execute(() -> {
            FutureUtil.exec(() -> {
                read0(streamId, startOffset, endOffset, maxBytes, agent, uuid).whenComplete((ret, ex) -> {
                    if (ex != null) {
                        LOGGER.error("read {} [{}, {}) from block cache fail", streamId, startOffset, endOffset, ex);
                        readCf.completeExceptionally(ex);
                        return;
                    }
                    int totalReturnedSize = ret.getRecords().stream().mapToInt(StreamRecordBatch::size).sum();
                    this.readAheadManager.updateReadResult(streamId, startOffset,
                            ret.getRecords().get(ret.getRecords().size() - 1).getLastOffset(), totalReturnedSize);

                    if (ret.getCacheAccessType() == CacheAccessType.BLOCK_CACHE_HIT) {
                        OperationMetricsStats.getCounter(S3Operation.READ_STORAGE_BLOCK_CACHE).inc();
                    } else {
                        OperationMetricsStats.getCounter(S3Operation.READ_STORAGE_BLOCK_CACHE_MISS).inc();
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] read data complete, cache hit: {}, stream={}, {}-{}, total bytes: {} ",
                                ret.getCacheAccessType() == CacheAccessType.BLOCK_CACHE_HIT, streamId, startOffset, endOffset, totalReturnedSize);
                    }
                    OperationMetricsStats.getHistogram(S3Operation.READ_STORAGE_BLOCK_CACHE).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                    readCf.complete(ret);
                    this.inflightReadThrottle.release(uuid);
                });
            }, readCf, LOGGER, "read");
        });
        return readCf;
    }

    public CompletableFuture<ReadDataBlock> read0(long streamId, long startOffset, long endOffset, int maxBytes, ReadAheadAgent agent, UUID uuid) {
        if (startOffset >= endOffset || maxBytes <= 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(Collections.emptyList(), CacheAccessType.BLOCK_CACHE_MISS));
        }

        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;

        CompletableFuture<Void> inflightReadAheadTask = inflightReadAheadTasks.get(new ReadAheadTaskKey(streamId, nextStartOffset));
        if (inflightReadAheadTask != null) {
            CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
            inflightReadAheadTask.whenComplete((nil, ex) -> FutureUtil.propagate(read0(streamId, startOffset, endOffset, maxBytes, agent, uuid), readCf));
            return readCf;
        }

        // 1. get from cache
        BlockCache.GetCacheResult cacheRst = cache.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
        List<StreamRecordBatch> cacheRecords = cacheRst.getRecords();
        if (!cacheRecords.isEmpty()) {
            asyncReadAhead(streamId, agent, cacheRst.getReadAheadRecords());
            nextStartOffset = cacheRecords.get(cacheRecords.size() - 1).getLastOffset();
            nextMaxBytes -= Math.min(nextMaxBytes, cacheRecords.stream().mapToInt(StreamRecordBatch::size).sum());
            if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                // cache hit
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] read data hit cache, stream={}, {}-{}, total bytes: {} ", streamId, startOffset, endOffset, maxBytes);
                }
                return CompletableFuture.completedFuture(new ReadDataBlock(cacheRecords, CacheAccessType.BLOCK_CACHE_HIT));
            } else {
                // cache partially hit
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] read data partially hit cache, stream={}, {}-{}, total bytes: {} ", streamId, nextStartOffset, endOffset, nextMaxBytes);
                }
                return read0(streamId, nextStartOffset, endOffset, nextMaxBytes, agent, uuid).thenApply(rst -> {
                    List<StreamRecordBatch> records = new ArrayList<>(cacheRecords);
                    records.addAll(rst.getRecords());
                    return new ReadDataBlock(records, CacheAccessType.BLOCK_CACHE_MISS);
                });
            }
        }

        // 2. get from s3
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] read data cache miss, stream={}, {}-{}, total bytes: {} ", streamId, startOffset, endOffset, maxBytes);
        }
        return streamReader.syncReadAhead(streamId, startOffset, endOffset, maxBytes, agent, uuid)
                .thenApply(rst -> new ReadDataBlock(rst, CacheAccessType.BLOCK_CACHE_MISS));
    }

    private void asyncReadAhead(long streamId, ReadAheadAgent agent, List<ReadAheadRecord> readAheadRecords) {
        //TODO: read ahead only when there are enough inactive bytes to evict
        if (readAheadRecords.isEmpty()) {
            return;
        }
        ReadAheadRecord lastRecord = readAheadRecords.get(readAheadRecords.size() - 1);
        long nextRaOffset = lastRecord.nextRAOffset();
        int nextRaSize = agent.getNextReadAheadSize();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] async read ahead, stream={}, {}-{}, total bytes: {} ",
                    streamId, nextRaOffset, NOOP_OFFSET, nextRaSize);
        }

        // check if next ra hits cache
        if (cache.checkRange(streamId, nextRaOffset, nextRaSize)) {
            return;
        }

        streamReader.asyncReadAhead(streamId, nextRaOffset, NOOP_OFFSET, nextRaSize, agent);
    }

    public record ReadAheadTaskKey(long streamId, long startOffset) {

    }

    public record ReadAheadRecord(long nextRAOffset) {
    }

}
