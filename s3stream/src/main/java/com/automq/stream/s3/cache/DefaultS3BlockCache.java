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

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.OperationMetricsStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OFFSET;

public class DefaultS3BlockCache implements S3BlockCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3BlockCache.class);
    private final LRUCache<Long, ObjectReader> objectReaderLRU = new LRUCache<>();
    private final Map<ReadingTaskKey, CompletableFuture<?>> readAheadTasks = new ConcurrentHashMap<>();
    private final Semaphore readAheadLimiter = new Semaphore(16);
    private final BlockCache cache;
    private final ExecutorService mainExecutor;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final DataBlockReadAccumulator dataBlockReadAccumulator;

    public DefaultS3BlockCache(long cacheBytesSize, ObjectManager objectManager, S3Operator s3Operator) {
        this.cache = new BlockCache(cacheBytesSize);
        this.mainExecutor = Threads.newFixedThreadPool(
                2,
                ThreadUtils.createThreadFactory("s3-block-cache-main-%d", false),
                LOGGER);
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.dataBlockReadAccumulator = new DataBlockReadAccumulator(dataBlockRecords -> {
            List<StreamRecordBatch> records = dataBlockRecords.records();
            if (!records.isEmpty()) {
                long streamId = records.get(0).getStreamId();
                records.forEach(StreamRecordBatch::retain);
                cache.put(streamId, records);
            }
        });
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        TimerUtil timerUtil = new TimerUtil(TimeUnit.MILLISECONDS);
        CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
        // submit read task to mainExecutor to avoid read slower the caller thread.
        mainExecutor.execute(() -> FutureUtil.exec(() ->
                FutureUtil.propagate(read0(streamId, startOffset, endOffset, maxBytes, true), readCf), readCf, LOGGER, "read")
        );
        readCf.whenComplete((ret, ex) -> {
            if (ex != null) {
                LOGGER.error("read {} [{}, {}) from block cache fail", streamId, startOffset, endOffset, ex);
                // TODO: release read records memory
                return;
            }

            if (ret.getCacheAccessType() == CacheAccessType.BLOCK_CACHE_HIT) {
                OperationMetricsStats.getCounter(S3Operation.READ_STORAGE_BLOCK_CACHE).inc();
            } else {
                OperationMetricsStats.getCounter(S3Operation.READ_STORAGE_BLOCK_CACHE_MISS).inc();
            }
            OperationMetricsStats.getHistogram(S3Operation.READ_STORAGE_BLOCK_CACHE).update(timerUtil.elapsedAndReset());
        });
        return readCf;
    }

    public CompletableFuture<ReadDataBlock> read0(long streamId, long startOffset, long endOffset, int maxBytes, boolean awaitReadAhead) {
        if (startOffset >= endOffset || maxBytes <= 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(Collections.emptyList(), CacheAccessType.BLOCK_CACHE_MISS));
        }

        if (awaitReadAhead) {
            // expect read ahead will fill the cache with the data we need.
            CompletableFuture<?> readAheadCf = readAheadTasks.get(new ReadingTaskKey(streamId, startOffset));
            if (readAheadCf != null) {
                CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
                readAheadCf.whenComplete((nil, ex) -> FutureUtil.propagate(read0(streamId, startOffset, endOffset, maxBytes, false), readCf));
                return readCf;
            }
        }

        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;

        // 1. get from cache
        BlockCache.GetCacheResult cacheRst = cache.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
        List<StreamRecordBatch> cacheRecords = cacheRst.getRecords();
        if (!cacheRecords.isEmpty()) {
            nextStartOffset = cacheRecords.get(cacheRecords.size() - 1).getLastOffset();
            nextMaxBytes -= Math.min(nextMaxBytes, cacheRecords.stream().mapToInt(StreamRecordBatch::size).sum());
        }
        cacheRst.getReadAhead().ifPresent(readAhead -> backgroundReadAhead(streamId, readAhead));
        if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(cacheRecords, CacheAccessType.BLOCK_CACHE_HIT));
        }

        // 2. get from s3
        ReadContext context = new ReadContext(Collections.emptyList(), nextStartOffset, nextMaxBytes);
        CompletableFuture<List<S3ObjectMetadata>> getObjectsCf = objectManager.getObjects(streamId, nextStartOffset, endOffset, 2);
        return getObjectsCf.thenComposeAsync(objects -> {
            context.objects = objects;
            return readFromS3(streamId, endOffset, context)
                    .thenApply(s3Rst -> {
                        List<StreamRecordBatch> records = new ArrayList<>(cacheRst.getRecords());
                        records.addAll(s3Rst.getRecords());
                        return new ReadDataBlock(records, CacheAccessType.BLOCK_CACHE_MISS);
                    });
        }, mainExecutor);
    }

    @Override
    public void put(Map<Long, List<StreamRecordBatch>> stream2records) {
        stream2records.forEach(cache::put);
    }

    private CompletableFuture<ReadDataBlock> readFromS3(long streamId, long endOffset, ReadContext context) {
        CompletableFuture<Boolean /* empty objects */> getObjectsCf = CompletableFuture.completedFuture(false);
        if (context.objectIndex >= context.objects.size()) {
            getObjectsCf = objectManager
                    .getObjects(streamId, context.nextStartOffset, endOffset, 2)
                    .orTimeout(1, TimeUnit.MINUTES)
                    .thenApply(objects -> {
                        context.objects = objects;
                        context.objectIndex = 0;
                        if (context.objects.isEmpty()) {
                            if (endOffset == -1L) { // background read ahead
                                return true;
                            } else {
                                LOGGER.error("[BUG] fail to read, expect objects not empty, streamId={}, startOffset={}, endOffset={}",
                                        streamId, context.nextStartOffset, endOffset);
                                throw new IllegalStateException("fail to read, expect objects not empty");
                            }
                        }
                        return false;
                    });
        }
        CompletableFuture<List<ObjectReader.DataBlockIndex>> findIndexCf = getObjectsCf.thenCompose(emptyObjects -> {
            if (emptyObjects) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            ObjectReader reader = getObjectReader(context.objects.get(context.objectIndex));
            context.objectIndex++;
            context.reader = reader;
            return reader.find(streamId, context.nextStartOffset, endOffset, context.nextMaxBytes);
        });
        CompletableFuture<List<CompletableFuture<DataBlockRecords>>> getDataCf = findIndexCf.thenCompose(blockIndexes -> {
            if (blockIndexes.isEmpty()) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            List<CompletableFuture<DataBlockRecords>> blockCfList = blockIndexes.stream()
                    .map(i -> dataBlockReadAccumulator.readDataBlock(context.reader, i))
                    .collect(Collectors.toList());
            CompletableFuture<Void> allBlockCf = CompletableFuture.allOf(blockCfList.toArray(new CompletableFuture[0]));
            return allBlockCf.thenApply(nil -> blockCfList);
        });
        getDataCf.whenComplete((rst, ex) -> {
            if (context.reader != null) {
                context.reader.release();
                context.reader = null;
            }
        });
        return getDataCf.thenComposeAsync(blockCfList -> {
            if (blockCfList.isEmpty()) {
                return CompletableFuture.completedFuture(new ReadDataBlock(context.records, CacheAccessType.BLOCK_CACHE_MISS));
            }
            try {
                long nextStartOffset = context.nextStartOffset;
                int nextMaxBytes = context.nextMaxBytes;
                boolean fulfill = false;
                for (CompletableFuture<DataBlockRecords> blockCf : blockCfList) {
                    DataBlockRecords dataBlock = blockCf.get();
                    // TODO: add #getRecords to DataBlockRecords, use binary search to get the records we need.
                    if (!fulfill) {
                        for (StreamRecordBatch recordBatch : dataBlock.records()) {
                            if (fulfill) {
                                break;
                            }
                            if (recordBatch.getLastOffset() <= nextStartOffset) {
                                continue;
                            }
                            recordBatch.retain();
                            context.records.add(recordBatch);
                            nextStartOffset = recordBatch.getLastOffset();
                            nextMaxBytes -= Math.min(nextMaxBytes, recordBatch.size());
                            if ((endOffset != NOOP_OFFSET && nextStartOffset >= endOffset) || nextMaxBytes == 0) {
                                fulfill = true;
                            }
                        }
                    }
                    dataBlock.release();
                }
                context.nextStartOffset = nextStartOffset;
                context.nextMaxBytes = nextMaxBytes;
                if (fulfill) {
                    return CompletableFuture.completedFuture(new ReadDataBlock(context.records, CacheAccessType.BLOCK_CACHE_MISS));
                } else {
                    return readFromS3(streamId, endOffset, context);
                }
            } catch (Throwable e) {
                return FutureUtil.failedFuture(e);
            }
        }, mainExecutor);
    }

    private void backgroundReadAhead(long streamId, BlockCache.ReadAhead readahead) {
        mainExecutor.execute(() -> {
            CompletableFuture<List<S3ObjectMetadata>> getObjectsCf = objectManager.getObjects(streamId, readahead.startOffset(), NOOP_OFFSET, 2);
            getObjectsCf.thenAccept(objects -> {
                if (objects.isEmpty()) {
                    return;
                }
                if (!readAheadLimiter.tryAcquire()) {
                    // if inflight read ahead tasks exceed limit, skip this read ahead.
                    return;
                }

                CompletableFuture<ReadDataBlock> readAheadCf = readFromS3(streamId, NOOP_OFFSET,
                        new ReadContext(objects, readahead.startOffset(), readahead.size()));
                ReadingTaskKey readingTaskKey = new ReadingTaskKey(streamId, readahead.startOffset());
                readAheadTasks.put(readingTaskKey, readAheadCf);
                readAheadCf
                        .whenComplete((rst, ex) -> {
                            readAheadLimiter.release();
                            if (ex != null) {
                                LOGGER.error("background read ahead {} fail", readahead, ex);
                            } else {
                                rst.getRecords().forEach(StreamRecordBatch::release);
                            }
                            readAheadTasks.remove(readingTaskKey, readAheadCf);
                        });
            });
        });
    }

    private ObjectReader getObjectReader(S3ObjectMetadata metadata) {
        synchronized (objectReaderLRU) {
            // TODO: evict by object readers index cache size
            while (objectReaderLRU.size() > 128) {
                Optional.ofNullable(objectReaderLRU.pop()).ifPresent(entry -> entry.getValue().close());
            }
            ObjectReader objectReader = objectReaderLRU.get(metadata.objectId());
            if (objectReader == null) {
                objectReader = new ObjectReader(metadata, s3Operator);
                objectReaderLRU.put(metadata.objectId(), objectReader);
            }
            return objectReader.retain();
        }
    }

    static class ReadContext {
        List<S3ObjectMetadata> objects;
        int objectIndex;
        ObjectReader reader;
        List<StreamRecordBatch> records;
        long nextStartOffset;
        int nextMaxBytes;

        public ReadContext(List<S3ObjectMetadata> objects, long startOffset, int maxBytes) {
            this.objects = objects;
            this.records = new LinkedList<>();
            this.nextStartOffset = startOffset;
            this.nextMaxBytes = maxBytes;
        }

    }

    record ReadingTaskKey(long streamId, long startOffset) {

    }

}
