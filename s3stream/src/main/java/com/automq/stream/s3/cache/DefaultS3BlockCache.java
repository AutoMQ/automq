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
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
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
    private final Map<ReadAheadTaskKey, ReadAheadTaskContext> inflightReadAheadTasks = new ConcurrentHashMap<>();
    private final Map<ReadTaskKey, ReadTaskContext> inflightReadStatusMap = new ConcurrentHashMap<>();
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
    @WithSpan
    public CompletableFuture<ReadDataBlock> read(TraceContext traceContext,
                                                 @SpanAttribute long streamId,
                                                 @SpanAttribute long startOffset,
                                                 @SpanAttribute long endOffset,
                                                 @SpanAttribute int maxBytes) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] read data, stream={}, {}-{}, total bytes: {}", streamId, startOffset, endOffset, maxBytes);
        }
        final TraceContext finalTraceContext = new TraceContext(traceContext);
        this.readAheadManager.updateReadProgress(streamId, startOffset);
        TimerUtil timerUtil = new TimerUtil();
        CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
        ReadAheadAgent agent = this.readAheadManager.getOrCreateReadAheadAgent(streamId, startOffset);
        UUID uuid = UUID.randomUUID();
        ReadTaskKey key = new ReadTaskKey(streamId, startOffset, endOffset, maxBytes , uuid);
        ReadTaskContext context = new ReadTaskContext(agent, ReadBlockCacheStatus.INIT);
        this.inflightReadStatusMap.put(key, context);
        // submit read task to mainExecutor to avoid read slower the caller thread.
        mainExecutor.execute(() -> {
            try {
                FutureUtil.propagate(read0(finalTraceContext, streamId, startOffset, endOffset, maxBytes, uuid, context).whenComplete((ret, ex) -> {
                    if (ex != null) {
                        LOGGER.error("read {} [{}, {}), maxBytes: {} from block cache fail", streamId, startOffset, endOffset, maxBytes, ex);
                        this.inflightReadThrottle.release(uuid);
                        this.inflightReadStatusMap.remove(key);
                        return;
                    }
                    int totalReturnedSize = ret.getRecords().stream().mapToInt(StreamRecordBatch::size).sum();
                    this.readAheadManager.updateReadResult(streamId, startOffset,
                            ret.getRecords().get(ret.getRecords().size() - 1).getLastOffset(), totalReturnedSize);

                    long timeElapsed = timerUtil.elapsedAs(TimeUnit.NANOSECONDS);
                    boolean isCacheHit = ret.getCacheAccessType() == CacheAccessType.BLOCK_CACHE_HIT;
                    Span.fromContext(finalTraceContext.currentContext()).setAttribute("cache_hit", isCacheHit);
                    S3StreamMetricsManager.recordReadCacheLatency(MetricsLevel.INFO, timeElapsed, S3Operation.READ_STORAGE_BLOCK_CACHE, isCacheHit);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] read data complete, cache hit: {}, stream={}, {}-{}, total bytes: {}",
                                ret.getCacheAccessType() == CacheAccessType.BLOCK_CACHE_HIT, streamId, startOffset, endOffset, totalReturnedSize);
                    }
                    this.inflightReadThrottle.release(uuid);
                    this.inflightReadStatusMap.remove(key);
                }), readCf);
            } catch (Exception e) {
                LOGGER.error("read {} [{}, {}), maxBytes: {} from block cache fail, {}", streamId, startOffset, endOffset, maxBytes, e);
                this.inflightReadThrottle.release(uuid);
                this.inflightReadStatusMap.remove(key);
                readCf.completeExceptionally(e);
            }
        });
        return readCf;
    }

    @WithSpan
    public CompletableFuture<ReadDataBlock> read0(TraceContext traceContext,
                                                  @SpanAttribute long streamId,
                                                  @SpanAttribute long startOffset,
                                                  @SpanAttribute long endOffset,
                                                  @SpanAttribute int maxBytes,
                                                  UUID uuid, ReadTaskContext context) {
        ReadAheadAgent agent = context.agent;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] read0, stream={}, {}-{}, total bytes: {}, uuid: {} ", streamId, startOffset, endOffset, maxBytes, uuid);
        }

        if (startOffset >= endOffset || maxBytes <= 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(Collections.emptyList(), CacheAccessType.BLOCK_CACHE_MISS));
        }

        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;

        ReadAheadTaskContext inflightReadAheadTaskContext = inflightReadAheadTasks.get(new ReadAheadTaskKey(streamId, nextStartOffset));
        if (inflightReadAheadTaskContext != null) {
            CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
            context.setStatus(ReadBlockCacheStatus.WAIT_INFLIGHT_RA);
            inflightReadAheadTaskContext.cf.whenComplete((nil, ex) -> FutureUtil.exec(() -> FutureUtil.propagate(
                    read0(traceContext, streamId, startOffset, endOffset, maxBytes, uuid, context), readCf), readCf, LOGGER, "read0"));
            return readCf;
        }

        // 1. get from cache
        context.setStatus(ReadBlockCacheStatus.GET_FROM_CACHE);
        BlockCache.GetCacheResult cacheRst = cache.get(traceContext, streamId, nextStartOffset, endOffset, nextMaxBytes);
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
                return read0(traceContext, streamId, nextStartOffset, endOffset, nextMaxBytes, uuid, context).thenApply(rst -> {
                    List<StreamRecordBatch> records = new ArrayList<>(cacheRecords);
                    records.addAll(rst.getRecords());
                    return new ReadDataBlock(records, CacheAccessType.BLOCK_CACHE_MISS);
                });
            }
        }

        // 2. get from s3
        context.setStatus(ReadBlockCacheStatus.GET_FROM_S3);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] read data cache miss, stream={}, {}-{}, total bytes: {} ", streamId, startOffset, endOffset, maxBytes);
        }
        return streamReader.syncReadAhead(traceContext, streamId, startOffset, endOffset, maxBytes, agent, uuid)
                .thenCompose(rst -> {
                    if (!rst.isEmpty()) {
                        int remainBytes = maxBytes - rst.stream().mapToInt(StreamRecordBatch::size).sum();
                        long lastOffset = rst.get(rst.size() - 1).getLastOffset();
                        if (remainBytes > 0 && lastOffset < endOffset) {
                            // retry read
                            return read0(traceContext, streamId, lastOffset, endOffset, remainBytes, uuid, context).thenApply(rst2 -> {
                                List<StreamRecordBatch> records = new ArrayList<>(rst);
                                records.addAll(rst2.getRecords());
                                return new ReadDataBlock(records, CacheAccessType.BLOCK_CACHE_MISS);
                            });
                        }
                    }
                    return CompletableFuture.completedFuture(new ReadDataBlock(rst, CacheAccessType.BLOCK_CACHE_MISS));
                });
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

    public static class ReadAheadTaskContext {
        final CompletableFuture<Void> cf;
        ReadBlockCacheStatus status;

        public ReadAheadTaskContext(CompletableFuture<Void> cf, ReadBlockCacheStatus status) {
            this.cf = cf;
            this.status = status;
        }

        void setStatus(ReadBlockCacheStatus status) {
            this.status = status;
        }
    }

    public record ReadTaskKey(long streamId, long startOffset, long endOffset, int maxBytes, UUID uuid) {
        @Override
        public String toString() {
            return "ReadTaskKey{" +
                    "streamId=" + streamId +
                    ", startOffset=" + startOffset +
                    ", endOffset=" + endOffset +
                    ", maxBytes=" + maxBytes +
                    ", uuid=" + uuid +
                    '}';
        }
    }

    public static class ReadTaskContext {
        final ReadAheadAgent agent;
        ReadBlockCacheStatus status;

        public ReadTaskContext(ReadAheadAgent agent, ReadBlockCacheStatus status) {
            this.agent = agent;
            this.status = status;
        }

        void setStatus(ReadBlockCacheStatus status) {
            this.status = status;
        }
    }

    public record ReadAheadRecord(long nextRAOffset) {
    }

    public enum ReadBlockCacheStatus {
        /* Status for read request */
        INIT,
        WAIT_INFLIGHT_RA,
        GET_FROM_CACHE,
        GET_FROM_S3,

        /* Status for read ahead request */
        WAIT_DATA_INDEX,
        WAIT_FETCH_DATA,
        WAIT_THROTTLE,
    }

}
