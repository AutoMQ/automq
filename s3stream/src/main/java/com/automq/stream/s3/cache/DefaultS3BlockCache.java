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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OFFSET;

public class DefaultS3BlockCache implements S3BlockCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3BlockCache.class);
    private final Map<ReadAheadTaskKey, ReadAheadTaskContext> inflightReadAheadTasks = new ConcurrentHashMap<>();
    private final Map<ReadTaskKey, ReadTaskContext> inflightReadStatusMap = new ConcurrentHashMap<>();
    private final BlockCache cache;
    private final ExecutorService mainExecutor;
    private final ExecutorService callBackExecutor;
    private final ReadAheadManager readAheadManager;
    private final StreamReader streamReader;
    private final InflightReadThrottle inflightReadThrottle;

    public DefaultS3BlockCache(Config config, ObjectManager objectManager, S3Operator s3Operator) {
        int blockSize = config.objectBlockSize();
        CacheSizeSet cacheSizeSet = getCacheSize(config.blockCacheSize());

        this.cache = new BlockCache(cacheSizeSet.blockCacheSize);
        this.readAheadManager = new ReadAheadManager(blockSize, this.cache);
        this.mainExecutor = Threads.newFixedThreadPoolWithMonitor(
            2,
            "s3-block-cache-main",
            false,
            LOGGER);
        this.callBackExecutor = Threads.newFixedThreadPoolWithMonitor(
                2,
                "s3-block-cache-callback",
                false,
                LOGGER);
        this.inflightReadThrottle = new InflightReadThrottle(cacheSizeSet.inflightReadSize);
        this.streamReader = new StreamReader(s3Operator, objectManager, cache, inflightReadAheadTasks, inflightReadThrottle);
        LOGGER.info("Init s3 block cache, block cache size: {}, inflight read size: {}", cacheSizeSet.blockCacheSize, cacheSizeSet.inflightReadSize);
    }

    private CacheSizeSet getCacheSize(long blockCacheSize) {
        CacheSizeSet cacheSizeSet = new CacheSizeSet();
        cacheSizeSet.blockCacheSize = blockCacheSize;
        cacheSizeSet.inflightReadSize = (int) (0.1 * blockCacheSize);
        if (cacheSizeSet.inflightReadSize < 100 * 1024 * 1024) {
            cacheSizeSet.inflightReadSize = 100 * 1024 * 1024;
        } else {
            cacheSizeSet.blockCacheSize = (long) (0.9 * blockCacheSize);
        }
        return cacheSizeSet;
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
        long startTime = System.nanoTime();
        CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
        ReadAheadAgent agent = this.readAheadManager.getOrCreateReadAheadAgent(streamId, startOffset);
        UUID uuid = UUID.randomUUID();
        ReadTaskKey key = new ReadTaskKey(streamId, startOffset, endOffset, maxBytes, uuid);
        ReadTaskContext context = new ReadTaskContext(agent, ReadBlockCacheStatus.INIT);
        this.inflightReadStatusMap.put(key, context);
        // submit read task to mainExecutor to avoid read slower the caller thread.
        mainExecutor.execute(() -> {
            try {
                FutureUtil.propagateAsync(read0(finalTraceContext, streamId, startOffset, endOffset, maxBytes, uuid, context).whenComplete((ret, ex) -> {
                    if (ex != null) {
                        LOGGER.error("read {} [{}, {}), maxBytes: {} from block cache fail", streamId, startOffset, endOffset, maxBytes, ex);
                        this.inflightReadThrottle.release(uuid);
                        this.inflightReadStatusMap.remove(key);
                        return;
                    }
                    int totalReturnedSize = ret.getRecords().stream().mapToInt(StreamRecordBatch::size).sum();
                    this.readAheadManager.updateReadResult(streamId, startOffset,
                        ret.getRecords().get(ret.getRecords().size() - 1).getLastOffset(), totalReturnedSize);

                    long timeElapsed = TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS);
                    boolean isCacheHit = ret.getCacheAccessType() == CacheAccessType.BLOCK_CACHE_HIT;
                    StorageOperationStats.getInstance().readBlockCacheStats(isCacheHit).record(timeElapsed);
                    recordStageTime(isCacheHit, context);
                    Span.fromContext(finalTraceContext.currentContext()).setAttribute("cache_hit", isCacheHit);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] read data complete, cache hit: {}, stream={}, {}-{}, total bytes: {}",
                            ret.getCacheAccessType() == CacheAccessType.BLOCK_CACHE_HIT, streamId, startOffset, endOffset, totalReturnedSize);
                    }
                    this.inflightReadThrottle.release(uuid);
                    this.inflightReadStatusMap.remove(key);
                }), readCf, callBackExecutor);
            } catch (Exception e) {
                LOGGER.error("read {} [{}, {}), maxBytes: {} from block cache fail", streamId, startOffset, endOffset, maxBytes, e);
                this.inflightReadThrottle.release(uuid);
                this.inflightReadStatusMap.remove(key);
                readCf.completeExceptionally(e);
            }
        });
        return readCf;
    }

    private void recordStageTime(boolean isCacheHit, ReadTaskContext context) {
        if (isCacheHit) {
            StorageOperationStats.getInstance().readBlockCacheStageHitWaitInflightTimeStats.record(context.waitInflightTime);
            StorageOperationStats.getInstance().readBlockCacheStageHitReadCacheTimeStats.record(context.readBlockCacheTime);
            StorageOperationStats.getInstance().readBlockCacheStageHitReadAheadTimeStats.record(context.readAheadTime);
            StorageOperationStats.getInstance().readBlockCacheStageHitReadS3TimeStats.record(context.readS3Time);
        } else {
            StorageOperationStats.getInstance().readBlockCacheStageMissWaitInflightTimeStats.record(context.waitInflightTime);
            StorageOperationStats.getInstance().readBlockCacheStageMissReadCacheTimeStats.record(context.readBlockCacheTime);
            StorageOperationStats.getInstance().readBlockCacheStageMissReadAheadTimeStats.record(context.readAheadTime);
            StorageOperationStats.getInstance().readBlockCacheStageMissReadS3TimeStats.record(context.readS3Time);
        }
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

        TimerUtil inflightTimer = new TimerUtil();
        ReadAheadTaskContext inflightReadAheadTaskContext = inflightReadAheadTasks.get(new ReadAheadTaskKey(streamId, nextStartOffset));
        if (inflightReadAheadTaskContext != null) {
            CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
            context.setStatus(ReadBlockCacheStatus.WAIT_INFLIGHT_RA);
            inflightReadAheadTaskContext.cf.whenCompleteAsync((nil, ex) -> {
                context.waitInflightTime += inflightTimer.elapsedAs(TimeUnit.NANOSECONDS);
                FutureUtil.exec(() -> FutureUtil.propagate(
                        read0(traceContext, streamId, startOffset, endOffset, maxBytes, uuid, context), readCf), readCf, LOGGER, "read0");

            }, mainExecutor);
            return readCf;
        }

        // 1. get from cache
        context.setStatus(ReadBlockCacheStatus.GET_FROM_CACHE);
        TimerUtil timer = new TimerUtil();
        BlockCache.GetCacheResult cacheRst = cache.get(traceContext, streamId, nextStartOffset, endOffset, nextMaxBytes);
        context.readBlockCacheTime += timer.elapsedAndResetAs(TimeUnit.NANOSECONDS);
        List<StreamRecordBatch> cacheRecords = cacheRst.getRecords();
        if (!cacheRecords.isEmpty()) {
            asyncReadAhead(streamId, agent, cacheRst.getReadAheadRecords());
            nextStartOffset = cacheRecords.get(cacheRecords.size() - 1).getLastOffset();
            nextMaxBytes -= Math.min(nextMaxBytes, cacheRecords.stream().mapToInt(StreamRecordBatch::size).sum());
            context.readAheadTime += timer.elapsedAs(TimeUnit.NANOSECONDS);
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
        TimerUtil s3Timer = new TimerUtil();
        return streamReader.syncReadAhead(traceContext, streamId, startOffset, endOffset, maxBytes, agent, uuid)
            .thenCompose(rst -> {
                context.readS3Time += s3Timer.elapsedAs(TimeUnit.NANOSECONDS);
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

    public static final class ReadAheadTaskKey {
        private final long streamId;
        private final long startOffset;

        public ReadAheadTaskKey(long streamId, long startOffset) {
            this.streamId = streamId;
            this.startOffset = startOffset;
        }

        public long streamId() {
            return streamId;
        }

        public long startOffset() {
            return startOffset;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (ReadAheadTaskKey) obj;
            return this.streamId == that.streamId &&
                this.startOffset == that.startOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, startOffset);
        }

        @Override
        public String toString() {
            return "ReadAheadTaskKey[" +
                "streamId=" + streamId + ", " +
                "startOffset=" + startOffset + ']';
        }

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

    public static final class ReadTaskKey {
        private final long streamId;
        private final long startOffset;
        private final long endOffset;
        private final int maxBytes;
        private final UUID uuid;

        public ReadTaskKey(long streamId, long startOffset, long endOffset, int maxBytes, UUID uuid) {
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.maxBytes = maxBytes;
            this.uuid = uuid;
        }

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

        public long streamId() {
            return streamId;
        }

        public long startOffset() {
            return startOffset;
        }

        public long endOffset() {
            return endOffset;
        }

        public int maxBytes() {
            return maxBytes;
        }

        public UUID uuid() {
            return uuid;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (ReadTaskKey) obj;
            return this.streamId == that.streamId &&
                this.startOffset == that.startOffset &&
                this.endOffset == that.endOffset &&
                this.maxBytes == that.maxBytes &&
                Objects.equals(this.uuid, that.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, startOffset, endOffset, maxBytes, uuid);
        }

    }

    public static class ReadTaskContext {
        final ReadAheadAgent agent;
        ReadBlockCacheStatus status;
        long waitInflightTime;
        long readBlockCacheTime;
        long readAheadTime;
        long readS3Time;

        public ReadTaskContext(ReadAheadAgent agent, ReadBlockCacheStatus status) {
            this.agent = agent;
            this.status = status;
        }

        void setStatus(ReadBlockCacheStatus status) {
            this.status = status;
        }
    }

    public static final class ReadAheadRecord {
        private final long nextRAOffset;

        public ReadAheadRecord(long nextRAOffset) {
            this.nextRAOffset = nextRAOffset;
        }

        public long nextRAOffset() {
            return nextRAOffset;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (ReadAheadRecord) obj;
            return this.nextRAOffset == that.nextRAOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nextRAOffset);
        }

        @Override
        public String toString() {
            return "ReadAheadRecord[" +
                "nextRAOffset=" + nextRAOffset + ']';
        }

    }

    private static class CacheSizeSet {
        long blockCacheSize;
        int inflightReadSize;
    }

}
