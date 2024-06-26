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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.cache.DefaultS3BlockCache.ReadAheadRecord;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.biniarysearch.StreamRecordBatchList;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.model.StreamRecordBatch.OBJECT_OVERHEAD;

public class BlockCache implements ByteBufAlloc.OOMHandler {
    public static final Integer ASYNC_READ_AHEAD_NOOP_OFFSET = -1;
    static final int BLOCK_SIZE = 1024 * 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockCache.class);
    final Map<Long, StreamCache> stream2cache = new HashMap<>();
    private final long maxSize;
    private final LRUCache<CacheBlockKey, Integer> inactive = new LRUCache<>();
    private final LRUCache<CacheBlockKey, Integer> active = new LRUCache<>();
    private final AtomicLong size = new AtomicLong();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final List<CacheEvictListener> cacheEvictListeners = new ArrayList<>();

    public BlockCache(long maxSize) {
        this.maxSize = maxSize;
        S3StreamMetricsManager.registerBlockCacheSizeSupplier(size::get);
    }

    public void registerListener(CacheEvictListener listener) {
        cacheEvictListeners.add(listener);
    }

    public void put(long streamId, List<StreamRecordBatch> records) {
        put(streamId, ASYNC_READ_AHEAD_NOOP_OFFSET, ASYNC_READ_AHEAD_NOOP_OFFSET, records);
    }

    public void put(long streamId, long raAsyncOffset, long raEndOffset, List<StreamRecordBatch> records) {
        writeLock.lock();
        try {
            put0(streamId, raAsyncOffset, raEndOffset, records);
        } finally {
            writeLock.unlock();
        }
    }

    void put0(long streamId, long raAsyncOffset, long raEndOffset, List<StreamRecordBatch> records) {
        if (maxSize == 0 || records.isEmpty()) {
            records.forEach(StreamRecordBatch::release);
            return;
        }
        records = new ArrayList<>(records);
        StreamCache streamCache = stream2cache.computeIfAbsent(streamId, id -> new StreamCache());
        long startOffset = records.get(0).getBaseOffset();
        long endOffset = records.get(records.size() - 1).getLastOffset();

        if (raAsyncOffset != ASYNC_READ_AHEAD_NOOP_OFFSET && (raAsyncOffset < startOffset || raAsyncOffset >= endOffset)) {
            LOGGER.warn("raAsyncOffset out of range, stream={}, raAsyncOffset: {}, startOffset: {}, endOffset: {}", streamId, raAsyncOffset, startOffset, endOffset);
        }

        int size = records.stream().mapToInt(StreamRecordBatch::size).sum();
        size += records.size() * OBJECT_OVERHEAD;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] put block cache, stream={}, {}-{}, raAsyncOffset: {}, raEndOffset: {}, total bytes: {} ", streamId, startOffset, endOffset, raAsyncOffset, raEndOffset, size);
        }

        // remove overlapped part.
        SortedMap<Long, CacheBlock> tailMap = streamCache.tailBlocks(startOffset);
        for (Map.Entry<Long, CacheBlock> entry : tailMap.entrySet()) {
            CacheBlock cacheBlock = entry.getValue();
            if (cacheBlock.firstOffset >= endOffset) {
                break;
            }
            if (isWithinRange(raAsyncOffset, cacheBlock.firstOffset, cacheBlock.lastOffset) && cacheBlock.readAheadRecord == null) {
                cacheBlock.readAheadRecord = new ReadAheadRecord(raEndOffset);
            }
            // overlap is a rare case, so removeIf is fine for the performance.
            records.removeIf(record -> {
                boolean remove = record.getLastOffset() > cacheBlock.firstOffset && record.getBaseOffset() < cacheBlock.lastOffset;
                if (remove) {
                    record.release();
                }
                return remove;
            });
        }

        // ensure the cache size.
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] block cache size: {}/{}, ensure size: {} ", this.size.get(), maxSize, size);
        }
        ensureCapacity(size);

        // split to 1MB cache blocks which one block contains sequential records.
        long expectStartOffset = -1L;
        LinkedList<StreamRecordBatch> batchList = new LinkedList<>();
        int partSize = 0;
        for (StreamRecordBatch record : records) {
            if ((expectStartOffset == -1L || record.getBaseOffset() == expectStartOffset) && partSize < BLOCK_SIZE) {
                batchList.add(record);
                partSize += record.size();
            } else {
                ReadAheadRecord raRecord = isWithinRange(raAsyncOffset, batchList.getFirst().getBaseOffset(), batchList.getLast().getLastOffset()) ?
                    new ReadAheadRecord(raEndOffset) : null;
                put(streamId, streamCache, new CacheBlock(batchList, raRecord));
                batchList = new LinkedList<>();
                batchList.add(record);
                partSize = record.size();
            }
            expectStartOffset = record.getLastOffset();
        }
        if (!batchList.isEmpty()) {
            ReadAheadRecord raRecord = isWithinRange(raAsyncOffset, batchList.getFirst().getBaseOffset(), batchList.getLast().getLastOffset()) ?
                new ReadAheadRecord(raEndOffset) : null;
            put(streamId, streamCache, new CacheBlock(batchList, raRecord));
        }
    }

    public void setReadAheadRecord(long streamId, long raAsyncOffset, long raEndOffset) {
        writeLock.lock();
        try {
            StreamCache streamCache = stream2cache.get(streamId);
            if (streamCache == null) {
                return;
            }
            NavigableMap<Long, CacheBlock> streamCacheBlocks = streamCache.tailBlocks(raAsyncOffset);
            for (Map.Entry<Long, CacheBlock> entry : streamCacheBlocks.entrySet()) {
                CacheBlock cacheBlock = entry.getValue();
                if (isWithinRange(raAsyncOffset, cacheBlock.firstOffset, cacheBlock.lastOffset)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] set read ahead record, stream={}, raAsyncOffset: {}, raEndOffset: {}", streamId, raAsyncOffset, raEndOffset);
                    }
                    cacheBlock.readAheadRecord = new ReadAheadRecord(raEndOffset);
                    break;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private boolean isWithinRange(long raAsyncOffset, long startOffset, long endOffset) {
        return raAsyncOffset >= startOffset && raAsyncOffset < endOffset;
    }

    public boolean checkRange(long streamId, long startOffset, int maxBytes) {
        if (maxBytes <= 0) {
            return true;
        }
        readLock.lock();
        try {
            return checkRange0(streamId, startOffset, maxBytes);
        } finally {
            readLock.unlock();
        }
    }

    boolean checkRange0(long streamId, long startOffset, int maxBytes) {
        StreamCache streamCache = stream2cache.get(streamId);
        if (streamCache == null) {
            return false;
        }

        NavigableMap<Long, CacheBlock> streamCacheBlocks = streamCache.tailBlocks(startOffset);
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        LinkedList<StreamRecordBatch> records = new LinkedList<>();
        for (Map.Entry<Long, CacheBlock> entry : streamCacheBlocks.entrySet()) {
            CacheBlock cacheBlock = entry.getValue();
            if (cacheBlock.lastOffset <= nextStartOffset || nextStartOffset < cacheBlock.firstOffset) {
                break;
            }
            nextMaxBytes = readFromCacheBlock(records, cacheBlock, nextStartOffset, Long.MAX_VALUE, nextMaxBytes);
            nextStartOffset = records.getLast().getLastOffset();
            if (nextMaxBytes <= 0) {
                return true;
            }
        }
        return nextMaxBytes <= 0;
    }

    public GetCacheResult get(long streamId, long startOffset, long endOffset, int maxBytes) {
        return get(TraceContext.DEFAULT, streamId, startOffset, endOffset, maxBytes);
    }

    /**
     * Get records from cache.
     * Note: the records is retained, the caller should release it.
     */
    @WithSpan
    public GetCacheResult get(TraceContext context,
        @SpanAttribute long streamId,
        @SpanAttribute long startOffset,
        @SpanAttribute long endOffset,
        @SpanAttribute int maxBytes) {
        context.currentContext();
        if (startOffset >= endOffset || maxBytes <= 0) {
            return GetCacheResult.empty();
        }

        readLock.lock();
        try {
            return get0(streamId, startOffset, endOffset, maxBytes);
        } finally {
            readLock.unlock();
        }
    }

    public GetCacheResult get0(long streamId, long startOffset, long endOffset, int maxBytes) {
        StreamCache streamCache = stream2cache.get(streamId);
        if (streamCache == null) {
            return GetCacheResult.empty();
        }
        NavigableMap<Long, CacheBlock> streamCacheBlocks = streamCache.tailBlocks(startOffset);
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        List<ReadAheadRecord> readAheadRecords = new ArrayList<>();
        LinkedList<StreamRecordBatch> records = new LinkedList<>();
        for (Map.Entry<Long, CacheBlock> entry : streamCacheBlocks.entrySet()) {
            CacheBlock cacheBlock = entry.getValue();
            if (cacheBlock.lastOffset <= nextStartOffset || nextStartOffset < cacheBlock.firstOffset) {
                break;
            }
            if (cacheBlock.readAheadRecord != null) {
                readAheadRecords.add(cacheBlock.readAheadRecord);
                cacheBlock.readAheadRecord = null;
            }
            nextMaxBytes = readFromCacheBlock(records, cacheBlock, nextStartOffset, endOffset, nextMaxBytes);
            nextStartOffset = records.getLast().getLastOffset();
            boolean blockCompletedRead = nextStartOffset >= cacheBlock.lastOffset;
            CacheBlockKey cacheBlockKey = new CacheBlockKey(streamId, cacheBlock.firstOffset);
            if (blockCompletedRead) {
                active.remove(cacheBlockKey);
                inactive.put(cacheBlockKey, cacheBlock.size);
            } else {
                if (!active.touchIfExist(cacheBlockKey)) {
                    inactive.touchIfExist(cacheBlockKey);
                }
            }

            if (nextStartOffset >= endOffset || nextMaxBytes <= 0) {
                break;
            }

        }

        records.forEach(StreamRecordBatch::retain);
        return GetCacheResult.of(records, readAheadRecords);
    }

    private int readFromCacheBlock(LinkedList<StreamRecordBatch> records, CacheBlock cacheBlock,
        long nextStartOffset, long endOffset, int nextMaxBytes) {
        boolean matched = false;
        StreamRecordBatchList streamRecordBatchList = new StreamRecordBatchList(cacheBlock.records);
        int startIndex = streamRecordBatchList.search(nextStartOffset);
        if (startIndex < 0) {
            // mismatched
            return nextMaxBytes;
        }
        for (int i = startIndex; i < cacheBlock.records.size(); i++) {
            StreamRecordBatch record = cacheBlock.records.get(i);
            if (record.getBaseOffset() <= nextStartOffset && record.getLastOffset() > nextStartOffset) {
                records.add(record);
                nextStartOffset = record.getLastOffset();
                nextMaxBytes -= record.size();
                matched = true;
                if (nextStartOffset >= endOffset || nextMaxBytes <= 0) {
                    break;
                }
            } else if (matched) {
                break;
            }
        }
        return nextMaxBytes;
    }

    private void ensureCapacity(int size) {
        ensureCapacity0(size, false);
    }

    private int ensureCapacity0(int size, boolean forceEvict) {
        if (!forceEvict && (maxSize - this.size.get() >= size)) {
            return 0;
        }
        int evictBytes = 0;
        for (LRUCache<CacheBlockKey, Integer> lru : List.of(inactive, active)) {
            for (; ; ) {
                Map.Entry<CacheBlockKey, Integer> entry = lru.pop();
                if (entry == null) {
                    break;
                }
                StreamCache streamCache = stream2cache.get(entry.getKey().streamId);
                if (streamCache == null) {
                    LOGGER.error("[BUG] Stream cache not found for streamId: {}", entry.getKey().streamId);
                    continue;
                }
                CacheBlock cacheBlock = streamCache.remove(entry.getKey().startOffset);
                if (cacheBlock == null) {
                    LOGGER.error("[BUG] Cannot find stream cache block: {} {}", entry.getKey().streamId, entry.getKey().startOffset);
                } else {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[S3BlockCache] evict block, stream={}, {}-{}, total bytes: {} ", entry.getKey().streamId, cacheBlock.firstOffset, cacheBlock.lastOffset, cacheBlock.size);
                    }
                    cacheBlock.free();
                    evictBytes += cacheBlock.size;
                    cacheEvictListeners.forEach(listener -> listener.onCacheEvict(entry.getKey().streamId, cacheBlock.firstOffset, cacheBlock.lastOffset, cacheBlock.size));
                    if (forceEvict) {
                        if (evictBytes >= size) {
                            return evictBytes;
                        }
                    } else if (maxSize - this.size.addAndGet(-cacheBlock.size) >= size) {
                        return evictBytes;
                    }
                }
            }
        }
        return evictBytes;
    }

    private void logCacheStatus() {
        try {
            readLock.lock();
            List<Long> sortedStreamIds = new ArrayList<>(stream2cache.keySet());
            sortedStreamIds.sort(Long::compareTo);
            for (Long streamId : sortedStreamIds) {
                StreamCache streamCache = stream2cache.get(streamId);
                if (streamCache == null) {
                    continue;
                }
                for (Map.Entry<Long, CacheBlock> entry : streamCache.blocks().entrySet()) {
                    CacheBlockKey key = new CacheBlockKey(streamId, entry.getValue().firstOffset);
                    LOGGER.debug("[S3BlockCache] stream cache block, stream={}, {}-{}, inactive={}, active={}, total bytes: {} ",
                        streamId, entry.getValue().firstOffset, entry.getValue().lastOffset, inactive.containsKey(key), active.containsKey(key), entry.getValue().size);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private void put(long streamId, StreamCache streamCache, CacheBlock cacheBlock) {
        streamCache.put(cacheBlock);
        active.put(new CacheBlockKey(streamId, cacheBlock.firstOffset), cacheBlock.size);
        size.getAndAdd(cacheBlock.size);
    }

    @Override
    public int handle(int memoryRequired) {
        writeLock.lock();
        try {
            return ensureCapacity0(memoryRequired, true);
        } catch (Throwable e) {
            LOGGER.error("[UNEXPECTED] handle OOM failed", e);
            return 0;
        } finally {
            writeLock.unlock();
        }
    }

    public interface CacheEvictListener {
        void onCacheEvict(long streamId, long startOffset, long endOffset, int size);
    }

    static final class CacheBlockKey {
        private final long streamId;
        private final long startOffset;

        CacheBlockKey(long streamId, long startOffset) {
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
            var that = (CacheBlockKey) obj;
            return this.streamId == that.streamId &&
                this.startOffset == that.startOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, startOffset);
        }

        @Override
        public String toString() {
            return "CacheBlockKey[" +
                "streamId=" + streamId + ", " +
                "startOffset=" + startOffset + ']';
        }

    }

    public static class CacheBlock {
        List<StreamRecordBatch> records;
        long firstOffset;
        long lastOffset;
        int size;
        ReadAheadRecord readAheadRecord;

        public CacheBlock(List<StreamRecordBatch> records, ReadAheadRecord readAheadRecord) {
            this.records = records;
            this.firstOffset = records.get(0).getBaseOffset();
            this.lastOffset = records.get(records.size() - 1).getLastOffset();
            this.size = records.stream().mapToInt(StreamRecordBatch::size).sum();
            this.size += records.size() * OBJECT_OVERHEAD;
            this.readAheadRecord = readAheadRecord;
        }

        public void free() {
            records.forEach(StreamRecordBatch::release);
            records = null;
        }

        public long size() {
            return size;
        }
    }

    public static class GetCacheResult {
        private final List<StreamRecordBatch> records;
        private final List<ReadAheadRecord> readAheadRecords;

        private GetCacheResult(List<StreamRecordBatch> records, List<ReadAheadRecord> readAheadRecords) {
            this.records = records;
            this.readAheadRecords = readAheadRecords;
        }

        public static GetCacheResult empty() {
            return new GetCacheResult(Collections.emptyList(), Collections.emptyList());
        }

        public static GetCacheResult of(List<StreamRecordBatch> records, List<ReadAheadRecord> readAheadRecords) {
            return new GetCacheResult(records, readAheadRecords);
        }

        public List<StreamRecordBatch> getRecords() {
            return records;
        }

        public List<ReadAheadRecord> getReadAheadRecords() {
            return readAheadRecords;
        }
    }
}
