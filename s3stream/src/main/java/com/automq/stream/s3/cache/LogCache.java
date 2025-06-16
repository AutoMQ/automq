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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.utils.biniarysearch.StreamRecordBatchList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;

import static com.automq.stream.s3.cache.LogCache.StreamRange.NOOP_OFFSET;
import static com.automq.stream.utils.FutureUtil.suppress;

public class LogCache {
    public static final long MATCH_ALL_STREAMS = -1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogCache.class);
    private static final int DEFAULT_MAX_BLOCK_STREAM_COUNT = 10000;
    private static final Consumer<LogCacheBlock> DEFAULT_BLOCK_FREE_LISTENER = block -> {
    };
    final List<LogCacheBlock> blocks = new ArrayList<>();
    private final long capacity;
    private final long cacheBlockMaxSize;
    private final int maxCacheBlockStreamCount;
    private final AtomicLong size = new AtomicLong();
    private final Consumer<LogCacheBlock> blockFreeListener;
    // read write lock which guards the <code>LogCache.blocks</code>
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private LogCacheBlock activeBlock;
    private RecordOffset lastRecordOffset;

    public LogCache(long capacity, long cacheBlockMaxSize, int maxCacheBlockStreamCount,
        Consumer<LogCacheBlock> blockFreeListener) {
        this.capacity = capacity;
        this.cacheBlockMaxSize = cacheBlockMaxSize;
        this.maxCacheBlockStreamCount = maxCacheBlockStreamCount;
        this.activeBlock = new LogCacheBlock(cacheBlockMaxSize, maxCacheBlockStreamCount);
        this.blocks.add(activeBlock);
        this.blockFreeListener = blockFreeListener;
    }

    public LogCache(long capacity, long cacheBlockMaxSize) {
        this(capacity, cacheBlockMaxSize, DEFAULT_MAX_BLOCK_STREAM_COUNT, DEFAULT_BLOCK_FREE_LISTENER);
    }

    public LogCache(long capacity, long cacheBlockMaxSize, int maxCacheBlockStreamCount) {
        this(capacity, cacheBlockMaxSize, maxCacheBlockStreamCount, DEFAULT_BLOCK_FREE_LISTENER);
    }

    /**
     * Put a record batch into the cache.
     * record batched in the same stream should be put in order.
     */
    public boolean put(StreamRecordBatch recordBatch) {
        long startTime = System.nanoTime();
        tryRealFree();
        size.addAndGet(recordBatch.occupiedSize());
        readLock.lock();
        boolean full;
        try {
            full = activeBlock.put(recordBatch);
        } finally {
            readLock.unlock();
        }
        StorageOperationStats.getInstance().appendLogCacheStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
        return full;
    }

    public List<StreamRecordBatch> get(long streamId, long startOffset, long endOffset, int maxBytes) {
        return get(TraceContext.DEFAULT, streamId, startOffset, endOffset, maxBytes);
    }

    /**
     * Get streamId [startOffset, endOffset) range records with maxBytes limit.
     * <p>
     * - If the requested range can be fully satisfied, then return the corresponding cached records.
     * - Otherwise, return the latest continuous records and leave the remaining range to block cache.
     * <p>
     * e.g. Cached block: [0, 10], [100, 200]
     * <p>
     * - query [0,10] returns [0,10] (fully satisfied)
     * </p>
     * <p>
     * - query [0, 11] returns empty list (left intersect, leave all data to block cache for simplification)
     * </p>
     * <p>
     * - query [5, 20] returns empty list (left intersect, leave all data to block cache for simplification)
     * </p>
     * <p>
     * - query [90, 110) returns [100, 110] (right intersect, leave[90, 100) to block cache)
     * </p>
     * <p>
     * - query [40, 50] returns empty list (miss match)
     * </p>
     * Note: the records is retained, the caller should release it.
     */
    @WithSpan
    public List<StreamRecordBatch> get(TraceContext context,
        @SpanAttribute long streamId,
        @SpanAttribute long startOffset,
        @SpanAttribute long endOffset,
        @SpanAttribute int maxBytes) {
        context.currentContext();
        long startTime = System.nanoTime();
        List<StreamRecordBatch> records;
        readLock.lock();
        try {
            Long streamIdLong = streamId;
            records = get0(streamIdLong, startOffset, endOffset, maxBytes);
            records.forEach(StreamRecordBatch::retain);
        } finally {
            readLock.unlock();
        }

        long timeElapsed = TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS);
        boolean isCacheHit = !records.isEmpty() && records.get(0).getBaseOffset() <= startOffset;
        StorageOperationStats.getInstance().readLogCacheStats(isCacheHit).record(timeElapsed);
        return records;
    }

    public List<StreamRecordBatch> get0(Long streamId, long startOffset, long endOffset, int maxBytes) {
        List<StreamRecordBatch> rst = new LinkedList<>();
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        boolean fulfill = false;
        List<LogCacheBlock> blocks = this.blocks;
        for (LogCacheBlock archiveBlock : blocks) {
            List<StreamRecordBatch> records = archiveBlock.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
            if (records.isEmpty()) {
                continue;
            }
            nextStartOffset = records.get(records.size() - 1).getLastOffset();
            int recordsSize = 0;
            for (StreamRecordBatch record : records) {
                recordsSize += record.size();
            }
            nextMaxBytes -= Math.min(nextMaxBytes, recordsSize);
            rst.addAll(records);
            if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                fulfill = true;
                break;
            }
        }
        if (fulfill) {
            return rst;
        } else {
            long lastBlockStreamStartOffset = NOOP_OFFSET;
            for (int i = blocks.size() - 1; i >= 0; i--) {
                LogCacheBlock block = blocks.get(i);
                StreamRange streamRange = block.getStreamRange(streamId);
                if (streamRange.endOffset == NOOP_OFFSET) {
                    continue;
                }
                if (lastBlockStreamStartOffset == NOOP_OFFSET || lastBlockStreamStartOffset == streamRange.endOffset) {
                    lastBlockStreamStartOffset = streamRange.startOffset;
                } else {
                    break;
                }
            }
            if (lastBlockStreamStartOffset == NOOP_OFFSET /* Mismatch */
                || lastBlockStreamStartOffset >= endOffset /* non-right intersect */
                || lastBlockStreamStartOffset <= startOffset /* left intersect */) {
                return Collections.emptyList();
            }
            return get0(streamId, lastBlockStreamStartOffset, endOffset, maxBytes);
        }
    }

    public LogCacheBlock archiveCurrentBlock() {
        writeLock.lock();
        try {
            LogCacheBlock block = activeBlock;
            block.lastRecordOffset = lastRecordOffset;
            activeBlock = new LogCacheBlock(cacheBlockMaxSize, maxCacheBlockStreamCount);
            blocks.add(activeBlock);
            return block;
        } finally {
            writeLock.unlock();
        }
    }

    public Optional<LogCacheBlock> archiveCurrentBlockIfContains(long streamId) {
        writeLock.lock();
        try {
            return archiveCurrentBlockIfContains0(streamId);
        } finally {
            writeLock.unlock();
        }
    }

    Optional<LogCacheBlock> archiveCurrentBlockIfContains0(long streamId) {
        if (streamId == MATCH_ALL_STREAMS) {
            if (activeBlock.size() > 0) {
                return Optional.of(archiveCurrentBlock());
            } else {
                return Optional.empty();
            }
        } else {
            if (activeBlock.map.containsKey(streamId)) {
                return Optional.of(archiveCurrentBlock());
            } else {
                return Optional.empty();
            }
        }

    }

    public void markFree(LogCacheBlock block) {
        block.free = true;
        tryRealFree();
    }

    private void tryRealFree() {
        long currSize = size.get();
        if (currSize <= capacity * 0.9) {
            return;
        }
        List<LogCacheBlock> removed = new ArrayList<>();
        long freeSize = 0L;
        writeLock.lock();
        try {
            // free blocks
            currSize = size.get();
            Iterator<LogCacheBlock> iter = blocks.iterator();
            while (iter.hasNext()) {
                if (currSize - freeSize <= capacity * 0.9) {
                    break;
                }
                LogCacheBlock block = iter.next();
                if (block.free) {
                    iter.remove();
                    freeSize += block.size();
                    removed.add(block);
                } else {
                    break;
                }
            }
            // merge blocks to speed up the get.
            LogCacheBlock mergedBlock = null;
            iter = blocks.iterator();
            while (iter.hasNext()) {
                LogCacheBlock block = iter.next();
                if (!block.free) {
                    break;
                }
                if (mergedBlock == null
                    || mergedBlock.size() + block.size() >= cacheBlockMaxSize
                    || isDiscontinuous(mergedBlock, block)) {
                    mergedBlock = block;
                    continue;
                }
                mergeBlock(mergedBlock, block);
                iter.remove();
            }
        } finally {
            writeLock.unlock();
        }
        size.addAndGet(-freeSize);
        removed.forEach(b -> {
            blockFreeListener.accept(b);
            b.free();
        });
    }

    public int forceFree(int required) {
        AtomicInteger freedBytes = new AtomicInteger();
        List<LogCacheBlock> removed = new ArrayList<>();
        writeLock.lock();
        try {
            blocks.removeIf(block -> {
                if (!block.free || freedBytes.get() >= required) {
                    return false;
                }
                long blockSize = block.size();
                size.addAndGet(-blockSize);
                freedBytes.addAndGet((int) blockSize);
                removed.add(block);
                return true;
            });
        } finally {
            writeLock.unlock();
        }
        removed.forEach(b -> {
            blockFreeListener.accept(b);
            b.free();
        });
        return freedBytes.get();
    }

    public void setLastRecordOffset(RecordOffset lastRecordOffset) {
        readLock.lock();
        try {
            this.lastRecordOffset = lastRecordOffset;
        } finally {
            readLock.unlock();
        }
    }

    public long size() {
        return size.get();
    }

    public void clearStreamRecords(long streamId) {
        readLock.lock();
        try {
            for (LogCacheBlock block : blocks) {
                size.addAndGet(-block.free(streamId));
            }
        } finally {
            readLock.unlock();
        }
    }

    static boolean isDiscontinuous(LogCacheBlock left, LogCacheBlock right) {
        for (Map.Entry<Long, StreamCache> entry : left.map.entrySet()) {
            Long streamId = entry.getKey();
            StreamCache leftStreamCache = entry.getValue();
            StreamCache rightStreamCache = right.map.get(streamId);
            if (rightStreamCache == null) {
                continue;
            }
            if (leftStreamCache.endOffset() != rightStreamCache.startOffset()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Merge the right block to the left block
     */
    static void mergeBlock(LogCacheBlock left, LogCacheBlock right) {
        synchronized (left) {
            left.size.addAndGet(right.size());
            left.lastRecordOffset = right.lastRecordOffset;
            left.map.forEach((streamId, leftStreamCache) -> {
                StreamCache rightStreamCache = right.map.get(streamId);
                if (rightStreamCache != null) {
                    leftStreamCache.records.addAll(rightStreamCache.records);
                    leftStreamCache.endOffset(rightStreamCache.endOffset());
                }
            });
            right.map.forEach((streamId, rightStreamCache) -> {
                if (!left.map.containsKey(streamId)) {
                    left.map.put(streamId, rightStreamCache);
                }
            });
        }
    }

    public static class LogCacheBlock {
        private static final AtomicLong BLOCK_ID_ALLOC = new AtomicLong();
        final Map<Long, StreamCache> map = new ConcurrentHashMap<>();
        private final long blockId;
        private final long maxSize;
        private final int maxStreamCount;
        private final long createdTimestamp = System.currentTimeMillis();
        private final AtomicLong size = new AtomicLong();
        private final List<FreeListener> freeListeners = new ArrayList<>();
        volatile boolean free;
        private RecordOffset lastRecordOffset;

        public LogCacheBlock(long maxSize, int maxStreamCount) {
            this.blockId = BLOCK_ID_ALLOC.getAndIncrement();
            this.maxSize = maxSize;
            this.maxStreamCount = maxStreamCount;
        }

        public LogCacheBlock(long maxSize) {
            this(maxSize, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        }

        public long blockId() {
            return blockId;
        }

        public boolean isFull() {
            return size.get() >= maxSize || map.size() >= maxStreamCount;
        }

        public boolean put(StreamRecordBatch recordBatch) {
            map.compute(recordBatch.getStreamId(), (id, cache) -> {
                if (cache == null) {
                    cache = new StreamCache();
                }
                cache.add(recordBatch);
                return cache;
            });
            size.addAndGet(recordBatch.occupiedSize());
            return isFull();
        }

        public List<StreamRecordBatch> get(Long streamId, long startOffset, long endOffset, int maxBytes) {
            StreamCache cache = map.get(streamId);
            if (cache == null) {
                return Collections.emptyList();
            }
            return cache.get(startOffset, endOffset, maxBytes);
        }

        StreamRange getStreamRange(Long streamId) {
            StreamCache streamCache = map.get(streamId);
            if (streamCache == null) {
                return new StreamRange(NOOP_OFFSET, NOOP_OFFSET);
            } else {
                return streamCache.range();
            }
        }

        public Map<Long, List<StreamRecordBatch>> records() {
            return map.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().records))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public RecordOffset lastRecordOffset() {
            return lastRecordOffset;
        }

        public void lastRecordOffset(RecordOffset lastRecordOffset) {
            this.lastRecordOffset = lastRecordOffset;
        }

        public long size() {
            return size.get();
        }

        public void free() {
            suppress(() -> {
                List<StreamRangeBound> streams = new ArrayList<>(map.size());
                map.forEach((streamId, records) -> {
                    streams.add(new StreamRangeBound(streamId, records.startOffset(), records.endOffset()));
                    records.free();
                });
                map.clear();
                freeListeners.forEach(listener -> listener.onFree(streams));
            }, LOGGER);
        }

        public long free(long streamId) {
            AtomicLong size = new AtomicLong();
            suppress(() -> {
                StreamCache streamCache = map.remove(streamId);
                if (streamCache != null) {
                    size.addAndGet(streamCache.free());
                }
            }, LOGGER);
            this.size.addAndGet(-size.get());
            return size.get();
        }

        public void addFreeListener(FreeListener freeListener) {
            freeListeners.add(freeListener);
        }

        public long createdTimestamp() {
            return createdTimestamp;
        }

        public boolean containsStream(long streamId) {
            if (MATCH_ALL_STREAMS == streamId) {
                return true;
            }
            return map.containsKey(streamId);
        }
    }

    public interface FreeListener {
        void onFree(List<StreamRangeBound> streamRanges);
    }

    public static class StreamRangeBound {
        private final long streamId;
        private final long startOffset;
        private final long endOffset;

        public StreamRangeBound(long streamId, long startOffset, long endOffset) {
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
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
    }

    static class StreamRange {
        public static final long NOOP_OFFSET = -1L;
        public static final StreamRange NOOP = new StreamRange(NOOP_OFFSET, NOOP_OFFSET);
        long startOffset;
        long endOffset;

        public StreamRange(long startOffset, long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }
    }

    static class StreamCache {
        List<StreamRecordBatch> records = new ArrayList<>();
        long startOffset = NOOP_OFFSET;
        long endOffset = NOOP_OFFSET;
        Map<Long, IndexAndCount> offsetIndexMap = new HashMap<>();

        synchronized void add(StreamRecordBatch recordBatch) {
            if (recordBatch.getBaseOffset() != endOffset && endOffset != NOOP_OFFSET) {
                RuntimeException ex = new IllegalArgumentException(String.format("streamId=%s record batch base offset mismatch, expect %s, actual %s",
                    recordBatch.getStreamId(), endOffset, recordBatch.getBaseOffset()));
                LOGGER.error("[FATAL]", ex);
            }
            records.add(recordBatch);
            if (startOffset == NOOP_OFFSET) {
                startOffset = recordBatch.getBaseOffset();
            }
            endOffset = recordBatch.getLastOffset();
        }

        synchronized List<StreamRecordBatch> get(long startOffset, long endOffset, int maxBytes) {
            if (this.startOffset > startOffset || this.endOffset <= startOffset) {
                return Collections.emptyList();
            }
            int startIndex = searchStartIndex(startOffset);
            if (startIndex < 0) {
                // mismatched
                return Collections.emptyList();
            }
            int endIndex = -1;
            int remainingBytesSize = maxBytes;
            long rstEndOffset = NOOP_OFFSET;
            for (int i = startIndex; i < records.size(); i++) {
                StreamRecordBatch record = records.get(i);
                endIndex = i + 1;
                remainingBytesSize -= Math.min(remainingBytesSize, record.size());
                rstEndOffset = record.getLastOffset();
                if (record.getLastOffset() >= endOffset || remainingBytesSize == 0) {
                    break;
                }
            }
            if (rstEndOffset != NOOP_OFFSET) {
                map(rstEndOffset, endIndex);
            }
            return new ArrayList<>(records.subList(startIndex, endIndex));
        }

        int searchStartIndex(long startOffset) {
            IndexAndCount indexAndCount = offsetIndexMap.get(startOffset);
            if (indexAndCount != null) {
                unmap(startOffset, indexAndCount);
                return indexAndCount.index;
            } else {
                // slow path
                StreamRecordBatchList search = new StreamRecordBatchList(records);
                return search.search(startOffset);
            }
        }

        final void map(long offset, int index) {
            offsetIndexMap.compute(offset, (k, v) -> {
                if (v == null) {
                    return new IndexAndCount(index);
                } else {
                    v.inc();
                    return v;
                }
            });
        }

        final void unmap(long startOffset, IndexAndCount indexAndCount) {
            if (indexAndCount.dec() == 0) {
                offsetIndexMap.remove(startOffset);
            }
        }

        synchronized StreamRange range() {
            return new StreamRange(startOffset, endOffset);
        }

        synchronized long free() {
            AtomicLong size = new AtomicLong();
            records.forEach(record -> {
                size.addAndGet(record.occupiedSize());
                record.release();
            });
            records.clear();
            return size.get();
        }

        synchronized long startOffset() {
            return startOffset;
        }

        synchronized long endOffset() {
            return endOffset;
        }

        synchronized void endOffset(long endOffset) {
            this.endOffset = endOffset;
        }
    }

    static class IndexAndCount {
        int index;
        int count;

        public IndexAndCount(int index) {
            this.index = index;
            this.count = 1;
        }

        public void inc() {
            count++;
        }

        public int dec() {
            return --count;
        }

    }
}
