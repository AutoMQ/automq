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

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.biniarysearch.StreamRecordBatchList;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.cache.LogCache.StreamRange.NOOP_OFFSET;
import static com.automq.stream.s3.model.StreamRecordBatch.OBJECT_OVERHEAD;
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
    private long confirmOffset;

    public LogCache(long capacity, long cacheBlockMaxSize, int maxCacheBlockStreamCount,
        Consumer<LogCacheBlock> blockFreeListener) {
        this.capacity = capacity;
        this.cacheBlockMaxSize = cacheBlockMaxSize;
        this.maxCacheBlockStreamCount = maxCacheBlockStreamCount;
        this.activeBlock = new LogCacheBlock(cacheBlockMaxSize, maxCacheBlockStreamCount);
        this.blocks.add(activeBlock);
        this.blockFreeListener = blockFreeListener;
        S3StreamMetricsManager.registerDeltaWalCacheSizeSupplier(size::get);
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
        size.addAndGet(recordBatch.size() + OBJECT_OVERHEAD);
        readLock.lock();
        boolean full;
        try {
            full = activeBlock.put(recordBatch);
        } finally {
            readLock.unlock();
        }
        StorageOperationStats.getInstance().appendLogCacheStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
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
            records = get0(streamId, startOffset, endOffset, maxBytes);
            records.forEach(StreamRecordBatch::retain);
        } finally {
            readLock.unlock();
        }

        long timeElapsed = TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS);
        boolean isCacheHit = !records.isEmpty() && records.get(0).getBaseOffset() <= startOffset;
        StorageOperationStats.getInstance().readLogCacheStats(isCacheHit).record(timeElapsed);
        return records;
    }

    public List<StreamRecordBatch> get0(long streamId, long startOffset, long endOffset, int maxBytes) {
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
            block.confirmOffset = confirmOffset;
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
        AtomicLong remainSize = new AtomicLong(currSize);
        List<LogCacheBlock> removed = new ArrayList<>();
        writeLock.lock();
        try {
            Iterator<LogCacheBlock> iter = blocks.iterator();
            while (iter.hasNext()) {
                if (remainSize.get() <= capacity * 0.9) {
                    break;
                }
                LogCacheBlock block = iter.next();
                if (block.free) {
                    iter.remove();
                    remainSize.addAndGet(-block.size());
                    removed.add(block);
                } else {
                    break;
                }
            }

        } finally {
            writeLock.unlock();
        }
        size.addAndGet(remainSize.get() - currSize);
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

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public long size() {
        return size.get();
    }

    public static class LogCacheBlock {
        private static final AtomicLong BLOCK_ID_ALLOC = new AtomicLong();
        final Map<Long, StreamCache> map = new ConcurrentHashMap<>();
        private final long blockId;
        private final long maxSize;
        private final int maxStreamCount;
        private final long createdTimestamp = System.currentTimeMillis();
        private final AtomicLong size = new AtomicLong();
        volatile boolean free;
        private long confirmOffset;

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

        public boolean put(StreamRecordBatch recordBatch) {
            map.compute(recordBatch.getStreamId(), (id, cache) -> {
                if (cache == null) {
                    cache = new StreamCache();
                }
                cache.add(recordBatch);
                return cache;
            });
            int recordSize = recordBatch.size();
            return size.addAndGet(recordSize + OBJECT_OVERHEAD) >= maxSize || map.size() >= maxStreamCount;
        }

        public List<StreamRecordBatch> get(long streamId, long startOffset, long endOffset, int maxBytes) {
            StreamCache cache = map.get(streamId);
            if (cache == null) {
                return Collections.emptyList();
            }
            return cache.get(startOffset, endOffset, maxBytes);
        }

        StreamRange getStreamRange(long streamId) {
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

        public long confirmOffset() {
            return confirmOffset;
        }

        public void confirmOffset(long confirmOffset) {
            this.confirmOffset = confirmOffset;
        }

        public long size() {
            return size.get();
        }

        public void free() {
            suppress(() -> {
                map.forEach((streamId, records) -> records.free());
                map.clear();
            }, LOGGER);
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

    static class StreamRange {
        public static final long NOOP_OFFSET = -1L;
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
            if (startIndex == -1) {
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

        synchronized void free() {
            records.forEach(StreamRecordBatch::release);
            records.clear();
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
