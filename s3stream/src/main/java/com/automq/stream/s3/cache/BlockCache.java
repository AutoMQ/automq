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


import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.cache.DefaultS3BlockCache.ReadAheadRecord;
import com.automq.stream.utils.biniarysearch.StreamRecordBatchList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BlockCache implements DirectByteBufAlloc.OOMHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockCache.class);
    static final int BLOCK_SIZE = 1024 * 1024;
    private final long maxSize;
    final Map<Long, StreamCache> stream2cache = new HashMap<>();
    private final LRUCache<CacheBlockKey, Integer> inactive = new LRUCache<>();
    private final LRUCache<CacheBlockKey, Integer> active = new LRUCache<>();
    private final AtomicLong size = new AtomicLong();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    public BlockCache(long maxSize) {
        this.maxSize = maxSize;
        DirectByteBufAlloc.registerOOMHandlers(this);
    }

    public void put(long streamId, List<StreamRecordBatch> records) {
        try {
            writeLock.lock();
            put0(streamId, -1, records);
        } finally {
            writeLock.unlock();
        }
    }

    public void put(long streamId, long raAsyncOffset, List<StreamRecordBatch> records) {
        try {
            writeLock.lock();
            put0(streamId, raAsyncOffset, records);
        } finally {
            writeLock.unlock();
        }
    }

    void put0(long streamId, long raAsyncOffset, List<StreamRecordBatch> records) {
        if (maxSize == 0 || records.isEmpty()) {
            records.forEach(StreamRecordBatch::release);
            return;
        }
        records = new ArrayList<>(records);
        StreamCache streamCache = stream2cache.computeIfAbsent(streamId, id -> new StreamCache());
        long startOffset = records.get(0).getBaseOffset();
        long endOffset = records.get(records.size() - 1).getLastOffset();

        if (raAsyncOffset == -1) {
            raAsyncOffset = startOffset;
        }

        if (raAsyncOffset < startOffset || raAsyncOffset >= endOffset) {
            LOGGER.error("raAsyncOffset out of range, raAsyncOffset: {}, startOffset: {}, endOffset: {}", raAsyncOffset, startOffset, endOffset);
        }

        int size = records.stream().mapToInt(StreamRecordBatch::size).sum();

        LOGGER.debug("[S3BlockCache] put block cache, stream={}, {}-{}, total bytes: {} ", streamId, startOffset, endOffset, size);

        // remove overlapped part.
        SortedMap<Long, CacheBlock> tailMap = streamCache.tailBlocks(startOffset);
        for (Map.Entry<Long, CacheBlock> entry : tailMap.entrySet()) {
            CacheBlock cacheBlock = entry.getValue();
            if (cacheBlock.firstOffset >= endOffset) {
                break;
            }
            if (isWithinRange(raAsyncOffset, cacheBlock.firstOffset, cacheBlock.lastOffset) && cacheBlock.readAheadRecord == null) {
                cacheBlock.readAheadRecord = new ReadAheadRecord(endOffset, size);
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
                        new ReadAheadRecord(endOffset, size) : null;
                put(streamId, streamCache, new CacheBlock(batchList, raRecord));
                batchList = new LinkedList<>();
                batchList.add(record);
                partSize = record.size();
            }
            expectStartOffset = record.getLastOffset();
        }
        if (!batchList.isEmpty()) {
            ReadAheadRecord raRecord = isWithinRange(raAsyncOffset, batchList.getFirst().getBaseOffset(), batchList.getLast().getLastOffset()) ?
                    new ReadAheadRecord(endOffset, size) : null;
            put(streamId, streamCache, new CacheBlock(batchList, raRecord));
        }
    }

    private boolean isWithinRange(long raAsyncOffset, long startOffset, long endOffset) {
        return raAsyncOffset >= startOffset && raAsyncOffset < endOffset;
    }

    public boolean checkRange(long streamId, long startOffset, int maxBytes) {
        if (maxBytes <= 0) {
            return true;
        }
        try {
            readLock.lock();
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


    /**
     * Get records from cache.
     * Note: the records is retained, the caller should release it.
     */
    public GetCacheResult get(long streamId, long startOffset, long endOffset, int maxBytes) {
        if (startOffset >= endOffset || maxBytes <= 0) {
            return GetCacheResult.empty();
        }
        try {
            readLock.lock();
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
                if (!active.touch(cacheBlockKey)) {
                    inactive.touch(cacheBlockKey);
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
        if (startIndex == -1) {
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
                    cacheBlock.free();
                    evictBytes += cacheBlock.size;
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

    private void put(long streamId, StreamCache streamCache, CacheBlock cacheBlock) {
        streamCache.put(cacheBlock);
        active.put(new CacheBlockKey(streamId, cacheBlock.firstOffset), cacheBlock.size);
        size.getAndAdd(cacheBlock.size);
    }

    @Override
    public int handle(int memoryRequired) {
        try {
            return ensureCapacity0(memoryRequired, true);
        } catch (Throwable e) {
            LOGGER.error("[UNEXPECTED] handle OOM failed", e);
            return 0;
        }
    }

    record CacheBlockKey(long streamId, long startOffset) {

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
            this.readAheadRecord = readAheadRecord;
        }

        public void free() {
            records.forEach(StreamRecordBatch::release);
            records = null;
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
