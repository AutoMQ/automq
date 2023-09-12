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

package kafka.log.s3.cache;


import kafka.log.s3.model.StreamRecordBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BlockCache {
    private final long maxSize;
    private final Map<Long, NavigableMap<Long, CacheBlock>> stream2cache = new HashMap<>();
    private final LRUCache<CacheKey, Integer> inactive = new LRUCache<>();
    private final LRUCache<CacheKey, Integer> active = new LRUCache<>();
    private final AtomicLong size = new AtomicLong();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    public BlockCache(long maxSize) {
        this.maxSize = maxSize;
    }

    public void put(long streamId, List<StreamRecordBatch> records) {
        try {
            writeLock.lock();
            put0(streamId, records);
        } finally {
            writeLock.unlock();
        }
    }

    public void put0(long streamId, List<StreamRecordBatch> records) {
        if (maxSize == 0 || records.isEmpty()) {
            records.forEach(StreamRecordBatch::release);
            return;
        }
        boolean overlapped = false;
        records = new ArrayList<>(records);
        NavigableMap<Long, CacheBlock> streamCache = stream2cache.computeIfAbsent(streamId, id -> new TreeMap<>());
        long startOffset = records.get(0).getBaseOffset();
        long endOffset = records.get(records.size() - 1).getLastOffset();
        // TODO: generate readahead.
        Map.Entry<Long, CacheBlock> floorEntry = streamCache.floorEntry(startOffset);
        SortedMap<Long, CacheBlock> tailMap = streamCache.tailMap(floorEntry != null ? floorEntry.getKey() : startOffset);
        // remove overlapped part.
        for (Map.Entry<Long, CacheBlock> entry : tailMap.entrySet()) {
            CacheBlock cacheBlock = entry.getValue();
            if (cacheBlock.firstOffset >= endOffset) {
                break;
            }
            // overlap is a rare case, so removeIf is fine for the performance.
            if (records.removeIf(record -> {
                boolean remove = record.getLastOffset() > cacheBlock.firstOffset && record.getBaseOffset() < cacheBlock.lastOffset;
                if (remove) {
                    record.release();
                }
                return remove;
            })) {
                overlapped = true;
            }
        }

        // ensure the cache size.
        int size = records.stream().mapToInt(StreamRecordBatch::size).sum();
        ensureCapacity(size);

        // TODO: split records to 1MB blocks.
        if (overlapped) {
            // split to multiple cache blocks.
            long expectStartOffset = -1L;
            List<StreamRecordBatch> part = new ArrayList<>(records.size() / 2);
            for (StreamRecordBatch record : records) {
                if (expectStartOffset == -1L || record.getBaseOffset() == expectStartOffset) {
                    part.add(record);
                } else {
                    put(streamId, streamCache, new CacheBlock(part));
                    part = new ArrayList<>(records.size() / 2);
                    part.add(record);
                }
                expectStartOffset = record.getLastOffset();
            }
            if (!part.isEmpty()) {
                put(streamId, streamCache, new CacheBlock(part));
            }
        } else {
            put(streamId, streamCache, new CacheBlock(records));
        }

    }


    /**
     * Get records from cache.
     * Note: the records is retained, the caller should release it.
     */
    public GetCacheResult get(long streamId, long startOffset, long endOffset, int maxBytes) {
        try {
            readLock.lock();
            return get0(streamId, startOffset, endOffset, maxBytes);
        } finally {
            readLock.unlock();
        }
    }

    public GetCacheResult get0(long streamId, long startOffset, long endOffset, int maxBytes) {
        NavigableMap<Long, CacheBlock> streamCache = stream2cache.get(streamId);
        if (streamCache == null) {
            return GetCacheResult.empty();
        }
        Map.Entry<Long, CacheBlock> floorEntry = streamCache.floorEntry(startOffset);
        streamCache = streamCache.tailMap(floorEntry != null ? floorEntry.getKey() : startOffset, true);
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        Readahead readahead = null;
        LinkedList<StreamRecordBatch> records = new LinkedList<>();
        for (Map.Entry<Long, CacheBlock> entry : streamCache.entrySet()) {
            CacheBlock cacheBlock = entry.getValue();
            if (cacheBlock.lastOffset < nextStartOffset || nextStartOffset < cacheBlock.firstOffset) {
                break;
            }
            if (readahead == null && cacheBlock.readahead != null) {
                readahead = cacheBlock.readahead;
            }
            nextMaxBytes = readFromCacheBlock(records, cacheBlock, nextStartOffset, endOffset, nextMaxBytes);
            nextStartOffset = records.getLast().getLastOffset();
            boolean blockCompletedRead = nextStartOffset >= cacheBlock.lastOffset;
            CacheKey cacheKey = new CacheKey(streamId, cacheBlock.firstOffset);
            if (blockCompletedRead) {
                active.remove(cacheKey);
                inactive.put(cacheKey, cacheBlock.size);
            } else {
                if (!active.touch(cacheKey)) {
                    inactive.touch(cacheKey);
                }
            }

            if (nextStartOffset >= endOffset || nextMaxBytes <= 0) {
                break;
            }

        }

        records.forEach(StreamRecordBatch::retain);
        return GetCacheResult.of(records, readahead);
    }

    private int readFromCacheBlock(LinkedList<StreamRecordBatch> records, CacheBlock cacheBlock,
                                   long nextStartOffset, long endOffset, int nextMaxBytes) {
        boolean matched = false;
        for (StreamRecordBatch record : cacheBlock.records) {
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
        if (maxSize - this.size.get() >= size) {
            return;
        }
        for (LRUCache<CacheKey, Integer> lru : List.of(inactive, active)) {
            for (; ; ) {
                Map.Entry<CacheKey, Integer> entry = lru.pop();
                if (entry == null) {
                    break;
                }
                CacheBlock cacheBlock = stream2cache.get(entry.getKey().streamId).remove(entry.getKey().startOffset);
                cacheBlock.free();
                if (maxSize - this.size.addAndGet(-entry.getValue()) >= size) {
                    return;
                }
            }
        }
    }

    private void put(long streamId, NavigableMap<Long, CacheBlock> streamCache, CacheBlock cacheBlock) {
        streamCache.put(cacheBlock.firstOffset, cacheBlock);
        active.put(new CacheKey(streamId, cacheBlock.firstOffset), cacheBlock.size);
        size.getAndAdd(cacheBlock.size);
    }

    static class CacheKey {
        final long streamId;
        final long startOffset;

        public CacheKey(long streamId, long startOffset) {
            this.streamId = streamId;
            this.startOffset = startOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, startOffset);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CacheKey) {
                CacheKey other = (CacheKey) obj;
                return streamId == other.streamId && startOffset == other.startOffset;
            } else {
                return false;
            }
        }
    }

    static class CacheBlock {
        List<StreamRecordBatch> records;
        long firstOffset;
        long lastOffset;
        int size;
        Readahead readahead;

        public CacheBlock(List<StreamRecordBatch> records, Readahead readahead) {
            this.records = records;
            this.firstOffset = records.get(0).getBaseOffset();
            this.lastOffset = records.get(records.size() - 1).getLastOffset();
            this.size = records.stream().mapToInt(StreamRecordBatch::size).sum();
            this.readahead = readahead;
        }

        public CacheBlock(List<StreamRecordBatch> records) {
            this(records, null);
        }

        public void free() {
            records.forEach(StreamRecordBatch::release);
            records = null;
        }
    }

    public static class GetCacheResult {
        private final List<StreamRecordBatch> records;
        private final Readahead readahead;

        private GetCacheResult(List<StreamRecordBatch> records, Readahead readahead) {
            this.records = records;
            this.readahead = readahead;
        }

        public static GetCacheResult empty() {
            return new GetCacheResult(Collections.emptyList(), null);
        }

        public static GetCacheResult of(List<StreamRecordBatch> records, Readahead readahead) {
            return new GetCacheResult(records, readahead);
        }

        public List<StreamRecordBatch> getRecords() {
            return records;
        }

        public Optional<Readahead> getReadahead() {
            if (readahead == null) {
                return Optional.empty();
            } else {
                return Optional.of(readahead);
            }
        }
    }

    public static class Readahead {
        private final long startOffset;
        private final int size;

        public Readahead(long startOffset, int size) {
            this.startOffset = startOffset;
            this.size = size;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public int getSize() {
            return size;
        }
    }

}
