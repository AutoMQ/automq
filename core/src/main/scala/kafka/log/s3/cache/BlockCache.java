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

import kafka.log.s3.FlatStreamRecordBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class BlockCache {
    private final long maxSize;
    private final Map<Long, NavigableMap<Long, CacheBlock>> stream2cache = new HashMap<>();
    private final LRUCache<CacheKey, Integer> inactive = new LRUCache<>();
    private final LRUCache<CacheKey, Integer> active = new LRUCache<>();
    private final AtomicLong size = new AtomicLong();

    public BlockCache(long maxSize) {
        this.maxSize = maxSize;
    }

    public void put(long streamId, List<FlatStreamRecordBatch> records) {
        if (maxSize == 0 || records.isEmpty()) {
            return;
        }
        boolean overlapped = false;
        records = new ArrayList<>(records);
        NavigableMap<Long, CacheBlock> streamCache = stream2cache.computeIfAbsent(streamId, id -> new TreeMap<>());
        long startOffset = records.get(0).baseOffset;
        long endOffset = records.get(records.size() - 1).lastOffset();
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
            if (records.removeIf(record -> record.lastOffset() > cacheBlock.firstOffset && record.baseOffset < cacheBlock.lastOffset)) {
                overlapped = true;
            }
        }

        // ensure the cache size.
        int size = records.stream().mapToInt(FlatStreamRecordBatch::size).sum();
        ensureCapacity(size);

        // TODO: split records to 1MB blocks.
        if (overlapped) {
            // split to multiple cache blocks.
            long expectStartOffset = -1L;
            List<FlatStreamRecordBatch> part = new ArrayList<>(records.size() / 2);
            for (FlatStreamRecordBatch record : records) {
                if (expectStartOffset == -1L || record.baseOffset == expectStartOffset) {
                    part.add(record);
                } else {
                    put(streamId, streamCache, new CacheBlock(part));
                    part = new ArrayList<>(records.size() / 2);
                    part.add(record);
                }
                expectStartOffset = record.lastOffset();
            }
            if (!part.isEmpty()) {
                put(streamId, streamCache, new CacheBlock(part));
            }
        } else {
            put(streamId, streamCache, new CacheBlock(records));
        }

    }

    public GetCacheResult get(long streamId, long startOffset, long endOffset, int maxBytes) {
        NavigableMap<Long, CacheBlock> streamCache = stream2cache.get(streamId);
        if (streamCache == null) {
            return GetCacheResult.empty();
        }
        Map.Entry<Long, CacheBlock> floorEntry = streamCache.floorEntry(startOffset);
        streamCache = streamCache.tailMap(floorEntry != null ? floorEntry.getKey() : startOffset, true);
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        Readahead readahead = null;
        LinkedList<FlatStreamRecordBatch> records = new LinkedList<>();
        for (Map.Entry<Long, CacheBlock> entry : streamCache.entrySet()) {
            CacheBlock cacheBlock = entry.getValue();
            if (cacheBlock.lastOffset < nextStartOffset || nextStartOffset < cacheBlock.firstOffset) {
                break;
            }
            if (readahead == null && cacheBlock.readahead != null) {
                readahead = cacheBlock.readahead;
            }
            boolean matched = false;
            for (FlatStreamRecordBatch record : cacheBlock.records) {
                if (record.baseOffset <= nextStartOffset && record.lastOffset() > nextStartOffset) {
                    records.add(record);
                    nextStartOffset = record.lastOffset();
                    nextMaxBytes -= record.size();
                    matched = true;
                    if (nextStartOffset >= endOffset || nextMaxBytes <= 0) {
                        break;
                    }
                } else if (matched) {
                    break;
                }
            }
            boolean blockCompletedRead = records.getLast().lastOffset() >= cacheBlock.lastOffset;
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
        return GetCacheResult.of(records, readahead);
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
        List<FlatStreamRecordBatch> records;
        long firstOffset;
        long lastOffset;
        int size;
        Readahead readahead;

        public CacheBlock(List<FlatStreamRecordBatch> records) {
            this.records = records;
            this.firstOffset = records.get(0).baseOffset;
            this.lastOffset = records.get(records.size() - 1).lastOffset();
            this.size = records.stream().mapToInt(FlatStreamRecordBatch::size).sum();
        }

        public void free() {
            records.forEach(r -> r.encodedBuf.release());
            records = null;
        }
    }

    static class GetCacheResult {
        private final List<FlatStreamRecordBatch> records;
        private final Readahead readahead;

        private GetCacheResult(List<FlatStreamRecordBatch> records, Readahead readahead) {
            this.records = records;
            this.readahead = readahead;
        }

        public static GetCacheResult empty() {
            return new GetCacheResult(Collections.emptyList(), null);
        }

        public static GetCacheResult of(List<FlatStreamRecordBatch> records, Readahead readahead) {
            return new GetCacheResult(records, readahead);
        }

        public List<FlatStreamRecordBatch> getRecords() {
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

    static class Readahead {
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

    static class LRUCache<K, V> {
        private final LinkedHashMap<K, V> cache;
        private final Set<Map.Entry<K, V>> cacheEntrySet;

        public LRUCache() {
            cache = new LinkedHashMap<>(16, .75f, true);
            cacheEntrySet = cache.entrySet();
        }

        public boolean touch(K key) {
            return cache.get(key) != null;
        }

        public void put(K key, V value) {
            if (cache.put(key, value) != null) {
                touch(key);
            }
        }

        public Map.Entry<K, V> pop() {
            Iterator<Map.Entry<K, V>> it = cacheEntrySet.iterator();
            if (!it.hasNext()) {
                return null;
            }
            Map.Entry<K, V> entry = it.next();
            if (entry == null) {
                return null;
            }
            it.remove();
            return entry;
        }

        public boolean remove(K key) {
            return cache.remove(key) != null;
        }
    }

}
