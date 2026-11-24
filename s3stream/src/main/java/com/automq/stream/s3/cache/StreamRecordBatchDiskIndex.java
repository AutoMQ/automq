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

import com.automq.stream.s3.model.StreamRecordBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Finds disk batch ranges by stream ID and record offset range.
 * Lookups are best-effort. New batches replace overlapping entries for the same stream.
 * Operations are thread-safe.
 */
final class StreamRecordBatchDiskIndex {
    private final int maxEntries;
    private final NavigableMap<Key, Entry> entries = new TreeMap<>();
    private final LRUCache<Key, Entry> lru = new LRUCache<>();

    /**
     * @param maxEntries maximum indexed batches across all streams; nonpositive values disable indexing
     */
    StreamRecordBatchDiskIndex(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    /**
     * Indexes the batch's existing disk copy. Admission and lookup are best-effort.
     *
     * @param batch batch whose metadata is copied; the caller retains ownership
     * @param range identifies this batch's existing disk copy, returned by an earlier diskCache.put(batch) call
     */
    synchronized void put(StreamRecordBatch batch, FileCache.ReadRange range) {
        if (maxEntries <= 0) {
            return;
        }
        Key key = new Key(batch.getStreamId(), batch.getBaseOffset(), batch.getLastOffset());
        Entry entry = new Entry(key, batch.size(), range.cacheId(), range.position(), range.length());
        if (key.baseOffset < key.endOffset) {
            // At most one non-empty predecessor can overlap: admitted ranges never overlap.
            Map.Entry<Key, Entry> previous = entries.lowerEntry(new Key(key.streamId, key.baseOffset, Long.MIN_VALUE));
            while (previous != null && previous.getKey().streamId == key.streamId
                && previous.getKey().baseOffset == previous.getKey().endOffset) {
                previous = entries.lowerEntry(previous.getKey());
            }
            if (previous != null && previous.getKey().streamId == key.streamId
                && previous.getKey().endOffset > key.baseOffset) {
                entries.remove(previous.getKey());
                lru.remove(previous.getKey());
            }
            Iterator<Key> keys = entries.subMap(new Key(key.streamId, key.baseOffset, Long.MIN_VALUE), true,
                new Key(key.streamId, key.endOffset, Long.MIN_VALUE), false).keySet().iterator();
            while (keys.hasNext()) {
                Key overlapping = keys.next();
                if (overlapping.baseOffset < overlapping.endOffset) {
                    keys.remove();
                    lru.remove(overlapping);
                    }
            }
        }
        entries.put(key, entry);
        lru.put(key, entry);
        while (lru.size() > maxEntries) {
            entries.remove(lru.pop().getKey());
        }
    }

    /**
     * Finds a continuous sequence of complete batches covering the requested range or payload byte limit.
     * The first batch may start before startOffset, and the last batch may exceed endOffset or maxBytes.
     *
     * @param streamId stream to look up
     * @param startOffset first requested record offset, inclusive
     * @param endOffset last requested record offset, exclusive
     * @param maxBytes soft payload byte limit, including the complete final batch
     * @return ordered batch ranges, or an empty list when coverage from startOffset is unavailable,
     *     for an empty range or a nonpositive byte limit;
     *     disk reads may still miss
     */
    synchronized List<FileCache.ReadRange> get(long streamId, long startOffset,
        long endOffset, int maxBytes) {
        if (startOffset >= endOffset || maxBytes <= 0) {
            return Collections.emptyList();
        }
        return get0(streamId, startOffset, endOffset, maxBytes);
    }

    private List<FileCache.ReadRange> get0(long streamId, long startOffset, long endOffset, int maxBytes) {
        Entry first = null;
        for (Entry candidate : entries.headMap(new Key(streamId, startOffset, Long.MAX_VALUE), true)
            .descendingMap().values()) {
            Key key = candidate.key;
            if (key.streamId != streamId) {
                break;
            }
            // Empty entries can lie inside a covering batch without replacing it.
            if (key.baseOffset == key.endOffset) {
                continue;
            }
            first = candidate;
            break;
        }
        if (first == null || first.key.endOffset <= startOffset) {
            return Collections.emptyList();
        }

        List<Entry> selected = new ArrayList<>();
        long nextOffset = startOffset;
        long payloadBytes = 0;
        for (Entry entry : entries.tailMap(first.key, true).values()) {
            Key key = entry.key;
            if (key.streamId != streamId || key.baseOffset > nextOffset) {
                break;
            }
            if (key.baseOffset == key.endOffset && key.baseOffset < nextOffset) {
                continue;
            }
            selected.add(entry);
            nextOffset = key.endOffset;
            payloadBytes += entry.payloadBytes;
            if (nextOffset >= endOffset || payloadBytes >= maxBytes) {
                return ranges(selected);
            }
        }
        return Collections.emptyList();
    }

    private List<FileCache.ReadRange> ranges(List<Entry> selected) {
        for (int i = selected.size() - 1; i >= 0; i--) {
            lru.touchIfExist(selected.get(i).key);
        }
        return new ArrayList<>(selected);
    }

    /**
     * Removes lookup metadata for ranges reported missing by a bulk disk read.
     * Each range carries its index key. Removes it only if the same entry is still indexed,
     * preserving newer replacements and ignoring ranges from another index.
     *
     * @param missingRanges original lookup ranges reported missing by the disk cache
     */
    synchronized void remove(List<FileCache.ReadRange> missingRanges) {
        for (FileCache.ReadRange range : missingRanges) {
            if (range instanceof Entry entry && entries.get(entry.key) == entry) {
                entries.remove(entry.key);
                lru.remove(entry.key);
            }
        }
    }

    /**
     * Removes the stream's lookup metadata. Disk bytes remain subject to the disk cache's eviction.
     */
    synchronized void clearStream(long streamId) {
        Iterator<Key> keys = entries.subMap(new Key(streamId, Long.MIN_VALUE, Long.MIN_VALUE), true,
            new Key(streamId, Long.MAX_VALUE, Long.MAX_VALUE), true).keySet().iterator();
        while (keys.hasNext()) {
            lru.remove(keys.next());
            keys.remove();
        }
    }

    synchronized int size() {
        return entries.size();
    }

    private record Key(long streamId, long baseOffset, long endOffset) implements Comparable<Key> {
        @Override
        public int compareTo(Key other) {
            int result = Long.compare(streamId, other.streamId);
            if (result == 0) {
                result = Long.compare(baseOffset, other.baseOffset);
            }
            return result == 0 ? Long.compare(endOffset, other.endOffset) : result;
        }
    }

    private record Entry(Key key, int payloadBytes, long cacheId, long position, int length) implements FileCache.ReadRange {
    }
}
