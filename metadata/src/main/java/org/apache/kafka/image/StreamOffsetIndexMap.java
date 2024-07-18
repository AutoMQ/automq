/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.image;

import com.automq.stream.s3.cache.LRUCache;
import com.google.common.annotations.VisibleForTesting;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

@ThreadSafe
public class StreamOffsetIndexMap {
    private final int maxSize;
    private final LRUCache<StreamOffset, Void> streamOffsetCache;
    private final Map<Long, NavigableMap<Long, Integer>> streamOffsetIndexMap;

    public StreamOffsetIndexMap(int maxSize) {
        this.maxSize = maxSize;
        this.streamOffsetCache = new LRUCache<>();
        this.streamOffsetIndexMap = new HashMap<>();
    }

    public synchronized void put(long streamId, long startOffset, int index) {
        if (streamOffsetIndexMap.containsKey(streamId) && streamOffsetIndexMap.get(streamId).containsKey(startOffset)) {
            return;
        }
        while (streamOffsetCache.size() >= maxSize) {
            Optional.ofNullable(streamOffsetCache.pop()).ifPresent(entry -> {
                StreamOffset streamOffset = entry.getKey();
                NavigableMap<Long, Integer> map = streamOffsetIndexMap.get(streamOffset.streamId);
                if (map != null) {
                    map.remove(streamOffset.startOffset);
                }
            });
        }
        streamOffsetCache.put(new StreamOffset(streamId, startOffset), null);
        streamOffsetIndexMap.compute(streamId, (k, v) -> {
            if (v == null) {
                v = new TreeMap<>();
            }
            v.put(startOffset, index);
            return v;
        });
    }

    public synchronized int floorIndex(long streamId, long offset) {
        NavigableMap<Long, Integer> map = streamOffsetIndexMap.get(streamId);
        if (map == null) {
            return 0;
        }
        Map.Entry<Long, Integer> entry = map.floorEntry(offset);
        if (entry == null) {
            return 0;
        }
        return entry.getValue();
    }

    @VisibleForTesting
    int cacheSize() {
        return streamOffsetCache.size();
    }

    @VisibleForTesting
    int entrySize() {
        return streamOffsetIndexMap.values().stream().mapToInt(NavigableMap::size).sum();
    }

    private static class StreamOffset {
        long streamId;
        long startOffset;

        public StreamOffset(long streamId, long startOffset) {
            this.streamId = streamId;
            this.startOffset = startOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamOffset that = (StreamOffset) o;
            return streamId == that.streamId && startOffset == that.startOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, startOffset);
        }
    }
}
