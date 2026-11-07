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

package org.apache.kafka.image;

import com.automq.stream.s3.cache.LRUCache;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import javax.annotation.concurrent.ThreadSafe;

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

    @VisibleForTesting
    void clear() {
        while (streamOffsetCache.size() > 0) {
            streamOffsetCache.pop();
        }
        streamOffsetIndexMap.clear();
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
