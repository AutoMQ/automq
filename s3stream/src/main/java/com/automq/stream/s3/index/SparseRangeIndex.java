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

package com.automq.stream.s3.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class SparseRangeIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparseRangeIndex.class);
    private final int compactNum;
    // sorted by startOffset in descending order
    private List<RangeIndex> sortedRangeIndexList;
    private int size = 0;
    private int evictIndex = 0;

    public SparseRangeIndex(int compactNum) {
        this(compactNum, new ArrayList<>());
    }

    public SparseRangeIndex(int compactNum, List<RangeIndex> sortedRangeIndexList) {
        this.compactNum = compactNum;
        init(sortedRangeIndexList);
    }

    private void init(List<RangeIndex> sortedRangeIndexList) {
        if (sortedRangeIndexList == null) {
            sortedRangeIndexList = new ArrayList<>();
        }
        this.sortedRangeIndexList = sortedRangeIndexList;
        this.size = sortedRangeIndexList.size() * RangeIndex.OBJECT_SIZE;
    }

    /**
     * Append new range index to the list.
     * @param newRangeIndex the range index to append
     * @return the change of size after appending
     */
    public int append(RangeIndex newRangeIndex) {
        int delta = 0;
        if (newRangeIndex == null) {
            return delta;
        }
        if (!this.sortedRangeIndexList.isEmpty()
            && newRangeIndex.compareTo(this.sortedRangeIndexList.get(this.sortedRangeIndexList.size() - 1)) <= 0) {
            LOGGER.error("Unexpected new range index {}, last: {}, maybe initialized with outdated index file, " +
                    "reset local cache", newRangeIndex, this.sortedRangeIndexList.get(this.sortedRangeIndexList.size() - 1));
            delta -= size;
            reset();
        }
        this.sortedRangeIndexList.add(newRangeIndex);
        size += RangeIndex.OBJECT_SIZE;
        return delta + RangeIndex.OBJECT_SIZE;
    }

    public void reset() {
        init(new ArrayList<>());
    }

    /**
     * Compact the list by removing the compacted object ids and add the new range index if not null.
     *
     * @param newRangeIndex the new range index to add
     * @param compactedObjectIds the object ids to compact
     * @return the change of size after compacting
     */
    public int compact(RangeIndex newRangeIndex, Set<Long> compactedObjectIds) {
        if (compactedObjectIds.isEmpty()) {
            return append(newRangeIndex);
        }
        List<RangeIndex> newRangeIndexList = new ArrayList<>();
        boolean found = false;
        for (RangeIndex rangeIndex : this.sortedRangeIndexList) {
            if (compactedObjectIds.contains(rangeIndex.getObjectId())) {
                continue;
            }
            if (newRangeIndex != null && !found && rangeIndex.compareTo(newRangeIndex) > 0) {
                // insert new range index into the list
                newRangeIndexList.add(newRangeIndex);
                found = true;
            }
            newRangeIndexList.add(rangeIndex);
        }
        if (newRangeIndex != null && !found) {
            // insert new range index into the end of the list
            newRangeIndexList.add(newRangeIndex);
        }
        int oldSize = size;
        init(newRangeIndexList);
        return size - oldSize;
    }

    /**
     * Try to evict one range index from the list, the eviction priority for each element is:
     * 1. any element that's not the first and last N compacted elements
     * 2. the last N compacted elements
     * 3. the first element
     * <p>
     * For example for a list of [0, 1, 2, 3, 4, 5], compact number is 2, the eviction result will be:
     * <ul>
     * <li><code>1rst: [0, 2, 3, 4, 5]</code></li>
     * <li><code>2nd:  [0, 2, 4, 5]</code></li>
     * <li><code>3rd:  [0, 4, 5]</code></li>
     * <li><code>4th:  [0, 5]</code></li>
     * <li><code>5th:  [0]</code></li>
     * <li><code>6th:  []</code></li>
     * </ul>
     *
     * @return evicted size
     */
    public int evictOnce() {
        int indexToEvict = -1;
        if (this.sortedRangeIndexList.isEmpty()) {
            return 0;
        } else if (this.sortedRangeIndexList.size() == 1) {
            // evict the only element
            indexToEvict = 0;
        } else if (this.sortedRangeIndexList.size() <= (1 + compactNum)) {
            indexToEvict = 1;
        }

        if (indexToEvict == -1) {
            if (evictIndex % this.sortedRangeIndexList.size() == 0
                || this.evictIndex >= this.sortedRangeIndexList.size() - compactNum) {
                this.evictIndex = 1;
            }
            indexToEvict = evictIndex++;
        }
        this.sortedRangeIndexList.remove(indexToEvict);
        size -= RangeIndex.OBJECT_SIZE;
        return RangeIndex.OBJECT_SIZE;
    }

    public int length() {
        return this.sortedRangeIndexList.size();
    }

    public int size() {
        return size;
    }

    List<RangeIndex> getRangeIndexList() {
        return this.sortedRangeIndexList;
    }

}
