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

package com.automq.stream.s3.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class SparseRangeIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparseRangeIndex.class);
    private final int compactNum;
    private final int sparsePadding;
    // sorted by startOffset in descending order
    private List<RangeIndex> sortedRangeIndexList;
    private long evictIndex = 0;

    public SparseRangeIndex(int compactNum, int sparsePadding) {
        this(compactNum, sparsePadding, new ArrayList<>());
    }

    public SparseRangeIndex(int compactNum, int sparsePadding, List<RangeIndex> sortedRangeIndexList) {
        this.compactNum = compactNum;
        this.sparsePadding = sparsePadding;
        this.sortedRangeIndexList = sortedRangeIndexList;
    }

    public void append(RangeIndex newRangeIndex) {
        if (newRangeIndex == null) {
            return;
        }
        if (!this.sortedRangeIndexList.isEmpty()
            && newRangeIndex.compareTo(this.sortedRangeIndexList.get(this.sortedRangeIndexList.size() - 1)) <= 0) {
            LOGGER.error("Unexpected new range index {}, last: {}, maybe initialized with outdated index file, " +
                    "reset local cache", newRangeIndex, this.sortedRangeIndexList.get(this.sortedRangeIndexList.size() - 1));
            reset();
        }
        this.sortedRangeIndexList.add(newRangeIndex);
        evict();
    }

    public void reset() {
        this.sortedRangeIndexList.clear();
        evictIndex = 0;
    }

    public void compact(RangeIndex newRangeIndex, Set<Long> compactedObjectIds) {
        if (compactedObjectIds.isEmpty()) {
            append(newRangeIndex);
            return;
        }
        List<RangeIndex> newRangeIndexList = new ArrayList<>();
        boolean found = false;
        for (RangeIndex rangeIndex : this.sortedRangeIndexList) {
            if (compactedObjectIds.contains(rangeIndex.getObjectId())) {
                continue;
            }
            if (newRangeIndex != null && !found && rangeIndex.compareTo(newRangeIndex) > 0) {
                newRangeIndexList.add(newRangeIndex);
                found = true;
            }
            newRangeIndexList.add(rangeIndex);
        }
        if (newRangeIndex != null && !found) {
            newRangeIndexList.add(newRangeIndex);
        }
        this.sortedRangeIndexList = newRangeIndexList;
    }

    private void evict() {
        if (this.sortedRangeIndexList.size() > this.compactNum) {
            if (evictIndex++ % (sparsePadding + 1) == 0) {
                this.sortedRangeIndexList.remove(this.sortedRangeIndexList.size() - this.compactNum - 1);
            }
        }
    }

    public int size() {
        return this.sortedRangeIndexList.size();
    }

    List<RangeIndex> getRangeIndexList() {
        return this.sortedRangeIndexList;
    }

}
