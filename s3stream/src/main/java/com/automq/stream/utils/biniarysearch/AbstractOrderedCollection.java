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

package com.automq.stream.utils.biniarysearch;

import java.util.Collections;
import java.util.List;

public abstract class AbstractOrderedCollection<T> {

    protected abstract int size();

    protected abstract ComparableItem<T> get(int index);

    /**
     * @see Collections#binarySearch(List, Object)
     */
    public int search(T target) {
        int low = 0;
        int high = size() - 1;
        while (low <= high) {
            int mid = low + ((high - low) >>> 1);
            ComparableItem<T> midVal = get(mid);
            if (midVal.isLessThan(target)) {
                low = mid + 1;
            } else if (midVal.isGreaterThan(target)) {
                high = mid - 1;
            } else {
                low = mid;
                break;
            }
        }
        if (low > high) {
            return -(low + 1);
        }
        return low;
    }
}
