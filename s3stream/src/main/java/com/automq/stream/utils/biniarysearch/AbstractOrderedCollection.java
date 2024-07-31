/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
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
