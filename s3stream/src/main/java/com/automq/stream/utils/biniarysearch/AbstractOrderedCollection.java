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
