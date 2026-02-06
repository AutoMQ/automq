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

package org.apache.kafka.metadata.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A sorted list implementation for S3StreamSetObject that maintains elements in natural order.
 * Uses ArrayList for efficient random access and binary search for O(log n) insertion and removal.
 *
 * Note: Elements are sorted by orderId (via compareTo), but equality is determined by objectId.
 * This means multiple elements with the same orderId but different objectIds can exist.
 *
 * Thread-safety: This class is not thread-safe. External synchronization is required for
 * concurrent access.
 */
public class SortedStreamSetObjectsList implements SortedStreamSetObjects {

    private final List<S3StreamSetObject> list;

    /**
     * Constructs a SortedStreamSetObjectsList by copying from another SortedStreamSetObjects.
     *
     * @param source the source collection to copy from
     * @throws NullPointerException if source is null
     */
    public SortedStreamSetObjectsList(SortedStreamSetObjects source) {
        Objects.requireNonNull(source, "source must not be null");
        this.list = new ArrayList<>(source.list());
    }

    /**
     * Constructs an empty SortedStreamSetObjectsList.
     */
    public SortedStreamSetObjectsList() {
        this.list = new ArrayList<>();
    }

    /**
     * Constructs a SortedStreamSetObjectsList from a list of S3StreamSetObjects.
     *
     * @param list the list of S3StreamSetObjects, must guarantee that the list is sorted
     * @throws NullPointerException if list is null
     */
    public SortedStreamSetObjectsList(List<S3StreamSetObject> list) {
        this.list = new ArrayList<>(Objects.requireNonNull(list, "list must not be null"));
    }

    @Override
    public int size() {
        return this.list.size();
    }

    @Override
    public boolean isEmpty() {
        return this.list.isEmpty();
    }

    @Override
    public Iterator<S3StreamSetObject> iterator() {
        return this.list.iterator();
    }

    @Override
    public List<S3StreamSetObject> list() {
        return list;
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof S3StreamSetObject)) {
            return false;
        }
        return findExactMatch((S3StreamSetObject) o) >= 0;
    }

    /**
     * Adds a new S3StreamSetObject to the list while maintaining sorted order.
     * Uses binary search to find the optimal insertion position in O(log n) time.
     *
     * @param s3StreamSetObject the object to add, must not be null
     * @return true if the object was successfully added
     * @throws NullPointerException if s3StreamSetObject is null
     */
    @Override
    public boolean add(S3StreamSetObject s3StreamSetObject) {
        Objects.requireNonNull(s3StreamSetObject, "s3StreamSetObject must not be null");

        int insertionIndex = findInsertionIndex(s3StreamSetObject);
        this.list.add(insertionIndex, s3StreamSetObject);
        return true;
    }

    /**
     * Removes the first occurrence of the specified element from this list using binary search.
     * Uses binary search to locate the element in O(log n) time, then performs exact match
     * using equals() since compareTo (orderId) and equals (objectId) use different fields.
     *
     * @param o element to be removed from this list, if present
     * @return true if this list contained the specified element
     */
    @Override
    public boolean remove(Object o) {
        if (!(o instanceof S3StreamSetObject)) {
            return false;
        }

        S3StreamSetObject target = (S3StreamSetObject) o;
        int index = findExactMatch(target);

        if (index >= 0) {
            this.list.remove(index);
            return true;
        }

        return false;
    }

    @Override
    public S3StreamSetObject get(int index) {
        return this.list.get(index);
    }

    @Override
    public void clear() {
        this.list.clear();
    }

    /**
     * Finds the exact match of the target object using binary search followed by linear scan.
     * Since compareTo uses orderId but equals uses objectId, we need to:
     * 1. Use binary search to find any element with matching orderId (O(log n))
     * 2. Scan nearby elements with the same orderId to find exact match by objectId (O(k) where k is duplicates)
     *
     * @param target the object to search for
     * @return the index of the exact match, or -1 if not found
     */
    private int findExactMatch(S3StreamSetObject target) {
        int index = Collections.binarySearch(this.list, target);

        if (index < 0) {
            // Element with matching orderId not found
            return -1;
        }

        // Binary search found an element with matching orderId (compareTo == 0)
        // Now we need to find the exact match using equals (matching objectId)

        // Check the found element first
        if (this.list.get(index).equals(target)) {
            return index;
        }

        // Search forward through elements with the same orderId
        int size = this.list.size();
        for (int i = index + 1; i < size; i++) {
            S3StreamSetObject element = this.list.get(i);
            if (target.compareTo(element) != 0) {
                // Moved past elements with the same orderId
                break;
            }
            if (element.equals(target)) {
                return i;
            }
        }

        // Search backward through elements with the same orderId
        for (int i = index - 1; i >= 0; i--) {
            S3StreamSetObject element = this.list.get(i);
            if (target.compareTo(element) != 0) {
                // Moved past elements with the same orderId
                break;
            }
            if (element.equals(target)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds the correct insertion index for the given object using binary search.
     * The object will be inserted at the leftmost position where it belongs according
     * to its natural ordering (orderId).
     *
     * @param target the object to find insertion position for
     * @return the index where the object should be inserted
     */
    private int findInsertionIndex(S3StreamSetObject target) {
        int index = Collections.binarySearch(this.list, target);

        if (index < 0) {
            // Element not found, binarySearch returns (-(insertion point) - 1)
            return -(index + 1);
        }

        // Found an element with the same orderId (compareTo == 0)
        // Insert at the leftmost position among duplicates to maintain stable ordering
        while (index > 0 && target.compareTo(this.list.get(index - 1)) == 0) {
            index--;
        }

        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SortedStreamSetObjectsList that = (SortedStreamSetObjectsList) o;
        return Objects.equals(list, that.list);
    }

    @Override
    public int hashCode() {
        return Objects.hash(list);
    }

    @Override
    public String toString() {
        return "SortedStreamSetObjectsList{" +
            "list=" + list.stream().map(S3StreamSetObject::toString).collect(Collectors.joining(",")) +
            '}';
    }
}
