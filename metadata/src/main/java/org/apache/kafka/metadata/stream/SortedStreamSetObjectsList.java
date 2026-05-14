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
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SortedStreamSetObjectsList implements SortedStreamSetObjects {

    // ArrayList instead of LinkedList for faster random access needed by binary search
    private final List<S3StreamSetObject> list;

    public SortedStreamSetObjectsList(SortedStreamSetObjects source) {
        Objects.requireNonNull(source, "source must not be null");
        this.list = new ArrayList<>(source.list());
    }

    public SortedStreamSetObjectsList() {
        this.list = new ArrayList<>();
    }

    /**
     * Construct a SortedStreamSetObjectsList from a list of S3StreamSetObjects.
     * @param list the list of S3StreamSetObjects, must guarantee that the list is sorted
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
        return Collections.unmodifiableList(list);
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof S3StreamSetObject)) {
            return false;
        }
        // Use binary search for faster lookup
        return findExactMatch((S3StreamSetObject) o) >= 0;
    }

    @Override
    public boolean add(S3StreamSetObject s3StreamSetObject) {
        Objects.requireNonNull(s3StreamSetObject, "s3StreamSetObject must not be null");

        // Use binary search to find where to insert (replaces linear scan)
        int insertionIndex = findInsertionIndex(s3StreamSetObject);
        this.list.add(insertionIndex, s3StreamSetObject);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (!(o instanceof S3StreamSetObject)) {
            return false;
        }

        S3StreamSetObject target = (S3StreamSetObject) o;

        // Use binary search to find the object (replaces linear scan)
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
    public boolean removeIf(Predicate<S3StreamSetObject> filter) {
        return this.list.removeIf(filter);
    }

    @Override
    public void clear() {
        this.list.clear();
    }

    /**
     * Finds the exact object in the list using binary search.
     * Note: compareTo() uses orderId, but equals() uses objectId.
     * So we binary search by orderId, then scan duplicates to find matching objectId.
     */
    private int findExactMatch(S3StreamSetObject target) {
        int index = Collections.binarySearch(this.list, target);

        if (index < 0) {
            // No element with matching orderId found
            return -1;
        }

        // Found an element with same orderId, check if it's the exact match
        if (this.list.get(index).equals(target)) {
            return index;
        }

        // Multiple objects can have the same orderId but different objectId
        // Search forward through duplicates
        int size = this.list.size();
        for (int i = index + 1; i < size; i++) {
            S3StreamSetObject element = this.list.get(i);
            if (target.compareTo(element) != 0) {
                break; // No more duplicates
            }
            if (element.equals(target)) {
                return i;
            }
        }

        // Search backward through duplicates
        for (int i = index - 1; i >= 0; i--) {
            S3StreamSetObject element = this.list.get(i);
            if (target.compareTo(element) != 0) {
                break; // No more duplicates
            }
            if (element.equals(target)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds where to insert a new object using binary search.
     * Inserts at the leftmost position among objects with the same orderId.
     * This keeps the list sorted and maintains stable insertion order.
     */
    private int findInsertionIndex(S3StreamSetObject target) {
        int index = Collections.binarySearch(this.list, target);

        if (index < 0) {
            // No matching orderId found, convert to insertion point
            // binarySearch returns (-(insertion point) - 1) when not found
            return -(index + 1);
        }

        // Found objects with same orderId, move to leftmost position
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
