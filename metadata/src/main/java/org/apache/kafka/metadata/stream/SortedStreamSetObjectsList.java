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

public class SortedStreamSetObjectsList implements SortedStreamSetObjects {

    private final List<S3StreamSetObject> list;

    public SortedStreamSetObjectsList(SortedStreamSetObjects source) {
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
        // Use ArrayList for efficient random access required by binary search
        this.list = list instanceof ArrayList ? list : new ArrayList<>(list);
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
        return this.list.contains(o);
    }

    @Override
    public boolean add(S3StreamSetObject s3StreamSetObject) {
        // Use binary search to find the insertion point in O(log n) time
        int index = Collections.binarySearch(this.list, s3StreamSetObject);
        if (index < 0) {
            // binarySearch returns (-(insertion point) - 1) when element is not found
            index = -(index + 1);
        }
        this.list.add(index, s3StreamSetObject);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (!(o instanceof S3StreamSetObject)) {
            return false;
        }
        S3StreamSetObject target = (S3StreamSetObject) o;
        // Use binary search to find the element in O(log n) time
        int index = Collections.binarySearch(this.list, target);
        if (index >= 0) {
            // Found the element at the exact position, but we need to verify it's the same object
            // since compareTo compares by orderId, but equals compares by objectId
            S3StreamSetObject found = this.list.get(index);
            if (found.equals(target)) {
                this.list.remove(index);
                return true;
            }
            // If not equal, search linearly around the found position for objects with the same orderId
            // This handles the case where multiple objects have the same orderId
            for (int i = index - 1; i >= 0 && this.list.get(i).compareTo(target) == 0; i--) {
                if (this.list.get(i).equals(target)) {
                    this.list.remove(i);
                    return true;
                }
            }
            for (int i = index + 1; i < this.list.size() && this.list.get(i).compareTo(target) == 0; i++) {
                if (this.list.get(i).equals(target)) {
                    this.list.remove(i);
                    return true;
                }
            }
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
