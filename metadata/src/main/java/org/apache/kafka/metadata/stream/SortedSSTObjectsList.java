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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SortedSSTObjectsList implements SortedSSTObjects {

    private final List<S3SSTObject> list;

    public SortedSSTObjectsList(SortedSSTObjects source) {
        this.list = new LinkedList<>(source.list());
    }

    public SortedSSTObjectsList() {
        this.list = new LinkedList<>();
    }

    /**
     * Construct a SortedSSTObjectsList from a list of S3SSTObjects.
     * @param list the list of S3SSTObjects, must guarantee that the list is sorted
     */
    public SortedSSTObjectsList(List<S3SSTObject> list) {
        this.list = list;
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
    public Iterator<S3SSTObject> iterator() {
        return this.list.iterator();
    }

    @Override
    public List<S3SSTObject> list() {
        return list;
    }

    @Override
    public boolean contains(Object o) {
        return this.list.contains(o);
    }

    @Override
    public boolean add(S3SSTObject s3SSTObject) {
        // TODO: optimize by binary search
        for (int index = 0; index < this.list.size(); index++) {
            S3SSTObject current = this.list.get(index);
            if (s3SSTObject.compareTo(current) <= 0) {
                this.list.add(index, s3SSTObject);
                return true;
            }
        }
        this.list.add(s3SSTObject);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        // TODO: optimize by binary search
        return this.list.remove(o);
    }



    @Override
    public S3SSTObject get(int index) {
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
        SortedSSTObjectsList that = (SortedSSTObjectsList) o;
        return Objects.equals(list, that.list);
    }

    @Override
    public int hashCode() {
        return Objects.hash(list);
    }

    @Override
    public String toString() {
        return "SortedSSTObjectsList{" +
            "list=" + list.stream().map(S3SSTObject::toString).collect(Collectors.joining(",")) +
            '}';
    }
}
