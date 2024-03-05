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

public class SortedStreamSetObjectsList implements SortedStreamSetObjects {

    private final List<S3StreamSetObject> list;

    public SortedStreamSetObjectsList(SortedStreamSetObjects source) {
        this.list = new LinkedList<>(source.list());
    }

    public SortedStreamSetObjectsList() {
        this.list = new LinkedList<>();
    }

    /**
     * Construct a SortedStreamSetObjectsList from a list of S3StreamSetObjects.
     * @param list the list of S3StreamSetObjects, must guarantee that the list is sorted
     */
    public SortedStreamSetObjectsList(List<S3StreamSetObject> list) {
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
        // TODO: optimize by binary search
        for (int index = 0; index < this.list.size(); index++) {
            S3StreamSetObject current = this.list.get(index);
            if (s3StreamSetObject.compareTo(current) <= 0) {
                this.list.add(index, s3StreamSetObject);
                return true;
            }
        }
        this.list.add(s3StreamSetObject);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        // TODO: optimize by binary search
        return this.list.remove(o);
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
