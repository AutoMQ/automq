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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

public interface SortedWALObjects {

    int size();

    boolean isEmpty();

    Iterator<S3WALObject> iterator();

    List<S3WALObject> list();

    boolean contains(Object o);

    boolean add(S3WALObject s3WALObject);

    default boolean addAll(Collection<S3WALObject> walObjects) {
        walObjects.forEach(this::add);
        return true;
    }

    boolean remove(Object o);

    default boolean removeIf(Predicate<S3WALObject> filter) {
        return this.list().removeIf(filter);
    }

    S3WALObject get(int index);

    void clear();

}
