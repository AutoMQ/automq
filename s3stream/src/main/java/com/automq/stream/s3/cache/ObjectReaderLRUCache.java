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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import java.util.Optional;

public class ObjectReaderLRUCache extends LRUCache<Long, ObjectReader> {

    private final int maxObjectSize;

    public ObjectReaderLRUCache(int maxObjectSize) {
        super();
        if (maxObjectSize <= 0) {
            throw new IllegalArgumentException("maxObjectSize must be positive");
        }
        this.maxObjectSize = maxObjectSize;
    }

    @Override
    public synchronized void put(Long key, ObjectReader value) {
        while (objectSize() > maxObjectSize) {
            Optional.ofNullable(pop()).ifPresent(entry -> entry.getValue().close());
        }
        super.put(key, value);
    }

    private int objectSize() {
        return cacheEntrySet.stream().filter(entry -> entry.getValue().basicObjectInfo().isDone())
            .mapToInt(entry -> {
                try {
                    return entry.getValue().basicObjectInfo().get().size();
                } catch (Exception e) {
                    return 0;
                }
            }).sum();
    }
}
