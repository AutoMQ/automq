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
