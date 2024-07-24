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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LRUCache<K, V> {
    protected final LinkedHashMap<K, V> cache;
    protected final Set<Map.Entry<K, V>> cacheEntrySet;

    public LRUCache() {
        cache = new LinkedHashMap<>(16, .75f, true);
        cacheEntrySet = cache.entrySet();
    }

    public synchronized boolean touchIfExist(K key) {
        return cache.get(key) != null;
    }

    public synchronized void put(K key, V value) {
        if (cache.put(key, value) != null) {
            touchIfExist(key);
        }
    }

    public synchronized V get(K key) {
        return cache.get(key);
    }

    public synchronized Map.Entry<K, V> pop() {
        Iterator<Map.Entry<K, V>> it = cacheEntrySet.iterator();
        if (!it.hasNext()) {
            return null;
        }
        Map.Entry<K, V> entry = it.next();
        if (entry == null) {
            return null;
        }
        it.remove();
        return entry;
    }

    public synchronized Map.Entry<K, V> peek() {
        Iterator<Map.Entry<K, V>> it = cacheEntrySet.iterator();
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    public synchronized boolean remove(K key) {
        return cache.remove(key) != null;
    }

    public synchronized int size() {
        return cache.size();
    }

    public synchronized boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    public synchronized void clear() {
        cache.clear();
    }
}
