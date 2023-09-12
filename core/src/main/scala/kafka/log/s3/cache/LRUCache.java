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

package kafka.log.s3.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LRUCache<K, V> {
    private final LinkedHashMap<K, V> cache;
    private final Set<Map.Entry<K, V>> cacheEntrySet;

    public LRUCache() {
        cache = new LinkedHashMap<>(16, .75f, true);
        cacheEntrySet = cache.entrySet();
    }

    public boolean touch(K key) {
        return cache.get(key) != null;
    }

    public void put(K key, V value) {
        if (cache.put(key, value) != null) {
            touch(key);
        }
    }

    public V get(K key) {
        return cache.get(key);
    }

    public Map.Entry<K, V> pop() {
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

    public boolean remove(K key) {
        return cache.remove(key) != null;
    }

    public int size() {
        return cache.size();
    }
}
