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

package org.apache.kafka.image;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class DeltaMap<K, V> {
    private static final double MERGE_DELETE_THRESHOLD = 0.1;
    private final int[] deltaThresholds;
    final List<Map<K, V>> deltas;
    Set<K> removed;
    boolean newRemoved;

    public DeltaMap() {
        this(new int[]{});
    }

    public DeltaMap(int[] deltaThresholds) {
        this.deltaThresholds = deltaThresholds;
        deltas = new ArrayList<>(deltaThresholds.length + 1);
        for (int threshold : deltaThresholds) {
            deltas.add(new HashMap<>(threshold));
        }
        deltas.add(new HashMap<>());
        removed = new HashSet<>();
    }

    public DeltaMap(int[] deltaThresholds, List<Map<K, V>> deltas, Set<K> removed) {
        this.deltaThresholds = deltaThresholds;
        this.deltas = deltas;
        this.removed = removed;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> DeltaMap<K, V> of(Object... kvList) {
        if (kvList.length % 2 != 0) {
            throw new IllegalArgumentException("kvList must be even length");
        }
        DeltaMap<K, V> map = new DeltaMap<>(new int[]{});
        for (int i = 0; i < kvList.length; i += 2) {
            map.put((K) kvList[i], (V) kvList[i + 1]);
        }
        return map;
    }

    public void put(K key, V value) {
        Map<K, V> delta0 = deltas.get(0);
        delta0.put(key, value);
        if (!removed.isEmpty()) {
            unmarkRemoved(key);
        }
    }

    public void putAll(Map<K, V> addDelta) {
        Map<K, V> delta0 = deltas.get(0);
        delta0.putAll(addDelta);
        if (!removed.isEmpty()) {
            addDelta.forEach((k, v) -> unmarkRemoved(k));
        }
    }

    public boolean containsKey(K key) {
        if (removed.contains(key)) {
            return false;
        }
        for (Map<K, V> delta : deltas) {
            if (delta.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    public V get(K key) {
        if (removed.contains(key)) {
            return null;
        }
        for (Map<K, V> delta : deltas) {
            V value = delta.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    public V getOrDefault(K key, V defaultValue) {
        V value = get(key);
        return value == null ? defaultValue : value;
    }

    public void remove(K key) {
        getRemovedForModify().add(key);
    }

    public void removeAll(Collection<K> newDeleted) {
        getRemovedForModify().addAll(newDeleted);
    }

    private Set<K> getRemovedForModify() {
        if (!newRemoved) {
            int mapSize = deltas.stream().mapToInt(Map::size).sum();
            if (mapSize > 0 && 1.0 * removed.size() / mapSize >= MERGE_DELETE_THRESHOLD) {
                compact();
            }
            this.removed = new HashSet<>(removed);
            newRemoved = true;
        }
        return removed;
    }

    private void unmarkRemoved(K key) {
        if (removed.contains(key)) {
            getRemovedForModify().remove(key);
        }
    }

    public boolean isEmpty() {
        for (Map<K, V> delta : deltas) {
            int deltaSize = delta.size();
            for (K deletedKey : removed) {
                if (delta.containsKey(deletedKey)) {
                    deltaSize--;
                } else {
                    return false;
                }
            }
            if (deltaSize > 0) {
                return false;
            }
        }
        return true;
    }

    public void forEach(BiConsumer<K, V> consumer) {
        Set<K> done = new HashSet<>(removed);
        for (int i = 0; i < deltas.size(); i++) {
            boolean lastDelta = i == deltas.size() - 1;
            deltas.get(i).forEach((k, v) -> {
                if (!done.contains(k)) {
                    consumer.accept(k, v);
                    if (!lastDelta) {
                        done.add(k);
                    }
                }
            });
        }
    }

    public DeltaMap<K, V> copy() {
        int[] deltaThresholds = this.deltaThresholds;
        List<Map<K, V>> deltas = new ArrayList<>(this.deltas);
        deltas.set(0, new HashMap<>(deltas.get(0)));
        // compact delta if exceeds threshold
        for (int i = 0; i < deltaThresholds.length; i++) {
            if (deltas.get(i).size() > deltaThresholds[i]) {
                // compact current delta to the next level
                Map<K, V> next = new HashMap<>(deltas.get(i + 1));
                next.putAll(deltas.get(i));
                deltas.set(i, new HashMap<>(deltaThresholds[i]));
                deltas.set(i + 1, next);
            }
        }
        return new DeltaMap<>(deltaThresholds, deltas, this.removed);
    }

    private Map<K, V> compact() {
        Map<K, V> all = new HashMap<>(deltas.get(deltas.size() - 1).size());
        for (int i = deltas.size() - 1; i >= 0; i--) {
            all.putAll(deltas.get(i));
        }
        removed.forEach(all::remove);
        for (int i = 0; i < deltas.size() - 1; i++) {
            deltas.set(i, new HashMap<>(deltaThresholds[i]));
        }
        deltas.set(deltas.size() - 1, all);
        removed = new HashSet<>();
        return all;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        @SuppressWarnings("unchecked") DeltaMap<K, V> other = (DeltaMap<K, V>) o;
        Map<K, V> deltaMap = compact();
        Map<K, V> otherMap = other.compact();
        return deltaMap.equals(otherMap);
    }

    @Override
    public int hashCode() {
        return compact().hashCode();
    }
}
