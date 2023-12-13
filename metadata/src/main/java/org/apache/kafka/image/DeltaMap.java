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
    Set<K> deleted;
    boolean newDeleted;

    public DeltaMap(int[] deltaThresholds) {
        this.deltaThresholds = deltaThresholds;
        deltas = new ArrayList<>(deltaThresholds.length + 1);
        for (int threshold : deltaThresholds) {
            deltas.add(new HashMap<>(threshold));
        }
        deltas.add(new HashMap<>());
        deleted = new HashSet<>();
    }

    public DeltaMap(int[] deltaThresholds, List<Map<K, V>> deltas, Set<K> deleted) {
        this.deltaThresholds = deltaThresholds;
        this.deltas = deltas;
        this.deleted = deleted;
    }

    public void putAll(Map<K, V> addDelta) {
        Map<K, V> delta0 = deltas.get(0);
        delta0.putAll(addDelta);
        if (!deleted.isEmpty()) {
            Set<K> deleted = getDeletedForModify();
            addDelta.forEach((k, v) -> deleted.remove(k));
        }
    }

    public V get(K key) {
        if (deleted.contains(key)) {
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

    public void deleteAll(Collection<K> newDeleted) {
        getDeletedForModify().addAll(newDeleted);
    }

    private Set<K> getDeletedForModify() {
        if (!newDeleted) {
            int mapSize = deltas.stream().mapToInt(Map::size).sum();
            if (mapSize > 0 && 1.0 * deleted.size() / mapSize >= MERGE_DELETE_THRESHOLD) {
                compact();
            }
            this.deleted = new HashSet<>(deleted);
            newDeleted = true;
        }
        return deleted;
    }

    public boolean isEmpty() {
        for (Map<K, V> delta : deltas) {
            int deltaSize = delta.size();
            for (K deletedKey : deleted) {
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
        Set<K> done = new HashSet<>(deleted);
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
        return new DeltaMap<>(deltaThresholds, deltas, this.deleted);
    }

    private Map<K, V> compact() {
        Map<K, V> all = new HashMap<>(deltas.get(deltas.size() - 1).size());
        for (int i = deltas.size() - 1; i >= 0; i--) {
            all.putAll(deltas.get(i));
        }
        deleted.forEach(all::remove);
        for (int i = 0; i < deltas.size() - 1; i++) {
            deltas.set(i, new HashMap<>(deltaThresholds[i]));
        }
        deltas.set(deltas.size() - 1, all);
        deleted = new HashSet<>();
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
        return super.hashCode();
    }
}
