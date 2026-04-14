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

import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.controller.stream.KVKey;
import org.apache.kafka.timeline.TimelineHashMap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class KVDelta {
    private final KVImage image;

    private final Map<KVKey, ByteBuffer> changedKV = new HashMap<>();
    private final Set<KVKey> removedKeys = new HashSet<>();

    public KVDelta(KVImage image) {
        this.image = image;
    }

    public KVImage image() {
        return image;
    }

    public void replay(KVRecord record) {
        record.keyValues().forEach(keyValue -> {
            KVKey kvKey = KVKey.of(keyValue.namespace(), keyValue.key());
            changedKV.put(kvKey, ByteBuffer.wrap(keyValue.value()));
            removedKeys.remove(kvKey);
        });
    }

    public void replay(RemoveKVRecord record) {
        List<String> keys = record.keys();
        List<String> namespaces = record.namespaces();
        if (namespaces != null && namespaces.size() != keys.size()) {
            throw new IllegalArgumentException("RemoveKVRecord: namespaces length " + namespaces.size()
                + " does not match keys length " + keys.size());
        }
        for (int i = 0; i < keys.size(); i++) {
            String ns = namespaces != null ? namespaces.get(i) : null;
            KVKey kvKey = KVKey.of(ns, keys.get(i));
            removedKeys.add(kvKey);
            changedKV.remove(kvKey);
        }
    }

    public KVImage apply() {
        RegistryRef registry = image.registryRef();
        // get original objects first
        TimelineHashMap<KVKey, ByteBuffer> newKVs;

        if (registry == RegistryRef.NOOP) {
            registry = new RegistryRef();
            newKVs = new TimelineHashMap<>(registry.registry(), 100000);
        } else {
            newKVs = image.timelineKVs();
        }

        RegistryRef finalRegistry = registry;
        finalRegistry.inLock(() -> {
            newKVs.putAll(changedKV);
            removedKeys.forEach(newKVs::remove);
        });

        registry = registry.next();

        return new KVImage(newKVs, registry);
    }

    public Map<KVKey, ByteBuffer> changedKV() {
        return changedKV;
    }
}
