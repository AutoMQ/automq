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
import org.apache.kafka.common.metadata.KVRecord.KeyValue;
import org.apache.kafka.controller.stream.KVKey;
import org.apache.kafka.controller.stream.KVNamespace;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.TimelineHashMap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public final class KVImage extends AbstractReferenceCounted {
    public static final KVImage EMPTY = new KVImage(new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), RegistryRef.NOOP);

    private final RegistryRef registryRef;

    private final TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> kvByNamespace;

    public KVImage(TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> kvByNamespace,
                   RegistryRef registryRef) {
        this.registryRef = registryRef;
        this.kvByNamespace = kvByNamespace;
    }

    public ByteBuffer getValue(KVKey kvKey) {
        return getValue(kvKey.namespace(), kvKey.key());
    }

    public ByteBuffer getValue(String namespace, String key) {
        if (kvByNamespace == null || registryRef == RegistryRef.NOOP) {
            return null;
        }
        return registryRef.inLock(() -> {
            TimelineHashMap<String, ByteBuffer> namespaceKVs = this.kvByNamespace.get(KVNamespace.of(namespace),
                registryRef.epoch());
            return namespaceKVs == null ? null : namespaceKVs.get(key, registryRef.epoch());
        });
    }

    public Map<String, ByteBuffer> namespaceKVs(String namespace) {
        if (kvByNamespace == null || registryRef == RegistryRef.NOOP) {
            return Collections.emptyMap();
        }
        return registryRef.inLock(() -> {
            TimelineHashMap<String, ByteBuffer> namespaceKVs = kvByNamespace.get(KVNamespace.of(namespace),
                registryRef.epoch());
            if (namespaceKVs == null) {
                return Collections.emptyMap();
            }
            Map<String, ByteBuffer> result = new HashMap<>();
            namespaceKVs.entrySet(registryRef.epoch()).forEach(e -> result.put(e.getKey(), e.getValue()));
            return result;
        });
    }

    public List<String> namespaces() {
        if (kvByNamespace == null || registryRef == RegistryRef.NOOP) {
            return Collections.emptyList();
        }
        return registryRef.inLock(() -> {
            List<String> result = new ArrayList<>();
            kvByNamespace.keySet(registryRef.epoch()).forEach(namespace -> result.add(namespace.namespace()));
            return result;
        });
    }

    Map<KVNamespace, Map<String, ByteBuffer>> kvsByNamespace() {
        if (kvByNamespace == null || registryRef == RegistryRef.NOOP) {
            return Collections.emptyMap();
        }
        return registryRef.inLock(() -> {
            Map<KVNamespace, Map<String, ByteBuffer>> result = new HashMap<>();
            kvByNamespace.entrySet(registryRef().epoch()).forEach(namespaceEntry -> {
                Map<String, ByteBuffer> namespaceKVs = new HashMap<>();
                namespaceEntry.getValue().entrySet(registryRef().epoch())
                    .forEach(e -> namespaceKVs.put(e.getKey(), e.getValue()));
                result.put(namespaceEntry.getKey(), namespaceKVs);
            });
            return result;
        });
    }

    void forEachEntry(BiConsumer<KVKey, ByteBuffer> action) {
        if (kvByNamespace == null || registryRef == RegistryRef.NOOP) {
            return;
        }
        registryRef.inLock(() -> {
            kvByNamespace.entrySet(registryRef().epoch()).forEach(namespaceEntry -> {
                String namespace = namespaceEntry.getKey().namespace();
                namespaceEntry.getValue().entrySet(registryRef().epoch())
                    .forEach(e -> action.accept(KVKey.of(namespace, e.getKey()), e.getValue()));
            });
        });
    }

    TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> timelineKVsByNamespace() {
        return kvByNamespace;
    }

    RegistryRef registryRef() {
        return registryRef;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (kvByNamespace == null || registryRef == RegistryRef.NOOP) {
            return;
        }

        AutoMQVersion autoMQVersion = options.metadataVersion().autoMQVersion();
        short kvVersion = autoMQVersion.kvRecordVersion();

        Map<KVNamespace, Map<String, ByteBuffer>> entries = kvsByNamespace();
        entries.forEach((kvNamespace, namespaceKVs) -> {
            String namespace = kvVersion >= 1 ? kvNamespace.namespace() : null;
            namespaceKVs.forEach((key, value) ->
                writer.write(new ApiMessageAndVersion(new KVRecord()
                    .setKeyValues(Collections.singletonList(
                        new KeyValue().setKey(key).setValue(value.array()).setNamespace(namespace))),
                    kvVersion)));
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KVImage kvImage = (KVImage) o;
        return kvsByNamespace().equals(kvImage.kvsByNamespace());
    }

    public boolean isEmpty() {
        return kvByNamespace.isEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(kvsByNamespace());
    }

    @Override
    protected void deallocate() {
        if (registryRef == RegistryRef.NOOP) {
            return;
        }
        registryRef.release();
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }
}
