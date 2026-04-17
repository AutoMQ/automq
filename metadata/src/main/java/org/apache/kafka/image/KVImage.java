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

    private final TimelineHashMap<KVKey, ByteBuffer> kv;

    public KVImage(TimelineHashMap<KVKey, ByteBuffer> kv, RegistryRef registryRef) {
        this.registryRef = registryRef;
        this.kv = kv;
    }

    public ByteBuffer getValue(KVKey kvKey) {
        if (kv == null || registryRef == RegistryRef.NOOP) {
            return null;
        }
        return registryRef.inLock(() -> this.kv.get(kvKey, registryRef.epoch()));
    }

    public void forEach(BiConsumer<KVKey, ByteBuffer> action) {
        if (kv == null || registryRef == RegistryRef.NOOP) {
            return;
        }
        registryRef.inLock(() -> {
            kv.entrySet(registryRef.epoch()).forEach(e -> action.accept(e.getKey(), e.getValue()));
        });
    }

    Map<KVKey, ByteBuffer> kvs() {
        if (kv == null || registryRef == RegistryRef.NOOP) {
            return Collections.emptyMap();
        }
        return registryRef.inLock(() -> {
            Map<KVKey, ByteBuffer> result = new HashMap<>();
            kv.entrySet(registryRef().epoch()).forEach(e -> result.put(e.getKey(), e.getValue()));
            return result;
        });
    }

    public TimelineHashMap<KVKey, ByteBuffer> timelineKVs() {
        return kv;
    }

    RegistryRef registryRef() {
        return registryRef;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (kv == null || registryRef == RegistryRef.NOOP) {
            return;
        }

        AutoMQVersion autoMQVersion = options.metadataVersion().autoMQVersion();
        short kvVersion = autoMQVersion.kvRecordVersion();

        List<Map.Entry<KVKey, ByteBuffer>> entries = registryRef.inLock(() ->
            new ArrayList<>(kv.entrySet(registryRef.epoch())));

        entries.forEach(e -> {
            KVKey kvKey = e.getKey();
            String namespace = kvVersion >= 1 ? kvKey.namespace() : null;
            writer.write(new ApiMessageAndVersion(new KVRecord()
                .setKeyValues(Collections.singletonList(
                    new KeyValue().setKey(kvKey.key()).setValue(e.getValue().array()).setNamespace(namespace))),
                kvVersion));
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
        return kvs().equals(kvImage.kvs());
    }

    public boolean isEmpty() {
        return kv.isEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(kv);
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
