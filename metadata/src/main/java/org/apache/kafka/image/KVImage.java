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


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.KVRecord.KeyValue;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.TimelineHashMap;

public final class KVImage extends AbstractReferenceCounted {
    public static final KVImage EMPTY = new KVImage(new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), RegistryRef.NOOP);

    private final RegistryRef registryRef;

    private final TimelineHashMap<String, ByteBuffer> kv;

    public KVImage(TimelineHashMap<String, ByteBuffer> kv, RegistryRef registryRef) {
        this.registryRef = registryRef;
        this.kv = kv;
    }

    public ByteBuffer getValue(String key) {
        if (kv == null || registryRef == RegistryRef.NOOP) {
            return null;
        }

        return registryRef.inLock(() -> this.kv.get(key, registryRef.epoch()));
    }

    public Map<String, ByteBuffer> kvs() {
        if (kv == null || registryRef == RegistryRef.NOOP) {
            return Collections.emptyMap();
        }

        return registryRef.inLock(() -> {
            Map<String, ByteBuffer> result = new HashMap<>();
            kv.entrySet(registryRef().epoch()).forEach(e -> result.put(e.getKey(), e.getValue()));
            return result;
        });
    }

    public TimelineHashMap<String, ByteBuffer> timelineKVs() {
        return kv;
    }

    RegistryRef registryRef() {
        return registryRef;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (kv == null || registryRef == RegistryRef.NOOP) {
            return;
        }

        List<List<KeyValue>> keyValues = registryRef.inLock(() -> kv.entrySet(registryRef.epoch())
            .stream()
            .map(e -> List.of(new KeyValue().setKey(e.getKey()).setValue(e.getValue().array())))
            .collect(Collectors.toList()));

        keyValues.forEach(kvs -> {
            writer.write(new ApiMessageAndVersion(new KVRecord()
                .setKeyValues(kvs), (short) 0));
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
