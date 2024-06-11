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
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.KVRecord.KeyValue;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public final class KVImage {

    public static final KVImage EMPTY = new KVImage(new DeltaMap<>(new int[] {1000, 10000}));

    private final DeltaMap<String, ByteBuffer> kv;

    public KVImage(final DeltaMap<String, ByteBuffer> kv) {
        this.kv = kv;
    }

    public DeltaMap<String, ByteBuffer> kv() {
        return kv;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        kv.forEach((k, v) -> {
            List<KeyValue> kvs = List.of(new KeyValue().setKey(k).setValue(v.array()));
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
        return kv.equals(kvImage.kv);
    }

    public boolean isEmpty() {
        return kv.isEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(kv);
    }
}
