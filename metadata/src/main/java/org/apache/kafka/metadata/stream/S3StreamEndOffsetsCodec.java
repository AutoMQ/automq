/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package org.apache.kafka.metadata.stream;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class S3StreamEndOffsetsCodec {
    public static final byte MAGIC = 0x01;
    private static final int UNIT_SIZE = 8 /* streamId */ + 8 /* endOffset */;

    public static byte[] encode(List<StreamEndOffset> offsets) {
        ByteBuf encoded = Unpooled.buffer(1 + UNIT_SIZE * offsets.size());
        encoded.writeByte(MAGIC);
        for (StreamEndOffset offset : offsets) {
            encoded.writeLong(offset.streamId());
            encoded.writeLong(offset.endOffset());
        }
        return encoded.array();
    }

    public static Iterable<StreamEndOffset> decode(byte[] bytes) {
        return () -> {
            ByteBuf encoded = Unpooled.wrappedBuffer(bytes);
            byte magic = encoded.getByte(0);
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid magic byte: " + magic);
            }
            int size = encoded.readableBytes() / UNIT_SIZE;
            AtomicInteger index = new AtomicInteger(0);
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return index.get() < size;
                }

                @Override
                public StreamEndOffset next() {
                    int base = 1 /* magic */ + index.get() * UNIT_SIZE;
                    StreamEndOffset offset = new StreamEndOffset(encoded.getLong(base), encoded.getLong(base + 8));
                    index.incrementAndGet();
                    return offset;
                }
            };
        };
    }

}
