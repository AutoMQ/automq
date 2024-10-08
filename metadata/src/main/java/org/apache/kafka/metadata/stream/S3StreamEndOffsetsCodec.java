/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
