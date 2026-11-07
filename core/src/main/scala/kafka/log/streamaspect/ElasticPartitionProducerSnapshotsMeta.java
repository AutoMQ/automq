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

package kafka.log.streamaspect;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * refers to all valid snapshot files
 */
public class ElasticPartitionProducerSnapshotsMeta {
    public static final byte MAGIC_CODE = 0x18;
    private final Map<Long, ByteBuffer> snapshots;

    public ElasticPartitionProducerSnapshotsMeta() {
        this(new HashMap<>());
    }

    public ElasticPartitionProducerSnapshotsMeta(Map<Long, ByteBuffer> snapshots) {
        this.snapshots = snapshots;
    }

    public Map<Long, ByteBuffer> getSnapshots() {
        return snapshots;
    }

    public boolean isEmpty() {
        return snapshots.isEmpty();
    }

    public ByteBuffer encode() {
        int size = 1 /* magic code */ + snapshots.size() * (8 /* offset */ + 4 /* length */);
        for (ByteBuffer snapshot : snapshots.values()) {
            if (snapshot != null) {
                size += snapshot.remaining();
            }
        }
        ByteBuf buf = Unpooled.buffer(size);
        buf.writeByte(MAGIC_CODE);
        snapshots.forEach((offset, snapshot) -> {
            buf.writeLong(offset);
            buf.writeInt(snapshot.remaining());
            buf.writeBytes(snapshot.duplicate());
        });
        return buf.nioBuffer();
    }

    public static ElasticPartitionProducerSnapshotsMeta decode(ByteBuffer buffer) {
        ByteBuf buf = Unpooled.wrappedBuffer(buffer);
        byte magicCode = buf.readByte();
        if (magicCode != MAGIC_CODE) {
            throw new IllegalArgumentException("invalid magic code " + magicCode);
        }
        Map<Long, ByteBuffer> snapshots = new HashMap<>();
        while (buf.readableBytes() != 0) {
            long offset = buf.readLong();
            int length = buf.readInt();
            byte[] snapshot = new byte[length];
            buf.readBytes(snapshot);
            ByteBuffer snapshotBuf = ByteBuffer.wrap(snapshot);
            snapshots.put(offset, snapshotBuf);
        }
        return new ElasticPartitionProducerSnapshotsMeta(snapshots);
    }
}
