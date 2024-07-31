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

package kafka.log.streamaspect;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
