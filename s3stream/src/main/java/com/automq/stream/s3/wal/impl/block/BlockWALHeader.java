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

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.common.ShutdownType;
import com.automq.stream.s3.wal.exception.UnmarshalException;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * Layout:
 * <p>
 * 0 - [4B] {@link BlockWALHeader#magicCode0} Magic code of the WAL header, used to verify the start of the WAL header
 * <p>
 * 1 - [8B] {@link BlockWALHeader#capacity1} Capacity of the block device, which is configured by the application
 * and should not be modified after the first start of the service
 * <p>
 * 2 - [8B] {@link BlockWALHeader#trimOffset2} The logical start offset of the WAL, records before which are
 * considered useless and have been deleted
 * <p>
 * 3 - [8B] {@link BlockWALHeader#lastWriteTimestamp3} The timestamp of the last write to the WAL header, used to
 * determine which WAL header is the latest when recovering
 * <p>
 * 4 - [8B] {@link BlockWALHeader#slidingWindowMaxLength4} The maximum size of the sliding window, which can be
 * scaled up when needed, and is used to determine when to stop recovering
 * <p>
 * 5 - [4B] {@link BlockWALHeader#shutdownType5} The shutdown type of the service, {@link ShutdownType#GRACEFULLY} or
 * {@link ShutdownType#UNGRACEFULLY}
 * <p>
 * 6 - [4B] {@link BlockWALHeader#nodeId6} the node id of the WAL
 * <p>
 * 7 - [4B] {@link BlockWALHeader#epoch7} the epoch id of the node
 * <p>
 * 8 - [4B] {@link BlockWALHeader#crc8} CRC of the rest of the WAL header, used to verify the correctness of the
 * WAL header
 */
public class BlockWALHeader {
    public static final int WAL_HEADER_MAGIC_CODE = 0x12345678;
    public static final int WAL_HEADER_SIZE = 4 // magic code
        + 8 // capacity
        + 8 // trim offset
        + 8 // last write timestamp
        + 8 // sliding window max length
        + 4 // shutdown type
        + 4 // node id
        + 4 // node epoch
        + 8; // crc
    public static final int WAL_HEADER_WITHOUT_CRC_SIZE = WAL_HEADER_SIZE - 4;
    private final AtomicLong trimOffset2 = new AtomicLong(-1);
    private final AtomicLong flushedTrimOffset = new AtomicLong(0);
    private final AtomicLong slidingWindowMaxLength4 = new AtomicLong(0);
    private int magicCode0 = WAL_HEADER_MAGIC_CODE;
    private long capacity1;
    private long lastWriteTimestamp3 = System.nanoTime();
    private ShutdownType shutdownType5 = ShutdownType.UNGRACEFULLY;
    private int nodeId6;
    private long epoch7;
    private int crc8;

    public BlockWALHeader(long capacity, long windowMaxLength) {
        this.capacity1 = capacity;
        this.slidingWindowMaxLength4.set(windowMaxLength);
    }

    public static BlockWALHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        BlockWALHeader blockWalHeader = new BlockWALHeader(0, 0);
        buf.markReaderIndex();
        blockWalHeader.magicCode0 = buf.readInt();
        blockWalHeader.capacity1 = buf.readLong();
        long trimOffset = buf.readLong();
        blockWalHeader.trimOffset2.set(trimOffset);
        blockWalHeader.flushedTrimOffset.set(trimOffset);
        blockWalHeader.lastWriteTimestamp3 = buf.readLong();
        blockWalHeader.slidingWindowMaxLength4.set(buf.readLong());
        blockWalHeader.shutdownType5 = ShutdownType.fromCode(buf.readInt());
        blockWalHeader.nodeId6 = buf.readInt();
        blockWalHeader.epoch7 = buf.readLong();
        blockWalHeader.crc8 = buf.readInt();
        buf.resetReaderIndex();

        if (blockWalHeader.magicCode0 != WAL_HEADER_MAGIC_CODE) {
            throw new UnmarshalException(String.format("WALHeader MagicCode not match, Recovered: [%d] expect: [%d]", blockWalHeader.magicCode0, WAL_HEADER_MAGIC_CODE));
        }

        int crc = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        if (crc != blockWalHeader.crc8) {
            throw new UnmarshalException(String.format("WALHeader CRC not match, Recovered: [%d] expect: [%d]", blockWalHeader.crc8, crc));
        }

        return blockWalHeader;
    }

    public long getCapacity() {
        return capacity1;
    }

    public long getTrimOffset() {
        return trimOffset2.get();
    }

    // Update the trim offset if the given trim offset is larger than the current one.
    public BlockWALHeader updateTrimOffset(long trimOffset) {
        trimOffset2.accumulateAndGet(trimOffset, Math::max);
        return this;
    }

    public long getFlushedTrimOffset() {
        return flushedTrimOffset.get();
    }

    public void updateFlushedTrimOffset(long flushedTrimOffset) {
        this.flushedTrimOffset.accumulateAndGet(flushedTrimOffset, Math::max);
    }

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp3;
    }

    public BlockWALHeader setLastWriteTimestamp(long lastWriteTimestamp) {
        this.lastWriteTimestamp3 = lastWriteTimestamp;
        return this;
    }

    public long getSlidingWindowMaxLength() {
        return slidingWindowMaxLength4.get();
    }

    public AtomicLong getAtomicSlidingWindowMaxLength() {
        return slidingWindowMaxLength4;
    }

    public ShutdownType getShutdownType() {
        return shutdownType5;
    }

    public BlockWALHeader setShutdownType(ShutdownType shutdownType) {
        this.shutdownType5 = shutdownType;
        return this;
    }

    public int getNodeId() {
        return nodeId6;
    }

    public BlockWALHeader setNodeId(int nodeId) {
        this.nodeId6 = nodeId;
        return this;
    }

    public long getEpoch() {
        return epoch7;
    }

    public BlockWALHeader setEpoch(long epoch) {
        this.epoch7 = epoch;
        return this;
    }

    @Override
    public String toString() {
        return "WALHeader{"
            + "magicCode=" + magicCode0
            + ", capacity=" + capacity1
            + ", trimOffset=" + trimOffset2
            + ", lastWriteTimestamp=" + lastWriteTimestamp3
            + ", slidingWindowMaxLength=" + slidingWindowMaxLength4
            + ", shutdownType=" + shutdownType5
            + ", nodeId=" + nodeId6
            + ", epoch=" + epoch7
            + ", crc=" + crc8
            + '}';
    }

    private ByteBuf marshalHeaderExceptCRC() {
        ByteBuf buf = ByteBufAlloc.byteBuffer(WAL_HEADER_SIZE);
        buf.writeInt(magicCode0);
        buf.writeLong(capacity1);
        buf.writeLong(trimOffset2.get());
        buf.writeLong(lastWriteTimestamp3);
        buf.writeLong(slidingWindowMaxLength4.get());
        buf.writeInt(shutdownType5.getCode());
        buf.writeInt(nodeId6);
        buf.writeLong(epoch7);
        return buf;
    }

    ByteBuf marshal() {
        ByteBuf buf = marshalHeaderExceptCRC();
        this.crc8 = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        buf.writeInt(crc8);
        return buf;
    }
}
