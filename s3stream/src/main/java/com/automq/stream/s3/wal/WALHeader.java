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

package com.automq.stream.s3.wal;

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * Layout:
 * <p>
 * 0 - [4B] {@link WALHeader#magicCode0} Magic code of the WAL header, used to verify the start of the WAL header
 * <p>
 * 1 - [8B] {@link WALHeader#capacity1} Capacity of the block device, which is configured by the application
 * and should not be modified after the first start of the service
 * <p>
 * 2 - [8B] {@link WALHeader#trimOffset2} The logical start offset of the WAL, records before which are
 * considered useless and have been deleted
 * <p>
 * 3 - [8B] {@link WALHeader#lastWriteTimestamp3} The timestamp of the last write to the WAL header, used to
 * determine which WAL header is the latest when recovering
 * <p>
 * 4 - [8B] {@link WALHeader#slidingWindowNextWriteOffset4} The offset of the next record to be written
 * in the sliding window
 * <p>
 * 5 - [8B] {@link WALHeader#slidingWindowStartOffset5} The start offset of the sliding window, all records
 * before this offset have been successfully written to the block device
 * <p>
 * 6 - [8B] {@link WALHeader#slidingWindowMaxLength6} The maximum size of the sliding window, which can be
 * scaled up when needed, and is used to determine when to stop recovering
 * <p>
 * 7 - [4B] {@link WALHeader#shutdownType7} The shutdown type of the service, {@link ShutdownType#GRACEFULLY} or
 * {@link ShutdownType#UNGRACEFULLY}
 * <p>
 * 8 - [4B] {@link WALHeader#nodeId8} the node id of the WAL
 * <p>
 * 9 - [4B] {@link WALHeader#epoch9} the epoch id of the node
 * <p>
 * 10 - [4B] {@link WALHeader#crc10} CRC of the rest of the WAL header, used to verify the correctness of the
 * WAL header
 */
class WALHeader {
    public static final int WAL_HEADER_MAGIC_CODE = 0x12345678;
    public static final int WAL_HEADER_SIZE = 4 // magic code
            + 8 // capacity
            + 8 // trim offset
            + 8 // last write timestamp
            + 8 // sliding window next write offset
            + 8 // sliding window start offset
            + 8 // sliding window max length
            + 4 // shutdown type
            + 4 // node id
            + 4 // node epoch
            + 8; // crc
    public static final int WAL_HEADER_WITHOUT_CRC_SIZE = WAL_HEADER_SIZE - 4;
    private final AtomicLong trimOffset2 = new AtomicLong(-1);
    private final AtomicLong flushedTrimOffset = new AtomicLong(0);
    private final AtomicLong slidingWindowNextWriteOffset4 = new AtomicLong(0);
    private final AtomicLong slidingWindowStartOffset5 = new AtomicLong(0);
    private final AtomicLong slidingWindowMaxLength6 = new AtomicLong(0);
    private int magicCode0 = WAL_HEADER_MAGIC_CODE;
    private long capacity1;
    private long lastWriteTimestamp3 = System.nanoTime();
    private ShutdownType shutdownType7 = ShutdownType.UNGRACEFULLY;
    private int nodeId8;
    private long epoch9;
    private int crc10;

    public static WALHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        WALHeader walHeader = new WALHeader();
        buf.markReaderIndex();
        walHeader.magicCode0 = buf.readInt();
        walHeader.capacity1 = buf.readLong();
        walHeader.trimOffset2.set(buf.readLong());
        walHeader.lastWriteTimestamp3 = buf.readLong();
        walHeader.slidingWindowNextWriteOffset4.set(buf.readLong());
        walHeader.slidingWindowStartOffset5.set(buf.readLong());
        walHeader.slidingWindowMaxLength6.set(buf.readLong());
        walHeader.shutdownType7 = ShutdownType.fromCode(buf.readInt());
        walHeader.nodeId8 = buf.readInt();
        walHeader.epoch9 = buf.readLong();
        walHeader.crc10 = buf.readInt();
        buf.resetReaderIndex();

        if (walHeader.magicCode0 != WAL_HEADER_MAGIC_CODE) {
            throw new UnmarshalException(String.format("WALHeader MagicCode not match, Recovered: [%d] expect: [%d]", walHeader.magicCode0, WAL_HEADER_MAGIC_CODE));
        }

        int crc = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        if (crc != walHeader.crc10) {
            throw new UnmarshalException(String.format("WALHeader CRC not match, Recovered: [%d] expect: [%d]", walHeader.crc10, crc));
        }

        return walHeader;
    }

    public long recordSectionCapacity() {
        return capacity1 - BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
    }

    public long getCapacity() {
        return capacity1;
    }

    public WALHeader setCapacity(long capacity) {
        this.capacity1 = capacity;
        return this;
    }

    public long getSlidingWindowStartOffset() {
        return slidingWindowStartOffset5.get();
    }

    public WALHeader setSlidingWindowStartOffset(long slidingWindowStartOffset) {
        this.slidingWindowStartOffset5.set(slidingWindowStartOffset);
        return this;
    }

    public long getTrimOffset() {
        return trimOffset2.get();
    }

    // Update the trim offset if the given trim offset is larger than the current one.
    public WALHeader updateTrimOffset(long trimOffset) {
        trimOffset2.accumulateAndGet(trimOffset, Math::max);
        return this;
    }

    public long getFlushedTrimOffset() {
        return flushedTrimOffset.get();
    }

    public void setFlushedTrimOffset(long flushedTrimOffset) {
        this.flushedTrimOffset.set(flushedTrimOffset);
    }

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp3;
    }

    public WALHeader setLastWriteTimestamp(long lastWriteTimestamp) {
        this.lastWriteTimestamp3 = lastWriteTimestamp;
        return this;
    }

    public long getSlidingWindowNextWriteOffset() {
        return slidingWindowNextWriteOffset4.get();
    }

    public WALHeader setSlidingWindowNextWriteOffset(long slidingWindowNextWriteOffset) {
        this.slidingWindowNextWriteOffset4.set(slidingWindowNextWriteOffset);
        return this;
    }

    public long getSlidingWindowMaxLength() {
        return slidingWindowMaxLength6.get();
    }

    public WALHeader setSlidingWindowMaxLength(long slidingWindowMaxLength) {
        this.slidingWindowMaxLength6.set(slidingWindowMaxLength);
        return this;
    }

    public ShutdownType getShutdownType() {
        return shutdownType7;
    }

    public WALHeader setShutdownType(ShutdownType shutdownType) {
        this.shutdownType7 = shutdownType;
        return this;
    }

    public int getNodeId() {
        return nodeId8;
    }

    public WALHeader setNodeId(int nodeId) {
        this.nodeId8 = nodeId;
        return this;
    }

    public long getEpoch() {
        return epoch9;
    }

    public WALHeader setEpoch(long epoch) {
        this.epoch9 = epoch;
        return this;
    }

    @Override
    public String toString() {
        return "WALHeader{"
                + "magicCode=" + magicCode0
                + ", capacity=" + capacity1
                + ", trimOffset=" + trimOffset2
                + ", lastWriteTimestamp=" + lastWriteTimestamp3
                + ", nextWriteOffset=" + slidingWindowNextWriteOffset4
                + ", slidingWindowStartOffset=" + slidingWindowStartOffset5
                + ", slidingWindowMaxLength=" + slidingWindowMaxLength6
                + ", shutdownType=" + shutdownType7
                + ", nodeId=" + nodeId8
                + ", epoch=" + epoch9
                + ", crc=" + crc10
                + '}';
    }

    private ByteBuf marshalHeaderExceptCRC() {
        ByteBuf buf = DirectByteBufAlloc.byteBuffer(WAL_HEADER_SIZE);
        buf.writeInt(magicCode0);
        buf.writeLong(capacity1);
        buf.writeLong(trimOffset2.get());
        buf.writeLong(lastWriteTimestamp3);
        buf.writeLong(slidingWindowNextWriteOffset4.get());
        buf.writeLong(slidingWindowStartOffset5.get());
        buf.writeLong(slidingWindowMaxLength6.get());
        buf.writeInt(shutdownType7.getCode());
        buf.writeInt(nodeId8);
        buf.writeLong(epoch9);
        return buf;
    }

    ByteBuf marshal() {
        ByteBuf buf = marshalHeaderExceptCRC();
        this.crc10 = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        buf.writeInt(crc10);
        return buf;
    }
}
