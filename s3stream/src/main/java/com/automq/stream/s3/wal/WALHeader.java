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
 * 4 - [8B] {@link WALHeader#slidingWindowMaxLength4} The maximum size of the sliding window, which can be
 * scaled up when needed, and is used to determine when to stop recovering
 * <p>
 * 5 - [4B] {@link WALHeader#shutdownType5} The shutdown type of the service, {@link ShutdownType#GRACEFULLY} or
 * {@link ShutdownType#UNGRACEFULLY}
 * <p>
 * 6 - [4B] {@link WALHeader#nodeId6} the node id of the WAL
 * <p>
 * 7 - [4B] {@link WALHeader#epoch7} the epoch id of the node
 * <p>
 * 8 - [4B] {@link WALHeader#crc8} CRC of the rest of the WAL header, used to verify the correctness of the
 * WAL header
 */
class WALHeader {
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

    public static WALHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        WALHeader walHeader = new WALHeader();
        buf.markReaderIndex();
        walHeader.magicCode0 = buf.readInt();
        walHeader.capacity1 = buf.readLong();
        long trimOffset = buf.readLong();
        walHeader.trimOffset2.set(trimOffset);
        walHeader.flushedTrimOffset.set(trimOffset);
        walHeader.lastWriteTimestamp3 = buf.readLong();
        walHeader.slidingWindowMaxLength4.set(buf.readLong());
        walHeader.shutdownType5 = ShutdownType.fromCode(buf.readInt());
        walHeader.nodeId6 = buf.readInt();
        walHeader.epoch7 = buf.readLong();
        walHeader.crc8 = buf.readInt();
        buf.resetReaderIndex();

        if (walHeader.magicCode0 != WAL_HEADER_MAGIC_CODE) {
            throw new UnmarshalException(String.format("WALHeader MagicCode not match, Recovered: [%d] expect: [%d]", walHeader.magicCode0, WAL_HEADER_MAGIC_CODE));
        }

        int crc = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        if (crc != walHeader.crc8) {
            throw new UnmarshalException(String.format("WALHeader CRC not match, Recovered: [%d] expect: [%d]", walHeader.crc8, crc));
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

    public void updateFlushedTrimOffset(long flushedTrimOffset) {
        this.flushedTrimOffset.accumulateAndGet(flushedTrimOffset, Math::max);
    }

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp3;
    }

    public WALHeader setLastWriteTimestamp(long lastWriteTimestamp) {
        this.lastWriteTimestamp3 = lastWriteTimestamp;
        return this;
    }

    public long getSlidingWindowMaxLength() {
        return slidingWindowMaxLength4.get();
    }

    public WALHeader setSlidingWindowMaxLength(long slidingWindowMaxLength) {
        this.slidingWindowMaxLength4.set(slidingWindowMaxLength);
        return this;
    }

    public ShutdownType getShutdownType() {
        return shutdownType5;
    }

    public WALHeader setShutdownType(ShutdownType shutdownType) {
        this.shutdownType5 = shutdownType;
        return this;
    }

    public int getNodeId() {
        return nodeId6;
    }

    public WALHeader setNodeId(int nodeId) {
        this.nodeId6 = nodeId;
        return this;
    }

    public long getEpoch() {
        return epoch7;
    }

    public WALHeader setEpoch(long epoch) {
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
        ByteBuf buf = DirectByteBufAlloc.byteBuffer(WAL_HEADER_SIZE);
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
