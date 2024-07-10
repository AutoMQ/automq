/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import io.netty.buffer.ByteBuf;
import java.util.Objects;

public final class DataBlockIndex {

    public static final int BLOCK_INDEX_SIZE = 8/* streamId */ + 8 /* startOffset */ + 4 /* endOffset delta */
        + 4 /* record count */ + 8 /* block position */ + 4 /* block size */;
    private final int blockId;
    private final long streamId;
    private final long startOffset;
    private final int endOffsetDelta;
    private final int recordCount;
    private final long startPosition;
    private final int size;

    public DataBlockIndex(long streamId, long startOffset, int endOffsetDelta, int recordCount,
        long startPosition, int size) {
        this(-1, streamId, startOffset, endOffsetDelta, recordCount, startPosition, size);
    }

    public DataBlockIndex(int blockId, long streamId, long startOffset, int endOffsetDelta, int recordCount,
        long startPosition, int size) {
        this.blockId = blockId;
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffsetDelta = endOffsetDelta;
        this.recordCount = recordCount;
        this.startPosition = startPosition;
        this.size = size;
    }

    public int id() {
        return blockId;
    }

    public long endOffset() {
        return startOffset + endOffsetDelta;
    }

    public long endPosition() {
        return startPosition + size;
    }

    public void encode(ByteBuf buf) {
        buf.writeLong(streamId);
        buf.writeLong(startOffset);
        buf.writeInt(endOffsetDelta);
        buf.writeInt(recordCount);
        buf.writeLong(startPosition);
        buf.writeInt(size);
    }

    public long streamId() {
        return streamId;
    }

    public long startOffset() {
        return startOffset;
    }

    public int endOffsetDelta() {
        return endOffsetDelta;
    }

    public int recordCount() {
        return recordCount;
    }

    public long startPosition() {
        return startPosition;
    }

    public int size() {
        return size;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        var that = (DataBlockIndex) obj;
        return this.streamId == that.streamId &&
            this.startOffset == that.startOffset &&
            this.endOffsetDelta == that.endOffsetDelta &&
            this.recordCount == that.recordCount &&
            this.startPosition == that.startPosition &&
            this.size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, startOffset, endOffsetDelta, recordCount, startPosition, size);
    }

    @Override
    public String toString() {
        return "DataBlockIndex[" +
            "blockId=" + blockId + ", " +
            "streamId=" + streamId + ", " +
            "startOffset=" + startOffset + ", " +
            "endOffsetDelta=" + endOffsetDelta + ", " +
            "recordCount=" + recordCount + ", " +
            "startPosition=" + startPosition + ", " +
            "size=" + size + ']';
    }

}
