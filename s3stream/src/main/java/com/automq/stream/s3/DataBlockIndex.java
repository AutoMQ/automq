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

package com.automq.stream.s3;

import java.util.Objects;

import io.netty.buffer.ByteBuf;

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
