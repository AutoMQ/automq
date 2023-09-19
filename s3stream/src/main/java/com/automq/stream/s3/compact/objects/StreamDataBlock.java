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

package com.automq.stream.s3.compact.objects;

import io.netty.buffer.ByteBuf;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class StreamDataBlock {
    public static final Comparator<StreamDataBlock> STREAM_OFFSET_COMPARATOR = Comparator.comparingLong(StreamDataBlock::getStartOffset);
    public static final Comparator<StreamDataBlock> BLOCK_POSITION_COMPARATOR = Comparator.comparingLong(StreamDataBlock::getBlockStartPosition);

    // Stream attributes
    private final long streamId;
    private final long startOffset;
    private final long endOffset;
    private final int blockId;

    // Object attributes
    private final long objectId;
    private final long blockPosition;
    private final int blockSize;
    private final int recordCount;
    private final CompletableFuture<ByteBuf> dataCf = new CompletableFuture<>();

    public StreamDataBlock(long streamId, long startOffset, long endOffset, int blockId,
                           long objectId, long blockPosition, int blockSize, int recordCount) {
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.blockId = blockId;
        this.objectId = objectId;
        this.blockPosition = blockPosition;
        this.blockSize = blockSize;
        this.recordCount = recordCount;
    }

    public long getStreamId() {
        return streamId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getStreamRangeSize() {
        return endOffset - startOffset;
    }

    public int getBlockId() {
        return blockId;
    }

    public long getObjectId() {
        return objectId;
    }

    public long getBlockStartPosition() {
        return blockPosition;
    }

    public long getBlockEndPosition() {
        return blockPosition + blockSize;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public CompletableFuture<ByteBuf> getDataCf() {
        return this.dataCf;
    }

    public void free() {
        this.dataCf.thenAccept(buf -> {
            if (buf != null) {
                buf.release();
            }
        });
    }

    @Override
    public String toString() {
        return "StreamDataBlock{" +
                "streamId=" + streamId +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", objectId=" + objectId +
                ", blockPosition=" + blockPosition +
                ", blockSize=" + blockSize +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamDataBlock that = (StreamDataBlock) o;
        return streamId == that.streamId && startOffset == that.startOffset && endOffset == that.endOffset
                && blockId == that.blockId && objectId == that.objectId && blockPosition == that.blockPosition
                && blockSize == that.blockSize && recordCount == that.recordCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, startOffset, endOffset, blockId, objectId, blockPosition, blockSize, recordCount);
    }

}