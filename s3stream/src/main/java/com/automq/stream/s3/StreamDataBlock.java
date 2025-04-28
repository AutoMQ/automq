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

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

public class StreamDataBlock {
    public static final Comparator<StreamDataBlock> STREAM_OFFSET_COMPARATOR = Comparator.comparingLong(StreamDataBlock::getStartOffset);
    public static final Comparator<StreamDataBlock> BLOCK_POSITION_COMPARATOR = Comparator.comparingLong(StreamDataBlock::getBlockStartPosition);
    private long objectId;
    private final DataBlockIndex dataBlockIndex;
    private final CompletableFuture<ByteBuf> dataCf = new CompletableFuture<>();
    private final AtomicInteger refCount = new AtomicInteger(1);

    public StreamDataBlock(long objectId, DataBlockIndex dataBlockIndex) {
        this.dataBlockIndex = dataBlockIndex;
        this.objectId = objectId;
    }

    public StreamDataBlock(long streamId, long startOffset, long endOffset,
        long objectId, long blockPosition, int blockSize, int recordCount) {
        this.objectId = objectId;
        this.dataBlockIndex = new DataBlockIndex(streamId, startOffset, (int) (endOffset - startOffset), recordCount, blockPosition, blockSize);
    }

    public long getStreamId() {
        return dataBlockIndex.streamId();
    }

    public long getStartOffset() {
        return dataBlockIndex.startOffset();
    }

    public long getEndOffset() {
        return dataBlockIndex.endOffset();
    }

    public long getStreamRangeSize() {
        return dataBlockIndex.endOffsetDelta();
    }

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getBlockStartPosition() {
        return dataBlockIndex.startPosition();
    }

    public long getBlockEndPosition() {
        return dataBlockIndex.endPosition();
    }

    public int getBlockSize() {
        return dataBlockIndex.size();
    }

    public DataBlockIndex dataBlockIndex() {
        return dataBlockIndex;
    }

    public CompletableFuture<ByteBuf> getDataCf() {
        return this.dataCf;
    }

    public ByteBuf getAndReleaseData() {
        if (refCount.getAndDecrement() == 0) {
            throw new IllegalStateException("Data has already been released");
        }
        return this.dataCf.join();
    }

    public void releaseRef() {
        refCount.decrementAndGet();
    }

    public void release() {
        if (refCount.decrementAndGet() == 0) {
            dataCf.thenAccept(buf -> {
                if (buf != null) {
                    buf.release();
                }
            });
        }
    }

    @Override
    public String toString() {
        return "StreamDataBlock{" +
            "objectId=" + objectId +
            ", dataBlockIndex=" + dataBlockIndex +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StreamDataBlock that = (StreamDataBlock) o;
        return objectId == that.objectId && dataBlockIndex.equals(that.dataBlockIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, dataBlockIndex);
    }

}
