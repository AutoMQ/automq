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
import io.netty.buffer.CompositeByteBuf;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class BlockImpl implements Block {

    /**
     * The soft limit of block size. (128 KiB)
     * TODO make it configurable
     */
    private static final long SOFT_BLOCK_SIZE_LIMIT = 1 << 17;

    private final long startOffset;
    /**
     * The max size of this block.
     * Any try to add a record to this block will fail if the size of this block exceeds this limit.
     */
    private final long maxSize;
    private final CompositeByteBuf data = DirectByteBufAlloc.compositeByteBuffer();
    private final List<CompletableFuture<WriteAheadLog.AppendResult.CallbackResult>> futures = new LinkedList<>();
    /**
     * The next offset to write in this block.
     * Align to {@link WALUtil#BLOCK_SIZE}
     */
    private long nextOffset = 0;

    /**
     * Create a block.
     * {@link #release()} must be called when this block is no longer used.
     */
    public BlockImpl(long startOffset, long maxSize) {
        this.startOffset = startOffset;
        this.maxSize = maxSize;
    }

    @Override
    public long startOffset() {
        return startOffset;
    }

    /**
     * Note: this method is NOT thread safe.
     */
    @Override
    public long addRecord(long recordSize, Function<Long, ByteBuf> recordSupplier, CompletableFuture<WriteAheadLog.AppendResult.CallbackResult> future) {
        // TODO no need to align to block size
        long requiredSize = WALUtil.alignLargeByBlockSize(recordSize);
        long requiredCapacity = nextOffset + requiredSize;

        if (requiredCapacity > maxSize) {
            return -1;
        }
        // if there is no record in this block, we can write a record larger than SOFT_BLOCK_SIZE_LIMIT
        if (requiredCapacity > SOFT_BLOCK_SIZE_LIMIT && !futures.isEmpty()) {
            return -1;
        }

        long recordOffset = startOffset + nextOffset;
        ByteBuf record = recordSupplier.apply(recordOffset);
        // padding record to required size
        // TODO no need to align to block size
        if (record.readableBytes() < requiredSize) {
            ByteBuf padding = DirectByteBufAlloc.byteBuffer((int) (requiredSize - record.readableBytes()));
            padding.writeZero(padding.capacity());
            record = DirectByteBufAlloc.compositeByteBuffer().addComponents(true, record, padding);
        }
        data.addComponent(true, record);
        nextOffset += requiredSize;
        futures.add(future);

        return recordOffset;
    }

    @Override
    public List<CompletableFuture<WriteAheadLog.AppendResult.CallbackResult>> futures() {
        return futures;
    }

    @Override
    public ByteBuf data() {
        return data;
    }
}
