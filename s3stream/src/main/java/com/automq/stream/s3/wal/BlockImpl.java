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
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.ByteBufMetricsStats;
import com.automq.stream.s3.metrics.stats.OperationMetricsStats;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class BlockImpl implements Block {

    private final long startOffset;
    /**
     * The max size of this block.
     * Any try to add a record to this block will fail if the size of this block exceeds this limit.
     */
    private final long maxSize;
    /**
     * The soft limit of this block.
     * Any try to add a record to this block will fail if the size of this block exceeds this limit,
     * unless the block is empty.
     */
    private final long softLimit;
    private final List<CompletableFuture<WriteAheadLog.AppendResult.CallbackResult>> futures = new LinkedList<>();
    private final List<Supplier<ByteBuf>> records = new LinkedList<>();
    private final TimerUtil timer;
    /**
     * The next offset to write in this block.
     * Align to {@link WALUtil#BLOCK_SIZE}
     */
    private long nextOffset = 0;
    private CompositeByteBuf data = null;

    /**
     * Create a block.
     * {@link #release()} must be called when this block is no longer used.
     */
    public BlockImpl(long startOffset, long maxSize, long softLimit) {
        this.startOffset = startOffset;
        this.maxSize = maxSize;
        this.softLimit = softLimit;
        this.timer = new TimerUtil();
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
        assert data == null;
        long requiredCapacity = nextOffset + recordSize;
        if (requiredCapacity > maxSize) {
            return -1;
        }
        // if there is no record in this block, we can write a record larger than SOFT_BLOCK_SIZE_LIMIT
        if (requiredCapacity > softLimit && !futures.isEmpty()) {
            return -1;
        }

        long recordOffset = startOffset + nextOffset;
        records.add(() -> recordSupplier.apply(recordOffset));
        nextOffset += recordSize;
        futures.add(future);

        return recordOffset;
    }

    @Override
    public List<CompletableFuture<WriteAheadLog.AppendResult.CallbackResult>> futures() {
        return futures;
    }

    @Override
    public ByteBuf data() {
        if (null != data) {
            return data;
        }
        if (records.isEmpty()) {
            return null;
        }

        data = DirectByteBufAlloc.compositeByteBuffer();
        for (Supplier<ByteBuf> supplier : records) {
            ByteBuf record = supplier.get();
            ByteBufMetricsStats.getHistogram("wal_record").update(record.readableBytes());
            data.addComponent(true, record);
        }
        ByteBufMetricsStats.getHistogram("wal_block").update(data.readableBytes());
        return data;
    }

    @Override
    public long size() {
        return nextOffset;
    }

    @Override
    public void polled() {
        OperationMetricsStats.getHistogram(S3Operation.APPEND_STORAGE_WAL_BLOCK_POLLED).update(timer.elapsedAs(TimeUnit.NANOSECONDS));
    }
}
