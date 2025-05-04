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

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.FixedSizeByteBufPool;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Systems;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;

public class BlockImpl implements Block {

    /**
     * The pool for record headers.
     */
    private static final FixedSizeByteBufPool HEADER_POOL = new FixedSizeByteBufPool(RECORD_HEADER_SIZE, 1024 * Systems.CPU_CORES);

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
    private final List<CompletableFuture<AppendResult.CallbackResult>> futures = new LinkedList<>();
    private final List<Supplier<Record>> recordSuppliers = new LinkedList<>();
    private final long startTime;
    /**
     * The next offset to write in this block.
     * Align to {@link WALUtil#BLOCK_SIZE}
     */
    private long nextOffset = 0;
    /**
     * Lazily generated records and data.
     */
    private List<Record> records = null;
    private CompositeByteBuf data = null;

    /**
     * Create a block.
     * {@link #release()} must be called when this block is no longer used.
     */
    public BlockImpl(long startOffset, long maxSize, long softLimit) {
        this.startOffset = startOffset;
        this.maxSize = maxSize;
        this.softLimit = softLimit;
        this.startTime = System.nanoTime();
    }

    @Override
    public long startOffset() {
        return startOffset;
    }

    /**
     * Note: this method is NOT thread safe.
     */
    @Override
    public long addRecord(long recordSize, RecordSupplier recordSupplier,
        CompletableFuture<AppendResult.CallbackResult> future) {
        assert records == null;
        long requiredCapacity = nextOffset + recordSize;
        if (requiredCapacity > maxSize) {
            return -1;
        }
        // if there is no record in this block, we can write a record larger than SOFT_BLOCK_SIZE_LIMIT
        if (requiredCapacity > softLimit && !futures.isEmpty()) {
            return -1;
        }

        long recordOffset = startOffset + nextOffset;
        recordSuppliers.add(() -> {
            ByteBuf header = HEADER_POOL.get().retain();
            return recordSupplier.get(recordOffset, header);
        });
        nextOffset += recordSize;
        futures.add(future);

        return recordOffset;
    }

    @Override
    public List<CompletableFuture<AppendResult.CallbackResult>> futures() {
        return futures;
    }

    @Override
    public ByteBuf data() {
        maybeGenerateRecords();
        maybeGenerateData();
        return data;
    }

    private void maybeGenerateRecords() {
        if (null != records) {
            return;
        }
        records = recordSuppliers.stream()
            .map(Supplier::get)
            .collect(Collectors.toUnmodifiableList());
    }

    private void maybeGenerateData() {
        if (null != data) {
            return;
        }
        data = ByteBufAlloc.compositeByteBuffer();
        for (Record record : records) {
            data.addComponents(true, record.header(), record.body());
        }
    }

    @Override
    public long size() {
        return nextOffset;
    }

    @Override
    public void release() {
        if (null != data) {
            data.release();
        }
        if (null != records) {
            records.stream()
                .map(Record::header)
                .forEach(HEADER_POOL::release);
        }
    }

    @Override
    public void polled() {
        StorageOperationStats.getInstance().appendWALBlockPolledStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
    }
}
