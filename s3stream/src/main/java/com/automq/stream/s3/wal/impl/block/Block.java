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

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A Block contains multiple records, and will be written to the WAL in one batch.
 */
public interface Block {
    /**
     * The start offset of this block.
     * Align to {@link WALUtil#BLOCK_SIZE}
     */
    long startOffset();

    /**
     * The start time of this block*
     * @return
     */
    long startTime();
    /**
     * Append a record to this block.
     * Cannot be called after {@link #data()} is called.
     *
     * @param recordSize     The size of this record.
     * @param recordSupplier The supplier of this record which receives the start offset of this record as the parameter.
     * @param future         The future of this record, which will be completed when the record is written to the WAL.
     * @return The start offset of this record. If the size of this block exceeds the limit, return -1.
     */
    long addRecord(long recordSize, Function<Long, ByteBuf> recordSupplier,
        CompletableFuture<AppendResult.CallbackResult> future);

    /**
     * Futures of all records in this block.
     */
    List<CompletableFuture<AppendResult.CallbackResult>> futures();

    default boolean isEmpty() {
        return futures().isEmpty();
    }

    /**
     * The content of this block, which contains multiple records.
     * The first call of this method will marshal all records in this block to a ByteBuf. It will be cached for later calls.
     * It returns null if this block is empty.
     */
    ByteBuf data();

    /**
     * The size of this block.
     */
    long size();

    default void release() {
        ByteBuf data = data();
        if (null != data) {
            data.release();
        }
    }

    /**
     * Called when this block is polled and sent to the writer.
     * Used for metrics.
     */
    void polled();
}
