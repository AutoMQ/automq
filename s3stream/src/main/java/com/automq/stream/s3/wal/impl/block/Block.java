/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.util.WALUtil;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

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
     * Append a record to this block.
     * Cannot be called after {@link #data()} is called.
     *
     * @param recordSize     The size of this record.
     * @param recordSupplier The supplier of this record.
     * @param future         The future of this record, which will be completed when the record is written to the WAL.
     * @return The start offset of this record. If the size of this block exceeds the limit, return -1.
     */
    long addRecord(long recordSize, RecordSupplier recordSupplier,
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
     */
    ByteBuf data();

    /**
     * The size of this block.
     */
    long size();

    /**
     * The end offset of this block.
     */
    default long endOffset() {
        return startOffset() + size();
    }

    void release();

    /**
     * Mark this block has been written to the WAL.
     */
    void markWritten();

    /**
     * Check if this block has been written to the WAL.
     */
    boolean isWritten();

    /**
     * Called when this block is polled and sent to the writer.
     * Used for metrics.
     */
    void polled();

    @FunctionalInterface
    interface RecordSupplier {
        /**
         * Generate a record.
         *
         * @param recordStartOffset The start offset of this record.
         * @param emptyHeader       An empty {@link ByteBuf} with the size of {@link RecordHeader#RECORD_HEADER_SIZE}. It will be used to marshal the header.
         * @return The record.
         */
        Record get(long recordStartOffset, ByteBuf emptyHeader);
    }
}
