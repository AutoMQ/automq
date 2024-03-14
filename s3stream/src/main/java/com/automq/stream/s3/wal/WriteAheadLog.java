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

package com.automq.stream.s3.wal;

import com.automq.stream.s3.trace.context.TraceContext;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public interface WriteAheadLog {

    WriteAheadLog start() throws IOException;

    void shutdownGracefully();

    /**
     * Get write ahead log metadata
     *
     * @return {@link WALMetadata}
     */
    WALMetadata metadata();

    /**
     * Append data to log, note append may be out of order.
     * ex. when sequence append R1 R2 , R2 maybe complete before R1.
     * {@link ByteBuf#release()} will be called whatever append success or not.
     *
     * @return The data position will be written.
     */
    AppendResult append(TraceContext context, ByteBuf data, int crc) throws OverCapacityException;

    default AppendResult append(TraceContext context, ByteBuf data) throws OverCapacityException {
        return append(context, data, 0);
    }

    default AppendResult append(ByteBuf data, int crc) throws OverCapacityException {
        return append(TraceContext.DEFAULT, data, crc);
    }

    default AppendResult append(ByteBuf data) throws OverCapacityException {
        return append(TraceContext.DEFAULT, data, 0);
    }

    Iterator<RecoverResult> recover();

    /**
     * Reset all data in log.
     * Equivalent to trim to the end of the log.
     *
     * @return future complete when reset done.
     */
    CompletableFuture<Void> reset();

    /**
     * Trim data <= offset in log.
     *
     * @param offset inclusive trim offset.
     * @return future complete when trim done.
     */
    CompletableFuture<Void> trim(long offset);

    interface AppendResult {
        // The pre-allocated starting offset of the record
        long recordOffset();

        CompletableFuture<CallbackResult> future();

        interface CallbackResult {
            // The record before this offset has been flushed to disk
            long flushedOffset();
        }
    }

    interface RecoverResult {
        ByteBuf record();

        /**
         * @see AppendResult#recordOffset()
         */
        long recordOffset();
    }

    class OverCapacityException extends Exception {
        public OverCapacityException(String message) {
            super(message);
        }
    }

}
