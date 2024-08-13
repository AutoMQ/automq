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

package com.automq.stream.s3.wal;

import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.WALFencedException;
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

    /**
     * Recover log from the beginning.
     *
     * @return iterator of recover result.
     *
     * @throws WALFencedException If the log is fenced, it cannot be recovered and the process should exit.
     */
    Iterator<RecoverResult> recover() throws WALFencedException;

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
}
