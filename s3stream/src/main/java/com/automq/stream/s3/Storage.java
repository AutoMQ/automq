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

package com.automq.stream.s3;

import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import java.util.concurrent.CompletableFuture;

/**
 * Write ahead log for server.
 */
public interface Storage {

    void startup();

    void shutdown();

    /**
     * Append stream record.
     *
     * @param streamRecord {@link StreamRecordBatch}
     */
    CompletableFuture<Void> append(AppendContext context, StreamRecordBatch streamRecord);

    default CompletableFuture<Void> append(StreamRecordBatch streamRecord) {
        return append(AppendContext.DEFAULT, streamRecord);
    }

    CompletableFuture<ReadDataBlock> read(FetchContext context, long streamId, long startOffset, long endOffset,
        int maxBytes);

    default CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        return read(FetchContext.DEFAULT, streamId, startOffset, endOffset, maxBytes);
    }

    /**
     * Force stream record in WAL upload to s3
     */
    CompletableFuture<Void> forceUpload(long streamId);
}
