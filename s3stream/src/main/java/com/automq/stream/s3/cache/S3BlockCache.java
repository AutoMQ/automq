/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache;

import com.automq.stream.s3.trace.context.TraceContext;
import java.util.concurrent.CompletableFuture;

/**
 * Like linux page cache, S3BlockCache is responsible for:
 * 1. read from S3 when the data block is not in cache.
 * 2. caching the data blocks of S3 objects.
 */
public interface S3BlockCache {

    CompletableFuture<ReadDataBlock> read(TraceContext context, long streamId, long startOffset, long endOffset,
        int maxBytes);

    default CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        return read(TraceContext.DEFAULT, streamId, startOffset, endOffset, maxBytes);
    }
}
