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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.context.FetchContext;
import java.util.concurrent.CompletableFuture;

/**
 * Like linux page cache, S3BlockCache is responsible for:
 * 1. read from S3 when the data block is not in cache.
 * 2. caching the data blocks of S3 objects.
 */
public interface S3BlockCache {

    CompletableFuture<ReadDataBlock> read(FetchContext context, long streamId, long startOffset, long endOffset,
                                          int maxBytes);

    default CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        return read(FetchContext.DEFAULT, streamId, startOffset, endOffset, maxBytes);
    }
}
