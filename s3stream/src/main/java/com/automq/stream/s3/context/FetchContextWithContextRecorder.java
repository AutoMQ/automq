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

package com.automq.stream.s3.context;

import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.blockcache.StreamReader;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FetchContextWithContextRecorder extends FetchContext {
    long streamId;
    long startOffset;
    long endOffset;
    long maxBytes;

    public FetchContextWithContextRecorder(long streamId, long startOffset, long endOffset, long maxBytes) {
        super();
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.maxBytes = maxBytes;
    }

    CompletableFuture<ReadDataBlock> readBlockCacheCf;
    ConcurrentHashMap<Long, StreamReader.ReadContext> readContext = new ConcurrentHashMap<>();
    StreamReader reader;

    @Override
    public CompletableFuture<ReadDataBlock> recordReadBlockCacheCf(CompletableFuture<ReadDataBlock> readBlockCacheCf) {
        this.readBlockCacheCf = readBlockCacheCf;
        return readBlockCacheCf;
    }

    @Override
    public StreamReader.ReadContext recordBlockCacheReadContext(long leftRetries, StreamReader.ReadContext readContext) {
        this.readContext.put(leftRetries, readContext);
        return readContext;
    }

    @Override
    public void recordStreamReader(StreamReader finalStreamReader) {
        this.reader = finalStreamReader;
    }

    Queue<StreamReader.GetBlocksContext> getBlocksContext = new ConcurrentLinkedQueue<>();

    @Override
    public void recordGetBlocksContext(StreamReader.GetBlocksContext context) {
        this.getBlocksContext.add(context);
    }
}
