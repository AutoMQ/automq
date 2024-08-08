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

import com.automq.stream.api.ReadOptions;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.blockcache.StreamReader;
import com.automq.stream.s3.trace.context.TraceContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.util.concurrent.CompletableFuture;

public class FetchContext extends TraceContext implements FetchContextRecorder {
    public static final FetchContext DEFAULT = new FetchContext();
    private ReadOptions readOptions = ReadOptions.DEFAULT;

    public FetchContext() {
        super(false, null, null);
    }

    public FetchContext(TraceContext context) {
        super(context);
    }

    public FetchContext(boolean isTraceEnabled, Tracer tracer, Context currentContext) {
        super(isTraceEnabled, tracer, currentContext);
    }

    public ReadOptions readOptions() {
        return readOptions;
    }

    public void setReadOptions(ReadOptions readOptions) {
        this.readOptions = readOptions;
    }

    @Override
    public CompletableFuture<ReadDataBlock> recordReadBlockCacheCf(CompletableFuture<ReadDataBlock> readBlockCacheCf) {
        // noop
        return readBlockCacheCf;
    }

    @Override
    public StreamReader.ReadContext recordBlockCacheReadContext(long leftRetries, StreamReader.ReadContext readContext) {
        // noop
        return readContext;
    }

    @Override
    public void recordStreamReader(StreamReader finalStreamReader) {
        // noop
    }

    @Override
    public void recordGetBlocksContext(StreamReader.GetBlocksContext context) {
        // noop
    }
}
