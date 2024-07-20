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

package com.automq.stream.s3.wal.impl;

import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.WriteAheadLog;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryWriteAheadLog implements WriteAheadLog {
    private final AtomicLong offsetAlloc = new AtomicLong();

    @Override
    public WriteAheadLog start() throws IOException {
        return this;
    }

    @Override
    public void shutdownGracefully() {

    }

    @Override
    public WALMetadata metadata() {
        return new WALMetadata(0, 0);
    }

    @Override
    public AppendResult append(TraceContext traceContext, ByteBuf data, int crc) {
        data.release();
        long offset = offsetAlloc.getAndIncrement();
        return new AppendResult() {
            @Override
            public long recordOffset() {
                return offset;
            }

            @Override
            public CompletableFuture<CallbackResult> future() {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    @Override
    public Iterator<RecoverResult> recover() {
        List<RecoverResult> l = Collections.emptyList();
        return l.iterator();
    }

    @Override
    public CompletableFuture<Void> reset() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        return CompletableFuture.completedFuture(null);
    }
}
