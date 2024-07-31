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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;

public abstract class WriterWrapper implements Writer {
    private final Writer inner;

    public WriterWrapper(Writer writer) {
        this.inner = writer;
    }

    @Override
    public CompletableFuture<Void> write(ByteBuf data) {
        return inner.write(data);
    }

    @Override
    public void copyOnWrite() {
        inner.copyOnWrite();
    }

    @Override
    public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
        inner.copyWrite(s3ObjectMetadata, start, end);
    }

    @Override
    public boolean hasBatchingPart() {
        return inner.hasBatchingPart();
    }

    @Override
    public CompletableFuture<Void> close() {
        return inner.close();
    }

    @Override
    public CompletableFuture<Void> release() {
        return inner.release();
    }

    public abstract short bucketId();
}
