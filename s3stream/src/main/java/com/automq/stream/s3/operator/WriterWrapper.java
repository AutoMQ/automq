/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metadata.S3ObjectMetadata;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

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
