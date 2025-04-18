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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.S3ObjectStats;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.utils.FutureUtil;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * If object data size is less than ObjectWriter.MAX_UPLOAD_SIZE, we should use single upload to upload it.
 * Else, we should use multi-part upload to upload it.
 */
class ProxyWriter implements Writer {
    final ObjectWriter objectWriter = new ObjectWriter();
    private final WriteOptions writeOptions;
    private final AbstractObjectStorage objectStorage;
    private final String path;
    private final long minPartSize;
    Writer largeObjectWriter = null;

    public ProxyWriter(WriteOptions writeOptions, AbstractObjectStorage objectStorage, String path, long minPartSize) {
        this.writeOptions = writeOptions;
        this.objectStorage = objectStorage;
        this.path = path;
        this.minPartSize = minPartSize;
    }

    public ProxyWriter(WriteOptions writeOptions, AbstractObjectStorage objectStorage, String path) {
        this(writeOptions, objectStorage, path, Writer.MIN_PART_SIZE);
    }

    @Override
    public CompletableFuture<Void> write(ByteBuf part) {
        if (largeObjectWriter != null) {
            return largeObjectWriter.write(part);
        } else {
            objectWriter.write(part);
            if (objectWriter.isFull()) {
                newLargeObjectWriter(writeOptions, objectStorage, path);
            }
            return objectWriter.cf;
        }
    }

    @Override
    public void copyOnWrite() {
        if (largeObjectWriter != null) {
            largeObjectWriter.copyOnWrite();
        } else {
            objectWriter.copyOnWrite();
        }
    }

    @Override
    public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
        if (largeObjectWriter == null) {
            newLargeObjectWriter(writeOptions, objectStorage, path);
        }
        largeObjectWriter.copyWrite(s3ObjectMetadata, start, end);
    }

    @Override
    public boolean hasBatchingPart() {
        if (largeObjectWriter != null) {
            return largeObjectWriter.hasBatchingPart();
        } else {
            return objectWriter.hasBatchingPart();
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        if (largeObjectWriter != null) {
            return largeObjectWriter.close();
        } else {
            return objectWriter.close();
        }
    }

    @Override
    public CompletableFuture<Void> release() {
        if (largeObjectWriter != null) {
            return largeObjectWriter.release();
        } else {
            return objectWriter.release();
        }
    }

    @Override
    public short bucketId() {
        return writeOptions.bucketId();
    }

    protected void newLargeObjectWriter(WriteOptions writeOptions, AbstractObjectStorage objectStorage, String path) {
        this.largeObjectWriter = new MultiPartWriter(writeOptions, objectStorage, path, minPartSize);
        if (objectWriter.data.readableBytes() > 0) {
            FutureUtil.propagate(largeObjectWriter.write(objectWriter.data), objectWriter.cf);
        } else {
            objectWriter.data.release();
            objectWriter.cf.complete(null);
        }
    }

    class ObjectWriter implements Writer {
        // max upload size, when object data size is larger than MAX_UPLOAD_SIZE, we should use multi-part upload to upload it.
        static final long MAX_UPLOAD_SIZE = 32L * 1024 * 1024;
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompositeByteBuf data = ByteBufAlloc.compositeByteBuffer();
        TimerUtil timerUtil = new TimerUtil();

        @Override
        public CompletableFuture<Void> write(ByteBuf part) {
            data.addComponent(true, part);
            return cf;
        }

        @Override
        public void copyOnWrite() {
            int size = data.readableBytes();
            if (size > 0) {
                ByteBuf buf = ByteBufAlloc.byteBuffer(size, writeOptions.allocType());
                buf.writeBytes(data.duplicate());
                CompositeByteBuf copy = ByteBufAlloc.compositeByteBuffer().addComponent(true, buf);
                this.data.release();
                this.data = copy;
            }
        }

        @Override
        public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBatchingPart() {
            return true;
        }

        @Override
        public CompletableFuture<Void> close() {
            S3ObjectStats.getInstance().objectStageReadyCloseStats.record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            int size = data.readableBytes();
            FutureUtil.propagate(objectStorage.write(writeOptions, path, data).thenApply(rst -> null), cf);
            cf.whenComplete((nil, e) -> {
                S3ObjectStats.getInstance().objectStageTotalStats.record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                S3ObjectStats.getInstance().objectNumInTotalStats.add(MetricsLevel.DEBUG, 1);
                S3ObjectStats.getInstance().objectUploadSizeStats.record(size);
            });
            return cf;
        }

        @Override
        public CompletableFuture<Void> release() {
            data.release();
            return CompletableFuture.completedFuture(null);
        }

        public boolean isFull() {
            return data.readableBytes() > MAX_UPLOAD_SIZE;
        }

        @Override
        public short bucketId() {
            return writeOptions.bucketId();
        }
    }
}
