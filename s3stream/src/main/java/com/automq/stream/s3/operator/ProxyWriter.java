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
import com.automq.stream.s3.metrics.S3ObjectMetrics;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.utils.FutureUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * Selects PutObject or multipart upload after observing the object operations. Before the selection is made,
 * operations are retained in call order and their buffers are owned by this writer. Closing replays them to the
 * PutObject path, while exceeding the multipart upload threshold replays them to a multipart writer.
 *
 * <p>This writer expects calls to be serialized. After {@link #copyOnWrite()} returns, callers may safely modify
 * buffers passed to earlier {@link #write(ByteBuf)} calls. {@link #release()} releases every retained buffer that
 * has not already been transferred to the selected writer.</p>
 */
public class ProxyWriter implements Writer {
    private static final long MULTIPART_UPLOAD_THRESHOLD = 32L * 1024 * 1024;
    private final WriteOptions writeOptions;
    private final ObjectStorage objectStorage;
    private final String path;
    private final long minPartSize;
    private final List<PendingOperation> pendingOperations = new ArrayList<>();
    private final CompletableFuture<Void> objectCf = new CompletableFuture<>();
    private final TimerUtil timerUtil = new TimerUtil();
    private Writer multiPartObjectWriter = null;
    private long size;

    public ProxyWriter(WriteOptions writeOptions, ObjectStorage objectStorage, String path, long minPartSize) {
        this.writeOptions = writeOptions;
        this.objectStorage = objectStorage;
        this.path = path;
        this.minPartSize = minPartSize;
    }

    public ProxyWriter(WriteOptions writeOptions, ObjectStorage objectStorage, String path) {
        this(writeOptions, objectStorage, path, Writer.MIN_PART_SIZE);
    }

    @Override
    public CompletableFuture<Void> write(ByteBuf part) {
        if (multiPartObjectWriter != null) {
            return multiPartObjectWriter.write(part);
        }
        size += part.readableBytes();
        pendingOperations.add(new PendingWrite(part));
        if (exceedsMultipartUploadThreshold()) {
            newMultiPartObjectWriter();
        }
        return objectCf;
    }

    @Override
    public void copyOnWrite() {
        if (multiPartObjectWriter != null) {
            multiPartObjectWriter.copyOnWrite();
        } else {
            pendingOperations.forEach(PendingOperation::copyOnWrite);
        }
    }

    @Override
    public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
        if (multiPartObjectWriter != null) {
            multiPartObjectWriter.copyWrite(s3ObjectMetadata, start, end);
            return;
        }
        size += end - start;
        pendingOperations.add(new PendingCopyWrite(s3ObjectMetadata, start, end));
        if (exceedsMultipartUploadThreshold()) {
            newMultiPartObjectWriter();
        }
    }

    @Override
    public boolean hasBatchingPart() {
        if (multiPartObjectWriter != null) {
            return multiPartObjectWriter.hasBatchingPart();
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> close() {
        if (multiPartObjectWriter != null) {
            return multiPartObjectWriter.close();
        }
        return closeWithPutObject();
    }

    @Override
    public CompletableFuture<Void> release() {
        if (multiPartObjectWriter != null) {
            return multiPartObjectWriter.release();
        }
        pendingOperations.forEach(PendingOperation::release);
        pendingOperations.clear();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public short bucketId() {
        return writeOptions.bucketId();
    }

    private void newMultiPartObjectWriter() {
        this.multiPartObjectWriter = new MultiPartWriter(writeOptions, objectStorage, path, minPartSize);
        List<CompletableFuture<Void>> writeCfs = new ArrayList<>();
        for (PendingOperation operation : pendingOperations) {
            writeCfs.add(operation.apply(multiPartObjectWriter));
        }
        pendingOperations.clear();
        FutureUtil.propagate(CompletableFuture.allOf(writeCfs.toArray(new CompletableFuture[0])), objectCf);
    }

    private CompletableFuture<Void> closeWithPutObject() {
        CompositeByteBuf data = ByteBufAlloc.compositeByteBuffer();
        CompletableFuture<Void> replayCf = CompletableFuture.completedFuture(null);
        for (PendingOperation operation : pendingOperations) {
            replayCf = operation.apply(replayCf, data);
        }
        pendingOperations.clear();
        S3ObjectMetrics.recordReadyCloseStage(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        replayCf.whenComplete((nil, ex) -> {
            if (ex != null) {
                data.release();
                objectCf.completeExceptionally(ex);
            } else {
                FutureUtil.propagate(objectStorage.write(writeOptions, path, data).thenApply(rst -> null), objectCf);
            }
        });
        objectCf.whenComplete((nil, e) -> {
            S3ObjectMetrics.recordTotalStage(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            S3ObjectMetrics.recordObject();
        });
        return objectCf;
    }

    private boolean exceedsMultipartUploadThreshold() {
        return size > MULTIPART_UPLOAD_THRESHOLD;
    }

    private interface PendingOperation {
        CompletableFuture<Void> apply(Writer writer);

        CompletableFuture<Void> apply(CompletableFuture<Void> previousCf, CompositeByteBuf data);

        default void copyOnWrite() {
        }

        default void release() {
        }
    }

    private class PendingWrite implements PendingOperation {
        private ByteBuf part;

        private PendingWrite(ByteBuf part) {
            this.part = part;
        }

        @Override
        public CompletableFuture<Void> apply(Writer writer) {
            CompletableFuture<Void> cf = writer.write(part);
            part = null;
            return cf;
        }

        @Override
        public CompletableFuture<Void> apply(CompletableFuture<Void> previousCf, CompositeByteBuf data) {
            ByteBuf dataPart = part;
            part = null;
            return previousCf.handle((nil, ex) -> {
                if (ex != null) {
                    dataPart.release();
                    throw new CompletionException(ex);
                }
                data.addComponent(true, dataPart);
                return null;
            });
        }

        @Override
        public void copyOnWrite() {
            ByteBuf copy = ByteBufAlloc.byteBuffer(part.readableBytes(), writeOptions.allocType());
            copy.writeBytes(part.duplicate());
            part.release();
            part = copy;
        }

        @Override
        public void release() {
            if (part != null) {
                part.release();
                part = null;
            }
        }
    }

    private class PendingCopyWrite implements PendingOperation {
        private final S3ObjectMetadata s3ObjectMetadata;
        private final long start;
        private final long end;

        private PendingCopyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            this.s3ObjectMetadata = s3ObjectMetadata;
            this.start = start;
            this.end = end;
        }

        @Override
        public CompletableFuture<Void> apply(Writer writer) {
            try {
                writer.copyWrite(s3ObjectMetadata, start, end);
                return CompletableFuture.completedFuture(null);
            } catch (Throwable ex) {
                return CompletableFuture.failedFuture(ex);
            }
        }

        @Override
        public CompletableFuture<Void> apply(CompletableFuture<Void> previousCf, CompositeByteBuf data) {
            return previousCf
                .thenCompose(nil -> objectStorage.rangeRead(
                    new ObjectStorage.ReadOptions().throttleStrategy(writeOptions.throttleStrategy()).bucket(s3ObjectMetadata.bucket()),
                    s3ObjectMetadata.key(), start, end))
                .thenAccept(buf -> data.addComponent(true, buf));
        }
    }
}
