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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * If object data size is less than ObjectWriter.MAX_UPLOAD_SIZE, we should use single upload to upload it.
 * Else, we should use multi-part upload to upload it.
 */
public class ProxyWriter implements Writer {
    final ObjectWriter objectWriter = new ObjectWriter();
    private final WriteOptions writeOptions;
    private final ObjectStorage objectStorage;
    private final String path;
    private final long minPartSize;
    Writer largeObjectWriter = null;
    // Ordered chain that serializes every operation issued to largeObjectWriter once we escalate. It is
    // only touched by the (single) calling thread; see newLargeObjectWriter for why it exists.
    private CompletableFuture<Void> mpwChain = null;

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
        if (largeObjectWriter != null) {
            return writeToLargeObject(part);
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
        if (largeObjectWriter != null) {
            copyWriteToLargeObject(s3ObjectMetadata, start, end);
            return;
        }
        if (end - start >= minPartSize) {
            // Large enough to benefit from a zero-download server-side UploadPartCopy, so it's worth paying
            // the multipart upload overhead (createMultipartUpload/completeMultipartUpload).
            newLargeObjectWriter(writeOptions, objectStorage, path);
            copyWriteToLargeObject(s3ObjectMetadata, start, end);
            return;
        }
        // Below minPartSize, S3 can't do a server-side UploadPartCopy for it anyway, so just buffer the range
        // read like write() does and let it ride the single PutObject path instead of forcing multipart.
        objectWriter.copyWrite(s3ObjectMetadata, start, end);
        if (objectWriter.isFull()) {
            newLargeObjectWriter(writeOptions, objectStorage, path);
        }
    }

    /**
     * Issue a copyWrite to largeObjectWriter in order, after the buffered-data handoff and any previously
     * issued operations. Never call largeObjectWriter.copyWrite directly - it must go through mpwChain so
     * parts are appended in call order and MultiPartWriter is only ever touched by a single thread.
     */
    private void copyWriteToLargeObject(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
        mpwChain = mpwChain.thenRun(() -> largeObjectWriter.copyWrite(s3ObjectMetadata, start, end));
    }

    /**
     * Issue a write to largeObjectWriter in order (see {@link #copyWriteToLargeObject}). The returned future
     * tracks the actual part upload; if an earlier operation in the chain failed we surface that failure and
     * release the part so it is not leaked.
     */
    private CompletableFuture<Void> writeToLargeObject(ByteBuf part) {
        CompletableFuture<Void> writeCf = new CompletableFuture<>();
        mpwChain = mpwChain.thenRun(() -> FutureUtil.propagate(largeObjectWriter.write(part), writeCf));
        mpwChain.whenComplete((nil, ex) -> {
            if (ex != null && !writeCf.isDone()) {
                part.release();
                writeCf.completeExceptionally(ex);
            }
        });
        return writeCf;
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
            // Wait until the handoff and every issued copyWrite/write has actually been submitted to
            // largeObjectWriter (registered as a part) before closing, otherwise close could complete the
            // multipart upload before the buffered data lands - dropping it.
            return mpwChain.thenCompose(nil -> largeObjectWriter.close());
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

    protected void newLargeObjectWriter(WriteOptions writeOptions, ObjectStorage objectStorage, String path) {
        this.largeObjectWriter = new MultiPartWriter(writeOptions, objectStorage, path, minPartSize);
        // Hand off everything already buffered in objectWriter as the first multipart operation, then serialize
        // every subsequent largeObjectWriter call onto this chain (see copyWriteToLargeObject/writeToLargeObject).
        // This guarantees two things:
        //   (a) parts are issued in the exact order copyWrite/write were called. Byte order in the final object
        //       must match, since the compaction index block encodes absolute byte offsets into the object.
        //   (b) MultiPartWriter, which assumes a single-threaded caller, is only ever touched by one thread at a
        //       time - even though the handoff below waits on range reads that complete on a different thread.
        // whenComplete keeps mpwChain mirroring lastOpCf, so a failed range read short-circuits the whole chain.
        this.mpwChain = objectWriter.lastOpCf.whenComplete((nil, ex) -> {
            if (ex != null) {
                objectWriter.data.release();
                objectWriter.cf.completeExceptionally(ex);
            } else if (objectWriter.data.readableBytes() > 0) {
                FutureUtil.propagate(largeObjectWriter.write(objectWriter.data), objectWriter.cf);
            } else {
                objectWriter.data.release();
                objectWriter.cf.complete(null);
            }
        });
    }

    class ObjectWriter implements Writer {
        // max upload size, when object data size is larger than MAX_UPLOAD_SIZE, we should use multi-part upload to upload it.
        static final long MAX_UPLOAD_SIZE = 32L * 1024 * 1024;
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompositeByteBuf data = ByteBufAlloc.compositeByteBuffer();
        TimerUtil timerUtil = new TimerUtil();
        // tracks the size synchronously so isFull() is accurate even while copyWrite's range reads are in flight.
        private long size = 0;
        private CompletableFuture<Void> lastOpCf = CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<Void> write(ByteBuf part) {
            size += part.readableBytes();
            lastOpCf = lastOpCf.thenAccept(nil -> data.addComponent(true, part));
            return cf;
        }

        @Override
        public void copyOnWrite() {
            int readable = data.readableBytes();
            if (readable > 0) {
                ByteBuf buf = ByteBufAlloc.byteBuffer(readable, writeOptions.allocType());
                buf.writeBytes(data.duplicate());
                CompositeByteBuf copy = ByteBufAlloc.compositeByteBuffer().addComponent(true, buf);
                this.data.release();
                this.data = copy;
            }
        }

        @Override
        public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            size += end - start;
            lastOpCf = lastOpCf
                .thenCompose(nil -> objectStorage.rangeRead(
                    new ObjectStorage.ReadOptions().throttleStrategy(writeOptions.throttleStrategy()).bucket(s3ObjectMetadata.bucket()),
                    s3ObjectMetadata.key(), start, end))
                .thenAccept(buf -> data.addComponent(true, buf));
        }

        @Override
        public boolean hasBatchingPart() {
            return true;
        }

        @Override
        public CompletableFuture<Void> close() {
            S3ObjectMetrics.recordReadyCloseStage(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            // wait for any in-flight copyWrite range reads to land in data before issuing the single PutObject.
            lastOpCf.whenComplete((nil, ex) -> {
                if (ex != null) {
                    // A range read failed, so we'll never issue the PutObject that would consume data - release
                    // the partially accumulated buffer instead of leaking it.
                    data.release();
                    cf.completeExceptionally(ex);
                } else {
                    FutureUtil.propagate(objectStorage.write(writeOptions, path, data).thenApply(rst -> null), cf);
                }
            });
            cf.whenComplete((nil, e) -> {
                S3ObjectMetrics.recordTotalStage(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                S3ObjectMetrics.recordObject();
            });
            return cf;
        }

        @Override
        public CompletableFuture<Void> release() {
            data.release();
            return CompletableFuture.completedFuture(null);
        }

        public boolean isFull() {
            return size > MAX_UPLOAD_SIZE;
        }

        @Override
        public short bucketId() {
            return writeOptions.bucketId();
        }
    }
}
