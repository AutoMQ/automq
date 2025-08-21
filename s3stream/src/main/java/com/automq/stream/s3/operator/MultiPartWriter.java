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
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.utils.FutureUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

public class MultiPartWriter implements Writer {
    private static final long MAX_MERGE_WRITE_SIZE = 32L * 1024 * 1024;
    final CompletableFuture<String> uploadIdCf = new CompletableFuture<>();
    private final AbstractObjectStorage operator;
    private final String path;
    private final ObjectStorage.WriteOptions writeOptions;
    private final List<CompletableFuture<AbstractObjectStorage.ObjectStorageCompletedPart>> parts = new LinkedList<>();
    private final AtomicInteger nextPartNumber = new AtomicInteger(1);
    /**
     * The minPartSize represents the minimum size of a part for a multipart object.
     */
    private final long minPartSize;
    private final long maxMergeWriteSize;
    private final TimerUtil timerUtil = new TimerUtil();
    private final AtomicLong totalWriteSize = new AtomicLong(0L);
    private String uploadId;
    private CompletableFuture<Void> closeCf;
    private ObjectPart objectPart = null;

    public MultiPartWriter(ObjectStorage.WriteOptions writeOptions, AbstractObjectStorage operator, String path,
        long minPartSize) {
        this(writeOptions, operator, path, minPartSize, MAX_MERGE_WRITE_SIZE);
    }

    public MultiPartWriter(ObjectStorage.WriteOptions writeOptions, AbstractObjectStorage operator, String path,
        long minPartSize, long maxMergeWriteSize) {
        this.writeOptions = writeOptions;
        this.operator = operator;
        this.path = path;
        this.minPartSize = minPartSize;
        this.maxMergeWriteSize = maxMergeWriteSize;
        init();
    }

    private void init() {
        FutureUtil.propagate(
            operator.createMultipartUpload(writeOptions, path).thenApply(uploadId -> {
                this.uploadId = uploadId;
                return uploadId;
            }),
            uploadIdCf
        );
    }

    @Override
    public CompletableFuture<Void> write(ByteBuf data) {
        totalWriteSize.addAndGet(data.readableBytes());

        if (objectPart == null) {
            objectPart = new ObjectPart(writeOptions.throttleStrategy());
        }
        ObjectPart objectPart = this.objectPart;

        objectPart.write(data);
        if (objectPart.size() > Math.max(minPartSize, maxMergeWriteSize)) {
            objectPart.upload();
            // finish current part.
            this.objectPart = null;
        }
        return objectPart.getFuture();
    }

    @Override
    public void copyOnWrite() {
        if (objectPart != null) {
            objectPart.copyOnWrite();
        }
    }

    @Override
    public boolean hasBatchingPart() {
        return objectPart != null;
    }

    @Override
    public void copyWrite(S3ObjectMetadata sourceObjectMateData, long start, long end) {
        long nextStart = start;
        for (; ; ) {
            long currentEnd = Math.min(nextStart + Writer.MAX_PART_SIZE, end);
            copyWrite0(sourceObjectMateData, nextStart, currentEnd);
            nextStart = currentEnd;
            if (currentEnd == end) {
                break;
            }
        }
    }

    public void copyWrite0(S3ObjectMetadata sourceObjectMateData, long start, long end) {
        long targetSize = end - start;
        if (objectPart == null) {
            if (targetSize < minPartSize) {
                this.objectPart = new ObjectPart(writeOptions.throttleStrategy());
                objectPart.readAndWrite(sourceObjectMateData, start, end);
            } else {
                new CopyObjectPart(sourceObjectMateData.key(), start, end);
            }
        } else {
            if (objectPart.size() + targetSize > maxMergeWriteSize) {
                long readAndWriteCopyEnd = start + minPartSize - objectPart.size();
                objectPart.readAndWrite(sourceObjectMateData, start, readAndWriteCopyEnd);
                objectPart.upload();
                this.objectPart = null;
                new CopyObjectPart(sourceObjectMateData.key(), readAndWriteCopyEnd, end);
            } else {
                objectPart.readAndWrite(sourceObjectMateData, start, end);
                if (objectPart.size() > minPartSize) {
                    objectPart.upload();
                    this.objectPart = null;
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        if (closeCf != null) {
            return closeCf;
        }

        if (objectPart != null) {
            // force upload the last part which can be smaller than minPartSize.
            objectPart.upload();
            objectPart = null;
        }

        S3ObjectStats.getInstance().objectStageReadyCloseStats.record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        closeCf = new CompletableFuture<>();
        CompletableFuture<Void> uploadDoneCf = uploadIdCf.thenCompose(uploadId -> CompletableFuture.allOf(parts.toArray(new CompletableFuture[0])));
        FutureUtil.propagate(uploadDoneCf.thenCompose(nil -> operator.completeMultipartUpload(writeOptions, path, uploadId, genCompleteParts())), closeCf);
        closeCf.whenComplete((nil, ex) -> {
            S3ObjectStats.getInstance().objectStageTotalStats.record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            S3ObjectStats.getInstance().objectNumInTotalStats.add(MetricsLevel.DEBUG, 1);
            S3ObjectStats.getInstance().objectUploadSizeStats.record(totalWriteSize.get());
        });
        return closeCf;
    }

    @Override
    public CompletableFuture<Void> release() {
        List<CompletableFuture<AbstractObjectStorage.ObjectStorageCompletedPart>> partsToWait = parts;
        if (objectPart != null) {
            // skip waiting for pending part
            partsToWait = partsToWait.subList(0, partsToWait.size() - 1);
        }
        // wait for all ongoing uploading parts to finish and release pending part
        return CompletableFuture.allOf(partsToWait.toArray(new CompletableFuture[0])).whenComplete((nil, ex) -> {
            if (objectPart != null) {
                objectPart.release();
            }
        });
    }

    @Override
    public short bucketId() {
        return writeOptions.bucketId();
    }

    private List<AbstractObjectStorage.ObjectStorageCompletedPart> genCompleteParts() {
        return this.parts.stream().map(cf -> {
            try {
                return cf.get();
            } catch (Throwable e) {
                // won't happen.
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    class ObjectPart {
        private final int partNumber = nextPartNumber.getAndIncrement();
        private final CompletableFuture<AbstractObjectStorage.ObjectStorageCompletedPart> partCf = new CompletableFuture<>();
        private final ThrottleStrategy throttleStrategy;
        private CompositeByteBuf partBuf = ByteBufAlloc.compositeByteBuffer();
        private CompletableFuture<Void> lastRangeReadCf = CompletableFuture.completedFuture(null);
        private long size;

        public ObjectPart(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            parts.add(partCf);
        }

        public void write(ByteBuf data) {
            size += data.readableBytes();
            // ensure addComponent happen before following write or copyWrite.
            this.lastRangeReadCf = lastRangeReadCf.thenAccept(nil -> partBuf.addComponent(true, data));
        }

        public void copyOnWrite() {
            int size = partBuf.readableBytes();
            if (size > 0) {
                ByteBuf buf = ByteBufAlloc.byteBuffer(size, writeOptions.allocType());
                buf.writeBytes(partBuf.duplicate());
                CompositeByteBuf copy = ByteBufAlloc.compositeByteBuffer().addComponent(true, buf);
                this.partBuf.release();
                this.partBuf = copy;
            }
        }

        public void readAndWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
            size += end - start;
            // TODO: parallel read and sequence add.
            this.lastRangeReadCf = lastRangeReadCf
                .thenCompose(nil ->
                    operator.rangeRead(
                        new ReadOptions().throttleStrategy(throttleStrategy).bucket(s3ObjectMetadata.bucket()),
                        s3ObjectMetadata.key(), start, end)
                )
                .thenAccept(buf -> partBuf.addComponent(true, buf));
        }

        public void upload() {
            this.lastRangeReadCf.whenComplete((nil, ex) -> {
                if (ex != null) {
                    partCf.completeExceptionally(ex);
                } else {
                    upload0();
                }
            });
        }

        private void upload0() {
            TimerUtil timerUtil = new TimerUtil();
            FutureUtil.propagate(uploadIdCf.thenCompose(uploadId -> operator.uploadPart(writeOptions, path, uploadId, partNumber, partBuf)), partCf);
            partCf.whenComplete((nil, ex) -> {
                S3ObjectStats.getInstance().objectStageUploadPartStats.record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            });
        }

        public long size() {
            return size;
        }

        public CompletableFuture<Void> getFuture() {
            return partCf.thenApply(nil -> null);
        }

        public void release() {
            partBuf.release();
        }
    }

    class CopyObjectPart {
        private final CompletableFuture<AbstractObjectStorage.ObjectStorageCompletedPart> partCf = new CompletableFuture<>();

        public CopyObjectPart(String sourcePath, long start, long end) {
            int partNumber = nextPartNumber.getAndIncrement();
            parts.add(partCf);
            FutureUtil.propagate(uploadIdCf.thenCompose(uploadId -> operator.uploadPartCopy(writeOptions, sourcePath, path, start, end, uploadId, partNumber)), partCf);
        }

        public CompletableFuture<Void> getFuture() {
            return partCf.thenApply(nil -> null);
        }
    }
}
