/*
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

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3ObjectStage;
import com.automq.stream.s3.metrics.stats.S3ObjectMetricsStats;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class MultiPartWriter implements Writer {
    private static final long MAX_MERGE_WRITE_SIZE = 16L * 1024 * 1024;
    private final S3Operator operator;
    private final String path;
    final CompletableFuture<String> uploadIdCf = new CompletableFuture<>();
    private String uploadId;
    private final List<CompletableFuture<CompletedPart>> parts = new LinkedList<>();
    private final AtomicInteger nextPartNumber = new AtomicInteger(1);
    private CompletableFuture<Void> closeCf;
    /**
     * The minPartSize represents the minimum size of a part for a multipart object.
     */
    private final long minPartSize;
    private ObjectPart objectPart = null;
    private final TimerUtil timerUtil = new TimerUtil();
    private final ThrottleStrategy throttleStrategy;
    private final AtomicLong totalWriteSize = new AtomicLong(0L);

    public MultiPartWriter(S3Operator operator, String path, long minPartSize, ThrottleStrategy throttleStrategy) {
        this.operator = operator;
        this.path = path;
        this.minPartSize = minPartSize;
        this.throttleStrategy = throttleStrategy;
        init();
    }

    private void init() {
        FutureUtil.propagate(
                operator.createMultipartUpload(path).thenApply(uploadId -> {
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
            objectPart = new ObjectPart(throttleStrategy);
        }
        ObjectPart objectPart = this.objectPart;

        objectPart.write(data);
        if (objectPart.size() > minPartSize) {
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
    public void copyWrite(String sourcePath, long start, long end) {
        long targetSize = end - start;
        if (objectPart == null) {
            if (targetSize < minPartSize) {
                this.objectPart = new ObjectPart(throttleStrategy);
                objectPart.readAndWrite(sourcePath, start, end);
            } else {
                new CopyObjectPart(sourcePath, start, end);
            }
        } else {
            if (objectPart.size() + targetSize > MAX_MERGE_WRITE_SIZE) {
                long readAndWriteCopyEnd = start + minPartSize - objectPart.size();
                objectPart.readAndWrite(sourcePath, start, readAndWriteCopyEnd);
                objectPart.upload();
                this.objectPart = null;
                new CopyObjectPart(sourcePath, readAndWriteCopyEnd, end);
            } else {
                objectPart.readAndWrite(sourcePath, start, end);
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

        S3ObjectMetricsStats.getHistogram(S3ObjectStage.READY_CLOSE).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
        closeCf = new CompletableFuture<>();
        CompletableFuture<Void> uploadDoneCf = uploadIdCf.thenCompose(uploadId -> CompletableFuture.allOf(parts.toArray(new CompletableFuture[0])));
        FutureUtil.propagate(uploadDoneCf.thenCompose(nil -> operator.completeMultipartUpload(path, uploadId, genCompleteParts())), closeCf);
        closeCf.whenComplete((nil, ex) -> {
            S3ObjectMetricsStats.getHistogram(S3ObjectStage.TOTAL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            S3ObjectMetricsStats.S3_OBJECT_COUNT.inc();
            S3ObjectMetricsStats.S3_OBJECT_UPLOAD_SIZE.update(totalWriteSize.get());
        });
        return closeCf;
    }

    private List<CompletedPart> genCompleteParts() {
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
        private CompositeByteBuf partBuf = DirectByteBufAlloc.compositeByteBuffer();
        private CompletableFuture<Void> lastRangeReadCf = CompletableFuture.completedFuture(null);
        private final CompletableFuture<CompletedPart> partCf = new CompletableFuture<>();
        private long size;
        private final ThrottleStrategy throttleStrategy;

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
                ByteBuf buf = DirectByteBufAlloc.byteBuffer(size);
                buf.writeBytes(partBuf.duplicate());
                CompositeByteBuf copy = DirectByteBufAlloc.compositeByteBuffer().addComponent(true, buf);
                this.partBuf.release();
                this.partBuf = copy;
            }
        }

        public void readAndWrite(String sourcePath, long start, long end) {
            size += end - start;
            // TODO: parallel read and sequence add.
            this.lastRangeReadCf = lastRangeReadCf
                    .thenCompose(nil -> operator.rangeRead(sourcePath, start, end, throttleStrategy))
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
            FutureUtil.propagate(uploadIdCf.thenCompose(uploadId -> operator.uploadPart(path, uploadId, partNumber, partBuf, throttleStrategy)), partCf);
            partCf.whenComplete((nil, ex) -> S3ObjectMetricsStats.getHistogram(S3ObjectStage.UPLOAD_PART).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS)));
        }

        public long size() {
            return size;
        }

        public CompletableFuture<Void> getFuture() {
            return partCf.thenApply(nil -> null);
        }
    }

    class CopyObjectPart {
        private final CompletableFuture<CompletedPart> partCf = new CompletableFuture<>();

        public CopyObjectPart(String sourcePath, long start, long end) {
            int partNumber = nextPartNumber.getAndIncrement();
            parts.add(partCf);
            FutureUtil.propagate(uploadIdCf.thenCompose(uploadId -> operator.uploadPartCopy(sourcePath, path, start, end, uploadId, partNumber)), partCf);
        }

        public CompletableFuture<Void> getFuture() {
            return partCf.thenApply(nil -> null);
        }
    }
}
