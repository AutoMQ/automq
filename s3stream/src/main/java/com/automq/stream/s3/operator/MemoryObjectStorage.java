/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MemoryObjectStorage extends AbstractObjectStorage {
    private final Map<String, ByteBuf> storage = new ConcurrentHashMap<>();
    private long delay = 0;
    private final short bucketId;

    public MemoryObjectStorage(boolean manualMergeRead, short bucketId) {
        super(BucketURI.parse(bucketId + "@s3://b"), NetworkBandwidthLimiter.NOOP, NetworkBandwidthLimiter.NOOP, 50, 0, true, false, manualMergeRead);
        this.bucketId = bucketId;
    }

    public MemoryObjectStorage(short bucketId) {
        this(false, bucketId);
    }

    public MemoryObjectStorage() {
        this(false, (short) 0);
    }

    public MemoryObjectStorage(boolean manualMergeRead) {
        this(manualMergeRead, (short) 0);
    }

    @Override
    CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
        ByteBuf value = storage.get(path);
        if (value == null) {
            return FutureUtil.failedFuture(new IllegalArgumentException("object not exist"));
        }
        int length = end != -1L ? (int) (end - start) : (int) (value.readableBytes() - start);
        ByteBuf rst = value.retainedSlice(value.readerIndex() + (int) start, length);
        CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
        buf.addComponent(true, rst);
        if (delay == 0) {
            return CompletableFuture.completedFuture(buf);
        } else {
            CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
            Threads.COMMON_SCHEDULER.schedule(() -> cf.complete(buf), delay, TimeUnit.MILLISECONDS);
            return cf;
        }
    }

    @Override
    CompletableFuture<Void> doWrite(WriteOptions options, String path, ByteBuf data) {
        if (data == null) {
            return FutureUtil.failedFuture(new IllegalArgumentException("data to write cannot be null"));
        }
        ByteBuf buf = Unpooled.buffer(data.readableBytes());
        buf.writeBytes(data.duplicate());
        storage.put(path, buf);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Writer writer(WriteOptions writeOptions, String path) {
        ByteBuf buf = Unpooled.buffer();
        storage.put(path, buf);
        return new Writer() {
            @Override
            public CompletableFuture<Void> write(ByteBuf part) {
                buf.writeBytes(part);
                // Release the part after write
                part.release();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void copyOnWrite() {

            }

            @Override
            public boolean hasBatchingPart() {
                return false;
            }

            @Override
            public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
                ByteBuf source = storage.get(s3ObjectMetadata.key());
                if (source == null) {
                    throw new IllegalArgumentException("object not exist");
                }
                buf.writeBytes(source.slice(source.readerIndex() + (int) start, (int) (end - start)));
            }

            @Override
            public CompletableFuture<Void> close() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> release() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public short bucketId() {
                return bucketId;
            }
        };
    }

    @Override
    CompletableFuture<String> doCreateMultipartUpload(WriteOptions options, String path) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf part) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPartCopy(WriteOptions options, String sourcePath, String path,
        long start, long end, String uploadId, int partNumber) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<Void> doCompleteMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        objectKeys.forEach(storage::remove);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    RetryStrategy toRetryStrategy(Throwable ex, S3Operation operation) {
        Throwable cause = FutureUtil.cause(ex);
        return cause instanceof UnsupportedOperationException || cause instanceof IllegalArgumentException
            ? RetryStrategy.ABORT : RetryStrategy.RETRY;
    }

    @Override
    void doClose() {
        storage.clear();
    }

    @Override
    CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        return CompletableFuture.completedFuture(storage.entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(prefix))
            .map(entry -> new ObjectInfo((short) 0, entry.getKey(), 0L, entry.getValue().readableBytes()))
            .collect(Collectors.toList()));
    }

    @Override
    protected <T> boolean bucketCheck(int bucketId, CompletableFuture<T> cf) {
        return true;
    }

    public ByteBuf get() {
        if (storage.size() != 1) {
            throw new IllegalStateException("expect only one object in storage");
        }
        return storage.values().iterator().next();
    }

    public boolean contains(String path) {
        return storage.containsKey(path);
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }
}
