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

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.model.CompletedPart;

public class MemoryS3Operator implements S3Operator {
    private final Map<String, ByteBuf> storage = new ConcurrentHashMap<>();
    private long delay = 0;

    @Override
    public void close() {
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(String path, long start, long end, ThrottleStrategy throttleStrategy) {
        ByteBuf value = storage.get(path);
        if (value == null) {
            return FutureUtil.failedFuture(new IllegalArgumentException("object not exist"));
        }
        int length = (int) (end - start);
        ByteBuf rst = value.retainedSlice(value.readerIndex() + (int) start, length);
        if (delay == 0) {
            return CompletableFuture.completedFuture(rst);
        } else {
            CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
            CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS).execute(() -> cf.complete(rst));
            return cf;
        }
    }

    @Override
    public CompletableFuture<Void> write(String path, ByteBuf data, ThrottleStrategy throttleStrategy) {
        ByteBuf buf = Unpooled.buffer(data.readableBytes());
        buf.writeBytes(data.duplicate());
        storage.put(path, buf);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Writer writer(Writer.Context context, String path, ThrottleStrategy throttleStrategy) {
        ByteBuf buf = Unpooled.buffer();
        storage.put(path, buf);
        return new Writer() {
            @Override
            public CompletableFuture<Void> write(ByteBuf part) {
                buf.writeBytes(part);
                // Keep the same behavior as a real S3Operator
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
        };
    }

    @Override
    public CompletableFuture<List<Pair<String, Long>>> list(String prefix) {
        CompletableFuture<List<Pair<String, Long>>> future = new CompletableFuture<>();
        future.complete(storage.keySet().stream().filter(key -> key.startsWith(prefix)).map(key -> Pair.of(key, System.currentTimeMillis())).collect(Collectors.toList()));
        return future;
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        storage.remove(path);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<String>> delete(List<String> objectKeys) {
        objectKeys.forEach(storage::remove);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<String> createMultipartUpload(String path) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data,
        ThrottleStrategy throttleStrategy) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<CompletedPart> uploadPartCopy(String sourcePath, String path, long start, long end,
        String uploadId, int partNumber) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> completeMultipartUpload(String path, String uploadId, List<CompletedPart> parts) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    public ByteBuf get() {
        if (storage.size() != 1) {
            throw new IllegalStateException("expect only one object in storage");
        }
        return storage.values().iterator().next();
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }
}
