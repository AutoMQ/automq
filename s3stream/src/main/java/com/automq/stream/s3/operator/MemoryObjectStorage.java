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
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MemoryObjectStorage extends AbstractObjectStorage {
    private final Map<String, ByteBuf> storage = new ConcurrentHashMap<>();
    private long delay = 0;

    public MemoryObjectStorage(boolean manualMergeRead) {
        super(manualMergeRead);
    }

    public MemoryObjectStorage() {
        this(false);
    }

    @Override
    void doRangeRead(String path, long start, long end, Consumer<Throwable> failHandler,
        Consumer<CompositeByteBuf> successHandler) {
        ByteBuf value = storage.get(path);
        if (value == null) {
            failHandler.accept(new IllegalArgumentException("object not exist"));
            return;
        }
        int length = end != -1L ? (int) (end - start) : (int) (value.readableBytes() - start);
        ByteBuf rst = value.retainedSlice(value.readerIndex() + (int) start, length);
        CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
        buf.addComponent(true, rst);
        if (delay == 0) {
            successHandler.accept(buf);
        } else {
            Threads.COMMON_SCHEDULER.schedule(() -> successHandler.accept(buf), delay, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    void doWrite(String path, ByteBuf data, Consumer<Throwable> failHandler, Runnable successHandler) {
        try {
            if (data == null) {
                failHandler.accept(new IllegalArgumentException("data to write cannot be null"));
                return;
            }
            ByteBuf buf = Unpooled.buffer(data.readableBytes());
            buf.writeBytes(data.duplicate());
            storage.put(path, buf);
            successHandler.run();
        } catch (Exception ex) {
            failHandler.accept(ex);
        }
    }

    @Override
    public Writer writer(WriteOptions writeOptions, String path) {
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
    void doCreateMultipartUpload(String path,
        Consumer<Throwable> failHandler, Consumer<String> successHandler) {
        failHandler.accept(new UnsupportedOperationException());
    }

    @Override
    void doUploadPart(String path, String uploadId, int partNumber, ByteBuf part,
        Consumer<Throwable> failHandler, Consumer<ObjectStorageCompletedPart> successHandler) {
        failHandler.accept(new UnsupportedOperationException());
    }

    @Override
    void doUploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber,
        long apiCallAttemptTimeout, Consumer<Throwable> failHandler,
        Consumer<ObjectStorageCompletedPart> successHandler) {
        failHandler.accept(new UnsupportedOperationException());
    }

    @Override
    void doCompleteMultipartUpload(String path, String uploadId, List<ObjectStorageCompletedPart> parts,
        Consumer<Throwable> failHandler, Runnable successHandler) {
        failHandler.accept(new UnsupportedOperationException());
    }

    @Override
    void doDeleteObjects(List<String> objectKeys, Consumer<Throwable> failHandler, Runnable successHandler) {
        objectKeys.forEach(storage::remove);
    }

    @Override
    boolean isUnrecoverable(Throwable ex) {
        if (ex instanceof UnsupportedOperationException) {
            return true;
        }
        if (ex instanceof IllegalArgumentException) {
            return true;
        }
        return false;
    }

    @Override
    void doClose() {
        storage.clear();
    }

    @Override
    CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        return CompletableFuture.completedFuture(storage.keySet().stream().filter(key -> key.startsWith(prefix)).map(s -> new ObjectInfo((short) 0, s, 0L)).collect(Collectors.toList()));
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
