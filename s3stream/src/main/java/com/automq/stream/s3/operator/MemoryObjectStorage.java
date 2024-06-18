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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class MemoryObjectStorage extends AbstractObjectStorage {
    private final Map<String, ByteBuf> storage = new ConcurrentHashMap<>();

    public MemoryObjectStorage(boolean manualMergeRead) {
        super(manualMergeRead);
    }

    @Override
    void doRangeRead(String path, long start, long end, Consumer<Throwable> failHandler, Consumer<CompositeByteBuf> successHandler) {
        ByteBuf value = storage.get(path);
        if (value == null) {
            failHandler.accept(new IllegalArgumentException("object not exist"));
            return;
        }
        int length = (int) (end - start);
        ByteBuf rst = value.retainedSlice(value.readerIndex() + (int) start, length);
        CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
        buf.addComponent(rst);
        successHandler.accept(buf);
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
    void doUploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber, long apiCallAttemptTimeout, Consumer<Throwable> failHandler, Consumer<ObjectStorageCompletedPart> successHandler) {
        failHandler.accept(new UnsupportedOperationException());
    }

    @Override
    void doCompleteMultipartUpload(String path, String uploadId, List<ObjectStorageCompletedPart> parts,
        Consumer<Throwable> failHandler, Runnable successHandler) {
        failHandler.accept(new UnsupportedOperationException());
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
}
