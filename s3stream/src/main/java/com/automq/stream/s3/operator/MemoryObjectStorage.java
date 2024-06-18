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
    boolean isUnrecoverable(Throwable ex) {
        return !(ex instanceof IllegalArgumentException);
    }

    @Override
    void  doClose() {
        storage.clear();
    }
}
