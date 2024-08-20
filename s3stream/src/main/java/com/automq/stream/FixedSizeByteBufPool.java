/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream;

import com.automq.stream.s3.ByteBufAlloc;
import io.netty.buffer.ByteBuf;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A pool of fixed-size {@link ByteBuf}.
 * Note: For performance reasons, there is no size limit for this pool. Callers should ensure the pool size is reasonable.
 */
public class FixedSizeByteBufPool {

    /**
     * The size of the {@link ByteBuf} in this pool.
     */
    private final int bufferSize;
    private final Queue<ByteBuf> pool = new ConcurrentLinkedQueue<>();

    public FixedSizeByteBufPool(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * Get a {@link ByteBuf} from the pool.
     * If the pool is empty, a new {@link ByteBuf} will be allocated.
     */
    public ByteBuf get() {
        ByteBuf buffer = pool.poll();
        return buffer == null ? allocate() : buffer;
    }

    private ByteBuf allocate() {
        return ByteBufAlloc.byteBuffer(bufferSize);
    }

    /**
     * Release a {@link ByteBuf} to the pool.
     * Note: the buffer MUST be gotten from this pool.
     */
    public void release(ByteBuf buffer) {
        assert buffer.capacity() == bufferSize;
        buffer.clear();
        pool.offer(buffer);
    }
}
