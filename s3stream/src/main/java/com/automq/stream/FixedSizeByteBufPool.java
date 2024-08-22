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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pool of fixed-size {@link ByteBuf}.
 */
public class FixedSizeByteBufPool {

    /**
     * The size of the {@link ByteBuf} in this pool.
     */
    private final int bufferSize;
    /**
     * The max size of the pool.
     * It is possible that the pool size exceeds this limit in some rare cases.
     */
    private final int maxPoolSize;
    private final Queue<ByteBuf> pool = new ConcurrentLinkedQueue<>();
    /**
     * The current size of the pool.
     * We use an {@link AtomicInteger} rather than {@link Queue#size()} to avoid the cost of traversing the queue.
     */
    private final AtomicInteger poolSize = new AtomicInteger(0);

    public FixedSizeByteBufPool(int bufferSize, int maxPoolSize) {
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Get a {@link ByteBuf} from the pool.
     * If the pool is empty, a new {@link ByteBuf} will be allocated.
     */
    public ByteBuf get() {
        ByteBuf buffer = pool.poll();
        if (buffer == null) {
            return allocate();
        }
        poolSize.decrementAndGet();
        return buffer;
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

        if (poolSize.get() >= maxPoolSize) {
            buffer.release();
            return;
        }

        buffer.clear();
        pool.offer(buffer);
        poolSize.incrementAndGet();
    }
}
