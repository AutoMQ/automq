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

package com.automq.stream;

import com.automq.stream.s3.ByteBufAlloc;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

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
        assert buffer.refCnt() == 1;

        if (poolSize.get() >= maxPoolSize) {
            buffer.release();
            return;
        }

        buffer.clear();
        pool.offer(buffer);
        poolSize.incrementAndGet();
    }
}
