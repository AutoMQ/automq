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

package com.automq.stream.s3;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.stats.ByteBufStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectByteBufAlloc {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufAlloc.class);
    private static final PooledByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;

    public static CompositeByteBuf compositeByteBuffer() {
        return ALLOC.compositeDirectBuffer(Integer.MAX_VALUE);
    }

    public static ByteBuf byteBuffer(int initCapacity) {
        return byteBuffer(initCapacity, null);
    }

    public static ByteBuf byteBuffer(int initCapacity, String name) {
        try {
            if (name != null) {
                ByteBufStats.getInstance().allocateByteBufSizeStats(name).record(MetricsLevel.DEBUG, initCapacity);
            }
            return ALLOC.directBuffer(initCapacity);
        } catch (OutOfMemoryError e) {
            LOGGER.error("alloc direct buffer OOM", e);
            System.err.println("alloc direct buffer OOM");
            Runtime.getRuntime().halt(1);
            throw e;
        }
    }

    public interface OOMHandler {
        /**
         * Try handle OOM exception.
         *
         * @param memoryRequired the memory required
         * @return freed memory.
         */
        int handle(int memoryRequired);
    }
}
