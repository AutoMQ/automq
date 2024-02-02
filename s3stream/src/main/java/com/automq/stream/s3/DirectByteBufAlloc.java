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
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectByteBufAlloc {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufAlloc.class);
    private static final PooledByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;
    private static final List<OOMHandler> OOM_HANDLERS = new ArrayList<>();
    private static long lastLogTimestamp = 0L;

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
            for (; ; ) {
                int freedBytes = 0;
                for (OOMHandler handler : OOM_HANDLERS) {
                    freedBytes += handler.handle(initCapacity);
                    try {
                        ByteBuf buf = ALLOC.directBuffer(initCapacity);
                        LOGGER.warn("OOM recovered, freed {} bytes", freedBytes);
                        return buf;
                    } catch (OutOfMemoryError e2) {
                        // ignore
                    }
                }
                if (System.currentTimeMillis() - lastLogTimestamp >= 1000L) {
                    LOGGER.error("try recover from OOM fail, freedBytes={}, retry later", freedBytes);
                    lastLogTimestamp = System.currentTimeMillis();
                }
                Threads.sleep(1L);
            }
        }
    }

    public static void registerOOMHandlers(OOMHandler handler) {
        OOM_HANDLERS.add(handler);
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
