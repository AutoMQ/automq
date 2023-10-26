/*
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

package com.automq.stream.s3;

import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DirectByteBufAlloc {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufAlloc.class);
    private static final PooledByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;
    private static final List<OOMHandler> OOM_HANDLERS = new ArrayList<>();
    private static long lastLogTimestamp = 0L;

    public static CompositeByteBuf compositeByteBuffer() {
        return ALLOC.compositeDirectBuffer(Integer.MAX_VALUE);
    }

    public static ByteBuf byteBuffer(int initCapacity) {
        try {
            return ALLOC.directBuffer(initCapacity);
        } catch (OutOfMemoryError e) {
            for (;;) {
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
         * @param memoryRequired the memory required
         * @return freed memory.
         */
        int handle(int memoryRequired);
    }
}
