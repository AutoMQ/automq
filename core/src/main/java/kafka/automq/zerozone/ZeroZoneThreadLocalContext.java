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

package kafka.automq.zerozone;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.util.concurrent.FastThreadLocal;

public class ZeroZoneThreadLocalContext {

    private static final FastThreadLocal<WriteContext> WRITE_CONTEXT = new FastThreadLocal<>() {
        @Override protected WriteContext initialValue() {
            return new WriteContext();
        }
    };

    public static WriteContext writeContext() {
        return WRITE_CONTEXT.get();
    }

    static void newWriteContext(ChannelOffset channelOffset, AtomicReference<CompletableFuture<?>> appendFuture) {
        WRITE_CONTEXT.set(new WriteContext(channelOffset, appendFuture));
    }

    public static void attach(WriteContext writeContext) {
        WRITE_CONTEXT.set(writeContext);
    }


    public static class WriteContext {
        private final ChannelOffset channelOffset;
        private final AtomicReference<CompletableFuture<?>> appendFuture;

        private WriteContext() {
            this(null, null);
        }

        private WriteContext(ChannelOffset channelOffset, AtomicReference<CompletableFuture<?>> appendFuture) {
            this.channelOffset = channelOffset;
            this.appendFuture = appendFuture;
        }

        /** Registers the exact storage append on this context, including after it has been detached. */
        public void trackAppend(CompletableFuture<?> append) {
            if (appendFuture != null) {
                appendFuture.set(append);
            }
        }

        public ChannelOffset channelOffset() {
            return channelOffset;
        }

        /** Consumes the current thread's context; the returned object remains usable by its owner. */
        public WriteContext detach() {
            WRITE_CONTEXT.set(new WriteContext());
            return this;
        }
    }


}
