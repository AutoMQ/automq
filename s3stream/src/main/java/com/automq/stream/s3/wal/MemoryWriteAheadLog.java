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

package com.automq.stream.s3.wal;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryWriteAheadLog implements WriteAheadLog {
    private final AtomicLong offsetAlloc = new AtomicLong();

    @Override
    public WriteAheadLog start() throws IOException {
        return this;
    }

    @Override
    public void shutdownGracefully() {

    }

    @Override
    public AppendResult append(ByteBuf data, int crc) {
        long offset = offsetAlloc.getAndIncrement();
        return new AppendResult() {
            @Override
            public long recordOffset() {
                return offset;
            }

            @Override
            public CompletableFuture<CallbackResult> future() {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    @Override
    public Iterator<RecoverResult> recover() {
        List<RecoverResult> l = Collections.emptyList();
        return l.iterator();
    }

    @Override
    public CompletableFuture<Void> reset() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        return CompletableFuture.completedFuture(null);
    }
}
