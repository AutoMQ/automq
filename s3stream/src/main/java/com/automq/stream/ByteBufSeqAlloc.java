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
import com.automq.stream.s3.ByteBufSupplier;

import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;

public class ByteBufSeqAlloc implements ByteBufSupplier {
    public static final int HUGE_BUF_SIZE = ByteBufAlloc.getChunkSize().orElse(4 << 20);
    // why not use ThreadLocal? the partition open has too much threads
    final AtomicReference<HugeBuf>[] hugeBufArray;
    private final int allocType;

    @SuppressWarnings("unchecked")
    public ByteBufSeqAlloc(int allocType, int concurrency) {
        this.allocType = allocType;
        hugeBufArray = new AtomicReference[concurrency];
        for (int i = 0; i < hugeBufArray.length; i++) {
            hugeBufArray[i] = new AtomicReference<>(new HugeBuf(ByteBufAlloc.byteBuffer(HUGE_BUF_SIZE, allocType)));
        }
    }

    @Override
    public ByteBuf alloc(int capacity) {
        if (capacity > HUGE_BUF_SIZE) {
            // if the request capacity is larger than HUGE_BUF_SIZE, just allocate a new ByteBuf
            return ByteBufAlloc.byteBuffer(capacity, allocType);
        }
        int bufIndex = Math.abs(Thread.currentThread().hashCode() % hugeBufArray.length);

        AtomicReference<HugeBuf> bufRef = hugeBufArray[bufIndex];
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (bufRef) {
            HugeBuf hugeBuf = bufRef.get();

            if (hugeBuf.satisfies(capacity)) {
                return hugeBuf.byteBuffer(capacity);
            }

            // if the request capacity cannot be satisfied by the current hugeBuf, allocate it in a new hugeBuf
            hugeBuf.buf.release();
            HugeBuf newHugeBuf = new HugeBuf(ByteBufAlloc.byteBuffer(HUGE_BUF_SIZE, allocType));
            bufRef.set(newHugeBuf);

            // As the request capacity is not larger than HUGE_BUF_SIZE, the new hugeBuf will satisfy the request
            assert newHugeBuf.satisfies(capacity);
            return newHugeBuf.byteBuffer(capacity);
        }
    }

    static class HugeBuf {
        final ByteBuf buf;
        int nextIndex;

        HugeBuf(ByteBuf buf) {
            this.buf = buf;
            this.nextIndex = 0;
        }

        ByteBuf byteBuffer(int capacity) {
            int start = nextIndex;
            nextIndex += capacity;
            ByteBuf slice = buf.retainedSlice(start, capacity);
            slice.writerIndex(slice.readerIndex());
            return slice;
        }

        boolean satisfies(int capacity) {
            return nextIndex + capacity <= buf.capacity();
        }
    }

}
