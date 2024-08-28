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
import java.util.concurrent.atomic.AtomicReference;

public class ByteBufSeqAlloc {
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

    public ByteBuf byteBuffer(int capacity) {
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
