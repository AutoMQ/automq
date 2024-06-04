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

package com.automq.stream;

import com.automq.stream.s3.ByteBufAlloc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.util.concurrent.atomic.AtomicReference;

public class ByteBufSeqAlloc {
    public static final int HUGE_BUF_SIZE = ByteBufAlloc.getChunkSize() > 0 ? ByteBufAlloc.getChunkSize() : 4 >> 20;
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
        if (capacity >= HUGE_BUF_SIZE) {
            // if the request capacity is larger than HUGE_BUF_SIZE, just allocate a new ByteBuf
            return ByteBufAlloc.byteBuffer(capacity, allocType);
        }
        int bufIndex = Math.abs(Thread.currentThread().hashCode() % hugeBufArray.length);

        AtomicReference<HugeBuf> bufRef = hugeBufArray[bufIndex];
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (bufRef) {
            HugeBuf hugeBuf = bufRef.get();

            if (hugeBuf.nextIndex + capacity <= hugeBuf.buf.capacity()) {
                // if the request capacity can be satisfied by the current hugeBuf, return a slice of it
                int nextIndex = hugeBuf.nextIndex;
                hugeBuf.nextIndex += capacity;
                ByteBuf slice = hugeBuf.buf.retainedSlice(nextIndex, capacity);
                return slice.writerIndex(slice.readerIndex());
            }

            // if the request capacity cannot be satisfied by the current hugeBuf
            // 1. slice the remaining of the current hugeBuf and release the hugeBuf
            // 2. create a new hugeBuf and slice the remaining of the required capacity
            // 3. return the composite ByteBuf of the two slices
            CompositeByteBuf cbf = ByteBufAlloc.compositeByteBuffer();
            int readLength = hugeBuf.buf.capacity() - hugeBuf.nextIndex;
            cbf.addComponent(false, hugeBuf.buf.retainedSlice(hugeBuf.nextIndex, readLength));
            capacity -= readLength;
            hugeBuf.buf.release();

            HugeBuf newHugeBuf = new HugeBuf(ByteBufAlloc.byteBuffer(HUGE_BUF_SIZE, allocType));
            bufRef.set(newHugeBuf);

            cbf.addComponent(false, newHugeBuf.buf.retainedSlice(0, capacity));
            newHugeBuf.nextIndex = capacity;

            return cbf;
        }
    }

    static class HugeBuf {
        final ByteBuf buf;
        int nextIndex;

        HugeBuf(ByteBuf buf) {
            this.buf = buf;
            this.nextIndex = 0;
        }
    }

}
