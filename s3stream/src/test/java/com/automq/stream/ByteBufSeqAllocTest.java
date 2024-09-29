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

import io.netty.buffer.ByteBuf;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ByteBufSeqAllocTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(ByteBufSeqAllocTest.class);

    @Test
    public void testAlloc() {
        ByteBufSeqAlloc alloc = new ByteBufSeqAlloc(0, 1);

        AtomicReference<ByteBufSeqAlloc.HugeBuf> bufRef = alloc.hugeBufArray[Math.abs(Thread.currentThread().hashCode() % alloc.hugeBufArray.length)];

        ByteBuf buf1 = alloc.byteBuffer(12);
        buf1.writeLong(1);
        buf1.writeInt(2);

        ByteBuf buf2 = alloc.byteBuffer(20);
        buf2.writeLong(3);
        buf2.writeInt(4);
        buf2.writeLong(5);

        ByteBuf buf3 = alloc.byteBuffer(ByteBufSeqAlloc.HUGE_BUF_SIZE - 12 - 20 - 4);

        ByteBuf oldHugeBuf = bufRef.get().buf;

        ByteBuf buf4 = alloc.byteBuffer(16);
        buf4.writeLong(6);
        buf4.writeLong(7);

        assertNotSame(oldHugeBuf, bufRef.get().buf);

        assertEquals(1, buf1.readLong());
        assertEquals(2, buf1.readInt());
        assertEquals(3, buf2.readLong());
        assertEquals(4, buf2.readInt());
        assertEquals(5, buf2.readLong());
        assertEquals(6, buf4.readLong());
        assertEquals(7, buf4.readLong());

        buf1.release();
        buf2.release();
        buf3.release();
        buf4.release();
        assertEquals(0, oldHugeBuf.refCnt());
        assertEquals(1, bufRef.get().buf.refCnt());

        ByteBuf oldHugeBuf2 = bufRef.get().buf;

        alloc.byteBuffer(ByteBufSeqAlloc.HUGE_BUF_SIZE - 12).release();
        alloc.byteBuffer(12).release();
        assertEquals(0, oldHugeBuf2.refCnt());
    }

    @Test
    public void testAllocCompare() throws InterruptedException {

        ByteBufSeqAllocV2 allocNew = new ByteBufSeqAllocV2(0, 8);
        ByteBufSeqAlloc alloc = new ByteBufSeqAlloc(0, 8);
        int rounds = 1000;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(50, 50, 3, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(rounds));
        CountDownLatch latch = new CountDownLatch(rounds);
        for (int i = 0; i < rounds; i++) {
            executor.execute(() -> {
                ByteBuf buf = null;
                ByteBuf newBuf = null;
                try {
                    int random = new Random().nextInt(100);
                    int capacity = 0;
                    if (random < 5) {
                        capacity = new Random().nextInt(5 << 20);
                    } else if (random < 15) {
                        capacity = new Random().nextInt(1 << 20);
                    } else if (random < 50) {
                        capacity = new Random().nextInt(5 << 10);
                    } else {
                        capacity = new Random().nextInt(2 << 10);
                    }

                    buf = alloc.byteBufferForTest(capacity);
                    newBuf = allocNew.byteBufferForTest(capacity);
                } finally {
                    if (buf != null) {
                        buf.release();
                    }
                    if (newBuf != null) {
                        newBuf.release();
                    }
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdownNow();
        alloc.releaseForTest();
        allocNew.releaseForTest();

        double oldAvg = alloc.getNoUsage().stream().mapToInt(Integer::intValue).average().orElse(0);
        double newAvg = allocNew.getNoUsage().stream().mapToInt(Integer::intValue).average().orElse(0);

        long oldSum = alloc.getNoUsage().stream().mapToInt(Integer::intValue).sum();
        long newSum = allocNew.getNoUsage().stream().mapToInt(Integer::intValue).sum();
        double oldCostTimeAvg = alloc.getCostTimes().stream().mapToLong(Long::longValue).average().orElse(0D);
        double newCostTimeAvg = allocNew.getCostTimes().stream().mapToLong(Long::longValue).average().orElse(0D);
        LOGGER.info("oldSize: {}, newSize: {}", alloc.getNoUsage().size(), allocNew.getNoUsage().size());
        LOGGER.info("oldSum: {}, newSum: {}", oldSum, newSum);
        LOGGER.info("oldAvg: {}, newAvg: {}", oldAvg, newAvg);
        LOGGER.info("oldCostTimeAvg: {}, newCostTimeAvg: {}", oldCostTimeAvg, newCostTimeAvg);
    }
}
