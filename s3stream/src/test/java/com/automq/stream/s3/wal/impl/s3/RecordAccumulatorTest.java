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

package com.automq.stream.s3.wal.impl.s3;

import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RecordAccumulatorTest {
    private RecordAccumulator recordAccumulator;
    private ObjectStorage objectStorage;
    private ConcurrentSkipListMap<Long, ByteBuf> generatedByteBufMap;
    private Random random;

    @BeforeEach
    public void setUp() {
        objectStorage = new MemoryObjectStorage();
        recordAccumulator = new RecordAccumulator(objectStorage);
        generatedByteBufMap = new ConcurrentSkipListMap<>();
        random = new Random();
    }

    private ByteBuf generateByteBuf(int size) {
        ByteBuf byteBuf = Unpooled.buffer(size);
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    @Test
    public void testOffset() throws OverCapacityException {
        ByteBuf byteBuf = generateByteBuf(10);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future);
        assertEquals(10, recordAccumulator.nextOffset());

        recordAccumulator.upload();
        long flushedOffset = future.join().flushedOffset();
        assertEquals(9, flushedOffset);
        assertEquals(9, recordAccumulator.flushedOffset());

        List<RecordAccumulator.WALObject> objectList = recordAccumulator.objectList();
        assertEquals(1, objectList.size());

        RecordAccumulator.WALObject object = objectList.get(0);
        ByteBuf result = objectStorage.rangeRead(ObjectStorage.ReadOptions.DEFAULT, object.path(), 0, object.length()).join();
        assertEquals(byteBuf, result);
        byteBuf.release();
    }

    @Test
    public void testInMultiThread() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch startBarrier = new CountDownLatch(threadCount);
        CountDownLatch stopCountDownLatch = new CountDownLatch(threadCount);
        List<CompletableFuture<AppendResult.CallbackResult>> futureList = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                startBarrier.countDown();
                try {
                    startBarrier.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                for (int j = 0; j < 100; j++) {
                    ByteBuf byteBuf = generateByteBuf(25);
                    try {
                        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
                        long offset = recordAccumulator.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);
                        futureList.add(future);
                        generatedByteBufMap.put(offset, byteBuf);
                    } catch (OverCapacityException e) {
                        byteBuf.release();
                        throw new RuntimeException(e);
                    }

                    if (random.nextInt(100) < 10) {
                        try {
                            recordAccumulator.upload();
                        } catch (OverCapacityException ignore) {
                        }
                    }
                }

                stopCountDownLatch.countDown();
            }).start();
        }

        stopCountDownLatch.await();

        // Ensure all records are uploaded.
        while (true) {
            try {
                recordAccumulator.upload();
                break;
            } catch (OverCapacityException ignore) {
                Thread.sleep(100);
            }
        }

        for (CompletableFuture<AppendResult.CallbackResult> future : futureList) {
            future.join();
        }

        assertEquals(100 * threadCount, generatedByteBufMap.size());

        assertFalse(recordAccumulator.objectList().isEmpty());

        CompositeByteBuf source = Unpooled.compositeBuffer();
        for (ByteBuf buffer : generatedByteBufMap.values()) {
            source.addComponent(true, buffer);
        }

        CompositeByteBuf result = Unpooled.compositeBuffer();
        for (RecordAccumulator.WALObject object : recordAccumulator.objectList()) {
            ByteBuf buf = objectStorage.rangeRead(ObjectStorage.ReadOptions.DEFAULT, object.path(), 0, object.length()).join();
            result.addComponent(true, buf);
        }

        assertEquals(source, result);
        source.release();
        result.release();
    }
}
