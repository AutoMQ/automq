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
import com.automq.stream.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RecordAccumulatorTest {
    private RecordAccumulator recordAccumulatorExt;
    private ObjectStorage objectStorage;
    private ConcurrentSkipListMap<Long, ByteBuf> generatedByteBufMap;
    private Random random;

    @BeforeEach
    public void setUp() {
        objectStorage = new MemoryObjectStorage();
        recordAccumulatorExt = new RecordAccumulator(Time.SYSTEM, objectStorage, ObjectStorageWALConfig.builder().build());
        recordAccumulatorExt.start();
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
        recordAccumulatorExt.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future);
        assertEquals(10, recordAccumulatorExt.nextOffset());

        recordAccumulatorExt.unsafeUpload(true);
        long flushedOffset = future.join().flushedOffset();
        assertEquals(10, flushedOffset);
        assertEquals(10, recordAccumulatorExt.flushedOffset());

        List<RecordAccumulator.WALObject> objectList = recordAccumulatorExt.objectList();
        assertEquals(1, objectList.size());

        RecordAccumulator.WALObject object = objectList.get(0);
        ByteBuf result = objectStorage.rangeRead(ObjectStorage.ReadOptions.DEFAULT, object.path(), 0, object.length()).join();
        result.skipBytes(WALObjectHeader.WAL_HEADER_SIZE);
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
                        long offset = recordAccumulatorExt.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);
                        futureList.add(future);
                        generatedByteBufMap.put(offset, byteBuf);
                    } catch (OverCapacityException e) {
                        byteBuf.release();
                        throw new RuntimeException(e);
                    }
                }

                stopCountDownLatch.countDown();
            }).start();
        }

        stopCountDownLatch.await();

        // Ensure all records are uploaded.
        try {
            recordAccumulatorExt.unsafeUpload(true);
        } catch (Exception e) {
            fail(e);
        }

        for (CompletableFuture<AppendResult.CallbackResult> future : futureList) {
            future.join();
        }

        assertEquals(100 * threadCount, generatedByteBufMap.size());

        assertFalse(recordAccumulatorExt.objectList().isEmpty());

        CompositeByteBuf source = Unpooled.compositeBuffer();
        for (ByteBuf buffer : generatedByteBufMap.values()) {
            source.addComponent(true, buffer);
        }

        CompositeByteBuf result = Unpooled.compositeBuffer();
        for (RecordAccumulator.WALObject object : recordAccumulatorExt.objectList()) {
            ByteBuf buf = objectStorage.rangeRead(ObjectStorage.ReadOptions.DEFAULT, object.path(), 0, object.length()).join();
            buf.skipBytes(WALObjectHeader.WAL_HEADER_SIZE);
            result.addComponent(true, buf);
        }

        assertEquals(source, result);
        source.release();
        result.release();
    }

    @Test
    public void testUploadPeriodically() throws OverCapacityException {
        assertTrue(recordAccumulatorExt.objectList().isEmpty());

        ByteBuf byteBuf = generateByteBuf(25);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulatorExt.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);

        await().atMost(Duration.ofSeconds(1)).until(future::isDone);
        assertEquals(1, recordAccumulatorExt.objectList().size());
    }

    @Test
    public void testShutdown() throws OverCapacityException, InterruptedException {
        ScheduledExecutorService executorService = recordAccumulatorExt.executorService();
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        ByteBuf byteBuf = generateByteBuf(25);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulatorExt.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> !future.isDone());
        assertTrue(recordAccumulatorExt.objectList().isEmpty());

        // Flush all data to S3 when close.
        recordAccumulatorExt.close();
        assertTrue(future.isDone());
        assertEquals(1, recordAccumulatorExt.objectList().size());
    }
}
