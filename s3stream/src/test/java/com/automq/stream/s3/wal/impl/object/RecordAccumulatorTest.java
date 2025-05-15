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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.WALFencedException;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RecordAccumulatorTest {
    private RecordAccumulator recordAccumulator;
    private MockObjectStorage objectStorage;
    private ConcurrentSkipListMap<Long, ByteBuf> generatedByteBufMap;
    private Random random;

    @BeforeEach
    public void setUp() {
        objectStorage = new MockObjectStorage();
        ObjectWALConfig config = ObjectWALConfig.builder()
            .withMaxBytesInBatch(123)
            .withNodeId(100)
            .withEpoch(1000)
            .withBatchInterval(Long.MAX_VALUE)
            .withStrictBatchLimit(true)
            .build();
        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, config);
        recordAccumulator.start();
        generatedByteBufMap = new ConcurrentSkipListMap<>();
        random = new Random();
    }

    @AfterEach
    public void tearDown() {
        objectStorage.triggerAll();
        recordAccumulator.close();
        objectStorage.close();
    }

    private ByteBuf generateByteBuf(int size) {
        ByteBuf byteBuf = Unpooled.buffer(size);
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    @Test
    public void testOffset() throws OverCapacityException, WALFencedException {
        ByteBuf byteBuf1 = generateByteBuf(50);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf1.readableBytes(), offset -> byteBuf1.retainedSlice().asReadOnly(), future);
        assertEquals(50, recordAccumulator.nextOffset());

        recordAccumulator.unsafeUpload(true);
        long flushedOffset = future.join().flushedOffset();
        assertEquals(50, flushedOffset);
        assertEquals(50, recordAccumulator.flushedOffset());

        List<RecordAccumulator.WALObject> objectList = recordAccumulator.objectList();
        assertEquals(1, objectList.size());

        RecordAccumulator.WALObject object = objectList.get(0);
        assertEquals(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE + 50, object.length());
        ByteBuf result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
        ByteBuf headerBuf = result.readBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
        WALObjectHeader objectHeader = WALObjectHeader.unmarshal(headerBuf);
        headerBuf.release();
        assertEquals(WALObjectHeader.DEFAULT_WAL_MAGIC_CODE, objectHeader.magicCode());
        assertEquals(0, objectHeader.startOffset());
        assertEquals(50, objectHeader.length());
        // The last write timestamp is not set currently.
        assertEquals(0L, objectHeader.stickyRecordLength());
        assertEquals(100, objectHeader.nodeId());
        assertEquals(1000, objectHeader.epoch());
        assertEquals(-1, objectHeader.trimOffset());

        assertEquals(byteBuf1, result);
        byteBuf1.release();

        // Test huge record.
        ByteBuf byteBuf2 = generateByteBuf(50);
        future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf2.readableBytes(), offset -> byteBuf2.retainedSlice().asReadOnly(), future);
        assertEquals(100, recordAccumulator.nextOffset());

        ByteBuf byteBuf3 = generateByteBuf(75);
        future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf3.readableBytes(), offset -> byteBuf3.retainedSlice().asReadOnly(), future);
        assertEquals(175, recordAccumulator.nextOffset());

        recordAccumulator.unsafeUpload(true);
        flushedOffset = future.join().flushedOffset();
        assertEquals(175, flushedOffset);
        assertEquals(175, recordAccumulator.flushedOffset());

        objectList = recordAccumulator.objectList();
        assertEquals(2, objectList.size());

        object = objectList.get(1);
        assertEquals(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE + 50 + 75, object.length());
        result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
        result.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
        CompositeByteBuf compositeBuffer = Unpooled.compositeBuffer();
        compositeBuffer.addComponents(true, byteBuf2);
        compositeBuffer.addComponents(true, byteBuf3);
        assertEquals(compositeBuffer, result);
        compositeBuffer.release();

        // Test record part
        ByteBuf byteBuf4 = generateByteBuf(50);
        future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf4.readableBytes(), offset -> byteBuf4.retainedSlice().asReadOnly(), future);
        assertEquals(225, recordAccumulator.nextOffset());

        ByteBuf byteBuf5 = generateByteBuf(50);
        future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf5.readableBytes(), offset -> byteBuf5.retainedSlice().asReadOnly(), future);
        assertEquals(275, recordAccumulator.nextOffset());

        recordAccumulator.unsafeUpload(true);
        flushedOffset = future.join().flushedOffset();
        assertEquals(275, flushedOffset);
        assertEquals(275, recordAccumulator.flushedOffset());

        objectList = recordAccumulator.objectList();
        assertEquals(4, objectList.size());

        object = objectList.get(2);
        assertEquals(123, object.length());
        result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
        result.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
        assertEquals(byteBuf4, result.readBytes(50));

        object = objectList.get(3);
        compositeBuffer = Unpooled.compositeBuffer();
        compositeBuffer.addComponents(true, result);
        result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
        result.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
        compositeBuffer.addComponents(true, result);
        assertEquals(compositeBuffer, byteBuf5);
        byteBuf4.release();
        byteBuf5.release();
    }

    @Test
    public void testStrictBatchLimit() throws OverCapacityException, WALFencedException {
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
        recordAccumulator.append(50, offset -> generateByteBuf(50), future);
        assertEquals(150, recordAccumulator.nextOffset());

        recordAccumulator.unsafeUpload(true);
        future.join();

        assertEquals(2, recordAccumulator.objectList().size());

        // Reset the RecordAccumulator with strict batch limit disabled.
        recordAccumulator.close();
        ObjectWALConfig config = ObjectWALConfig.builder()
            .withMaxBytesInBatch(115)
            .withNodeId(100)
            .withEpoch(1000)
            .withBatchInterval(Long.MAX_VALUE)
            .withStrictBatchLimit(false)
            .build();
        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, config);
        recordAccumulator.start();

        assertEquals(2, recordAccumulator.objectList().size());

        future = new CompletableFuture<>();
        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
        recordAccumulator.append(50, offset -> generateByteBuf(50), future);
        assertEquals(300, recordAccumulator.nextOffset());


        recordAccumulator.unsafeUpload(true);
        future.join();

        assertEquals(3, recordAccumulator.objectList().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInMultiThread(boolean strictBathLimit) throws InterruptedException, WALFencedException {
        recordAccumulator.close();

        ObjectWALConfig config = ObjectWALConfig.builder()
            .withMaxBytesInBatch(115)
            .withNodeId(100)
            .withEpoch(1000)
            .withBatchInterval(Long.MAX_VALUE)
            .withStrictBatchLimit(strictBathLimit)
            .build();
        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, config);
        recordAccumulator.start();

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
                    ByteBuf byteBuf = generateByteBuf(40);
                    try {
                        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
                        long offset = recordAccumulator.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);
                        futureList.add(future);
                        generatedByteBufMap.put(offset, byteBuf);

                        Thread.sleep(15);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                stopCountDownLatch.countDown();
            }).start();
        }

        stopCountDownLatch.await();

        // Ensure all records are uploaded.
        try {
            recordAccumulator.unsafeUpload(true);
        } catch (Exception e) {
            fail(e);
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
            ByteBuf buf = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
            buf.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
            result.addComponent(true, buf);
        }

        assertEquals(source, result);
        source.release();
        result.release();
    }

    @Test
    public void testUploadPeriodically() throws OverCapacityException, WALFencedException {
        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ObjectWALConfig.builder().build());
        recordAccumulator.start();

        assertTrue(recordAccumulator.objectList().isEmpty());

        ByteBuf byteBuf = generateByteBuf(25);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);

        await().atMost(Duration.ofSeconds(1)).until(future::isDone);
        assertEquals(1, recordAccumulator.objectList().size());
    }

    @Test
    public void testShutdown() throws InterruptedException, OverCapacityException, WALFencedException {
        ScheduledExecutorService executorService = recordAccumulator.executorService();
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        ByteBuf byteBuf = generateByteBuf(25);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> !future.isDone());
        assertTrue(recordAccumulator.objectList().isEmpty());

        // Flush all data to S3 when close.
        recordAccumulator.close();
        assertTrue(future.isDone());


        ObjectWALConfig config = ObjectWALConfig.builder()
            .withMaxBytesInBatch(115)
            .withNodeId(100)
            .withEpoch(1000)
            .withBatchInterval(Long.MAX_VALUE)
            .withStrictBatchLimit(true)
            .build();
        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, config);
        recordAccumulator.start();
        assertEquals(1, recordAccumulator.objectList().size());
    }

    @Test
    public void testTrim() throws OverCapacityException, WALFencedException {
        ByteBuf byteBuf1 = generateByteBuf(50);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf1.readableBytes(), offset -> byteBuf1.retainedSlice().asReadOnly(), future);

        ByteBuf byteBuf2 = generateByteBuf(50);
        future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf2.readableBytes(), offset -> byteBuf2.retainedSlice().asReadOnly(), future);

        recordAccumulator.unsafeUpload(true);
        long flushedOffset = future.join().flushedOffset();
        assertEquals(100, flushedOffset);
        assertEquals(100, recordAccumulator.flushedOffset());
        assertEquals(2, recordAccumulator.objectList().size());

        recordAccumulator.trim(50).join();
        assertEquals(2, recordAccumulator.objectList().size());

        recordAccumulator.trim(100).join();
        assertEquals(0, recordAccumulator.objectList().size());
    }

    @Test
    public void testReset() throws OverCapacityException, WALFencedException {
        ByteBuf byteBuf1 = generateByteBuf(50);
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf1.readableBytes(), offset -> byteBuf1.retainedSlice().asReadOnly(), future);
        recordAccumulator.unsafeUpload(true);
        future.join();

        ByteBuf byteBuf2 = generateByteBuf(50);
        future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf2.readableBytes(), offset -> byteBuf2.retainedSlice().asReadOnly(), future);
        recordAccumulator.unsafeUpload(true);
        future.join();

        // Close and restart with another node id.
        recordAccumulator.close();
        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ObjectWALConfig.builder().withEpoch(System.currentTimeMillis()).build());
        recordAccumulator.start();
        assertEquals(0, recordAccumulator.objectList().size());

        // Close and restart with the same node id and higher node epoch.
        recordAccumulator.close();
        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ObjectWALConfig.builder().withNodeId(100).withEpoch(System.currentTimeMillis()).build());
        recordAccumulator.start();
        assertEquals(2, recordAccumulator.objectList().size());

        ByteBuf byteBuf3 = generateByteBuf(50);
        future = new CompletableFuture<>();
        recordAccumulator.append(byteBuf3.readableBytes(), offset -> byteBuf3.retainedSlice().asReadOnly(), future);
        recordAccumulator.unsafeUpload(true);
        future.join();

        List<RecordAccumulator.WALObject> objectList = recordAccumulator.objectList();
        assertEquals(3, objectList.size());
        assertEquals(byteBuf1, objectStorage.read(new ReadOptions().bucket((short) 0), objectList.get(0).path()).join().skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE));
        assertEquals(byteBuf2, objectStorage.read(new ReadOptions().bucket((short) 0), objectList.get(1).path()).join().skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE));
        assertEquals(byteBuf3, objectStorage.read(new ReadOptions().bucket((short) 0), objectList.get(2).path()).join().skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE));

        recordAccumulator.reset().join();
        assertEquals(0, recordAccumulator.objectList().size());
    }

    @Test
    public void testSequentiallyComplete() throws WALFencedException, OverCapacityException, InterruptedException {
        objectStorage.markManualWrite();
        ByteBuf byteBuf = generateByteBuf(1);

        CompletableFuture<AppendResult.CallbackResult> future0 = new CompletableFuture<>();
        CompletableFuture<AppendResult.CallbackResult> future1 = new CompletableFuture<>();
        CompletableFuture<AppendResult.CallbackResult> future2 = new CompletableFuture<>();
        CompletableFuture<AppendResult.CallbackResult> future3 = new CompletableFuture<>();

        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future0);
        recordAccumulator.unsafeUpload(true);
        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future1);
        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future2);
        recordAccumulator.unsafeUpload(true);
        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future3);
        recordAccumulator.unsafeUpload(true);

        // sleep to wait for potential async callback
        Thread.sleep(100);
        assertFalse(future0.isDone());
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        assertFalse(future3.isDone());

        objectStorage.triggerWrite("1-3");
        Thread.sleep(100);
        assertFalse(future0.isDone());
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        assertFalse(future3.isDone());

        objectStorage.triggerWrite("0-1");
        Thread.sleep(100);
        assertTrue(future0.isDone());
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
        assertFalse(future3.isDone());

        objectStorage.triggerWrite("3-4");
        Thread.sleep(100);
        assertTrue(future0.isDone());
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
        assertTrue(future3.isDone());
    }
}
