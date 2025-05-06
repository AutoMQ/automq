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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.network.test.RecordTestNetworkBandwidthLimiter;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class AbstractObjectStorageTest {

    AbstractObjectStorage objectStorage;

    @BeforeEach
    public void setUp() {
        objectStorage = new MemoryObjectStorage(false);
    }

    @AfterEach
    public void tearDown() {
        objectStorage.close();
    }

    @Test
    public void testMergeTask() {
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 3000, S3ObjectType.STREAM);
        AbstractObjectStorage.MergedReadTask mergedReadTask = new AbstractObjectStorage.MergedReadTask(
            new AbstractObjectStorage.ReadTask(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0, 1024, new CompletableFuture<>()), 0);
        boolean ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 1024, 2048, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 2049, 3000, new CompletableFuture<>()));
        assertFalse(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
    }

    @Test
    public void testMergeTask2() {
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 4096, S3ObjectType.STREAM);
        AbstractObjectStorage.MergedReadTask mergedReadTask = new AbstractObjectStorage.MergedReadTask(
            new AbstractObjectStorage.ReadTask(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0, 1024, new CompletableFuture<>()), 0.5f);
        boolean ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 2048, 4096, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.25, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 1024, 1536, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.125, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
    }

    @Test
    void testMergeRead() throws ExecutionException, InterruptedException {
        S3ObjectMetadata s3ObjectMetadata1 = new S3ObjectMetadata(1, 33554944, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata2 = new S3ObjectMetadata(2, 3072, S3ObjectType.STREAM);
        objectStorage = new MemoryObjectStorage(true);
        objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata1.key(), TestUtils.random((int) s3ObjectMetadata1.objectSize())).get();
        objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata2.key(), TestUtils.random((int) s3ObjectMetadata2.objectSize())).get();

        objectStorage = spy(objectStorage);
        // obj0_0_1024 obj_1_1024_2048 obj_0_16776192_16777216 obj_0_2048_4096 obj_0_16777216_16778240
        CompletableFuture<ByteBuf> cf1 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 0, 1024);
        CompletableFuture<ByteBuf> cf2 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata2.key(), 1024, 3072);
        CompletableFuture<ByteBuf> cf3 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 31457280, 31461376);
        CompletableFuture<ByteBuf> cf4 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 2048, 4096);
        CompletableFuture<ByteBuf> cf5 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 33554432, 33554944);

        objectStorage.tryMergeRead();

        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(any(), eq(s3ObjectMetadata1.key()), eq(0L), eq(4096L));
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(any(), eq(s3ObjectMetadata2.key()), eq(1024L), eq(3072L));
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(any(), eq(s3ObjectMetadata1.key()), eq(31457280L), eq(31461376L));
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(any(), eq(s3ObjectMetadata1.key()), eq(33554432L), eq(33554944L));

        ByteBuf buf = cf1.get();
        assertEquals(1024, buf.readableBytes());
        buf.release();
        buf = cf2.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
        buf = cf3.get();
        assertEquals(4096, buf.readableBytes());
        buf.release();
        buf = cf4.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
        buf = cf5.get();
        assertEquals(512, buf.readableBytes());
        buf.release();
    }

    @Test
    void testByteBufRefCnt() throws ExecutionException, InterruptedException {
        objectStorage = new MemoryObjectStorage(false);
        S3ObjectMetadata s3ObjectMetadata1 = new S3ObjectMetadata(1, 100, S3ObjectType.STREAM);
        objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata1.key(), TestUtils.random((int) s3ObjectMetadata1.objectSize())).get();
        objectStorage = spy(objectStorage);
        objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata1.key(), 0, 100)
            .thenCompose(buf -> {
                assertEquals(1, buf.refCnt());
                buf.release();
                return CompletableFuture.completedFuture(null);
            }).get();
    }


    @Test
    void testFastRetry() throws Throwable {
        // Initialize memory storage and spy to track method calls
        objectStorage = new MemoryObjectStorage();
        objectStorage = spy(objectStorage);

        // Configure write options: enable fast retry, disable normal retry
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .enableFastRetry(true)
            .retry(false);

        // Mock S3 latency calculator via reflection to force fast retry condition
        Field latencyCalculatorField = AbstractObjectStorage.class.getDeclaredField("s3LatencyCalculator");
        latencyCalculatorField.setAccessible(true);
        S3LatencyCalculator mockCalculator = mock(S3LatencyCalculator.class);
        when(mockCalculator.valueAtPercentile(anyLong(), anyLong())).thenReturn(100L); // Force low latency to trigger fast retry
        latencyCalculatorField.set(objectStorage, mockCalculator);

        // Track doWrite() calls: first call hangs, second completes immediately
        AtomicInteger callCount = new AtomicInteger();
        CompletableFuture<Void> firstFuture = new CompletableFuture<>();
        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            int count = callCount.getAndIncrement();
            return (count == 0) ? firstFuture : CompletableFuture.completedFuture(null);
        });

        // Execute write operation
        ByteBuf data = TestUtils.randomPooled(1024);
        assertEquals(1, data.refCnt()); // Verify initial ref count

        CompletableFuture<ObjectStorage.WriteResult> writeFuture = objectStorage.write(options, "testKey", data);
        writeFuture.get(200, TimeUnit.MILLISECONDS); // Wait for write completion

        // Verify: two calls made (initial + retry), data ref count maintained during retry
        assertEquals(1, data.refCnt());
        assertEquals(2, callCount.get());

        // Complete initial future and verify data release
        firstFuture.complete(null);
        await().atMost(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(0, data.refCnt())); // Ensure buffer released
    }

    @Test
    void testWriteRetryTimeout() throws Throwable {
        // Setup storage with 100ms timeout (clearer time unit)
        objectStorage = spy(new MemoryObjectStorage());
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .retry(true)
            .timeout(1000L);

        // Mock hanging write operation
        AtomicInteger callCount = new AtomicInteger();
        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            int count = callCount.getAndIncrement();
            if (count < 12) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                Executors.newSingleThreadScheduledExecutor().schedule(
                    () -> future.completeExceptionally(new TimeoutException("Simulated timeout")),
                    100, TimeUnit.MILLISECONDS
                );
                return future;
            }
            // Second call: immediate success
            return CompletableFuture.completedFuture(null);
        });

        // Execute test
        ByteBuf data = TestUtils.randomPooled(1024);
        CompletableFuture<ObjectStorage.WriteResult> writeFuture =
            objectStorage.write(options, "testKey", data);
        // Verify timeout exception
        assertThrows(TimeoutException.class,
            () -> writeFuture.get(1, TimeUnit.SECONDS));
        // Verify resource cleanup
        await().atMost(2, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(0, data.refCnt()));
        // Verify: no successful calls made
        assertTrue(callCount.get() < 12);
    }

    @Test
    void testWritePermit() throws Exception {
        final int maxConcurrency = 5;
        objectStorage = spy(new MemoryObjectStorage(maxConcurrency));

        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .enableFastRetry(false)
            .retry(false);

        // Use completable future to block first 5 calls
        CompletableFuture<Void> barrierFuture = new CompletableFuture<>();
        AtomicInteger callCount = new AtomicInteger();

        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            int count = callCount.getAndIncrement();
            return (count < maxConcurrency)
                ? barrierFuture // Block first 5 calls
                : CompletableFuture.completedFuture(null); // Immediate success for 6th
        });

        // Phase 1: Submit max concurrency requests
        List<ByteBuf> buffers = new ArrayList<>();
        for (int i = 0; i < maxConcurrency; i++) {
            ByteBuf data = TestUtils.randomPooled(1024);
            buffers.add(data);
            objectStorage.write(options, "testKey", data);
        }

        // Verify initial calls reached max concurrency
        await().atMost(1, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(maxConcurrency, callCount.get()));

        // Phase 2: Submit 6th request beyond concurrency limit
        CompletableFuture<ObjectStorage.WriteResult> sixthWriteFuture =
            CompletableFuture.supplyAsync(() ->
                objectStorage.write(options, "testKey", TestUtils.random(1024))
            ).thenCompose(f -> f);

        // Release blocked calls and verify completion
        barrierFuture.complete(null);
        await().atMost(2, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                assertEquals(maxConcurrency + 1, callCount.get());
                assertTrue(sixthWriteFuture.isDone());

                // Verify: all buffers released
                for (ByteBuf buffer : buffers) {
                    assertEquals(0, buffer.refCnt());
                }
            });
    }

    @Test
    void testWaitWritePermit() throws Exception {
        final int maxConcurrency = 1;
        objectStorage = spy(new MemoryObjectStorage(maxConcurrency));

        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions()
            .enableFastRetry(false)
            .retry(false);

        // Block first call using completable future
        CompletableFuture<Void> blockingFuture = new CompletableFuture<>();
        AtomicInteger callCount = new AtomicInteger();

        when(objectStorage.doWrite(any(), anyString(), any())).thenAnswer(inv -> {
            callCount.incrementAndGet();
            return blockingFuture; // Always return blocking future for first call
        });

        // Phase 1: Acquire the only permit
        ByteBuf firstBuffer = TestUtils.randomPooled(1024);
        objectStorage.write(options, "testKey", firstBuffer);

        // Verify permit acquisition
        await().until(() -> callCount.get() == 1);

        // Phase 2: Verify blocking behavior with interrupt
        Thread blockingThread = new Thread(() -> {
            ByteBuf byteBuf = TestUtils.randomPooled(1024);
            try {
                CompletableFuture<ObjectStorage.WriteResult> future =
                    objectStorage.write(options, "testKey", byteBuf);
                ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
                assertTrue(exception.getCause() instanceof InterruptedException);
            } catch (Exception e) {
                // Ignore
            } finally {
                await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertEquals(0, byteBuf.refCnt());
                });
            }
        });

        blockingThread.start();

        Thread.sleep(1000);

        // Interrupt and verify
        blockingThread.interrupt();
        blockingThread.join();

        // Verify resource cleanup
        assertEquals(1, firstBuffer.refCnt());

        // Cleanup
        blockingFuture.complete(null);
        await().atMost(2, TimeUnit.SECONDS)
            .untilAsserted(() -> assertEquals(0, firstBuffer.refCnt()));
    }


    @Test
    void testReadToEndOfObject() throws ExecutionException, InterruptedException {
        objectStorage = new MemoryObjectStorage(true);
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 4096, S3ObjectType.STREAM);

        objectStorage.writer(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata.key()).write(TestUtils.random(4096));
        objectStorage = spy(objectStorage);

        CompletableFuture<ByteBuf> cf1 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0L, 1024L);
        CompletableFuture<ByteBuf> cf2 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 2048L, -1L);

        objectStorage.tryMergeRead();
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(any(), eq(s3ObjectMetadata.key()), eq(0L), eq(1024L));
        objectStorage.tryMergeRead();
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(any(), eq(s3ObjectMetadata.key()), eq(2048L), eq(-1L));

        ByteBuf buf = cf1.get();
        assertEquals(1024, buf.readableBytes());
        buf.release();
        buf = cf2.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
    }

    @Test
    void testReadToEndWithNetworkLimiter() throws InterruptedException {
        int objectSize = 4096;
        objectStorage = new MemoryObjectStorage(false);
        MemoryObjectStorage memoryObjectStorage = (MemoryObjectStorage) objectStorage;
        RecordTestNetworkBandwidthLimiter networkInboundBandwidthLimiter = (RecordTestNetworkBandwidthLimiter) memoryObjectStorage.getNetworkInboundBandwidthLimiter();

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, objectSize, S3ObjectType.STREAM);
        objectStorage.writer(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata.key()).write(TestUtils.random(objectSize));
        objectStorage = spy(objectStorage);

        // check read to end range read will increase the inboundWidthLimiter
        CompletableFuture<ByteBuf> cf2 = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0, -1L);

        cf2.join();
        Thread.sleep(100); // wait for async reacquire token
        assertEquals(objectSize, networkInboundBandwidthLimiter.getConsumed());
    }

    @Test
    void testRangeReadAllFailCanFinishTheReturnedCompletableFuture() {
        int objectSize = 4096;
        MemoryObjectStorage memoryObjectStorage = new MemoryObjectStorage(true);

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, objectSize, S3ObjectType.STREAM);
        memoryObjectStorage.writer(ObjectStorage.WriteOptions.DEFAULT, s3ObjectMetadata.key()).write(TestUtils.random(objectSize));
        memoryObjectStorage = spy(memoryObjectStorage);

        // mock read fail can finish the return completeFuture
        when(memoryObjectStorage.doRangeRead(any(), anyString(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException("mock exception")));

        CompletableFuture<ByteBuf> cf4 = memoryObjectStorage.rangeRead(new ReadOptions().bucket((short) 0), s3ObjectMetadata.key(), 0, -1L);
        memoryObjectStorage.tryMergeRead();

        try {
            cf4.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // expected.
        }

        assertTrue(cf4.isDone() && cf4.isCompletedExceptionally());
    }
}
