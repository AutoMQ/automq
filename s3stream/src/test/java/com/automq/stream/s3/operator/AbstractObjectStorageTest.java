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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
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
