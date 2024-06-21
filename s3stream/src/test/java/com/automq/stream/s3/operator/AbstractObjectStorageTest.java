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

package com.automq.stream.s3.operator;


import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;


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
            new AbstractObjectStorage.ReadTask(s3ObjectMetadata.key(), 0, 1024, new CompletableFuture<>()), 0);
        boolean ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata.key(), 1024, 2048, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata.key(), 2049, 3000, new CompletableFuture<>()));
        assertFalse(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
    }

    @Test
    public void testMergeTask2() {
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 4096, S3ObjectType.STREAM);
        AbstractObjectStorage.MergedReadTask mergedReadTask = new AbstractObjectStorage.MergedReadTask(
            new AbstractObjectStorage.ReadTask(s3ObjectMetadata.key(), 0, 1024, new CompletableFuture<>()), 0.5f);
        boolean ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata.key(), 2048, 4096, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.25, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata.key(), 1024, 1536, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.125, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
    }

    @Test
    void testMergeRead() throws ExecutionException, InterruptedException {
        objectStorage = new MemoryObjectStorage(true) {
            @Override
            CompletableFuture<ByteBuf> mergedRangeRead(String path, long start, long end) {
                return CompletableFuture.completedFuture(TestUtils.random((int) (end - start + 1)));
            }
        };
        objectStorage = spy(objectStorage);
        S3ObjectMetadata s3ObjectMetadata1 = new S3ObjectMetadata(1, 33554944, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata2 = new S3ObjectMetadata(2, 3072, S3ObjectType.STREAM);
        // obj0_0_1024 obj_1_1024_2048 obj_0_16776192_16777216 obj_0_2048_4096 obj_0_16777216_16778240
        CompletableFuture<ByteBuf> cf1 = objectStorage.rangeRead(ReadOptions.DEFAULT, s3ObjectMetadata1.key(), 0, 1024);
        CompletableFuture<ByteBuf> cf2 = objectStorage.rangeRead(ReadOptions.DEFAULT, s3ObjectMetadata2.key(), 1024, 3072);
        CompletableFuture<ByteBuf> cf3 = objectStorage.rangeRead(ReadOptions.DEFAULT, s3ObjectMetadata1.key(), 31457280, 31461376);
        CompletableFuture<ByteBuf> cf4 = objectStorage.rangeRead(ReadOptions.DEFAULT, s3ObjectMetadata1.key(), 2048, 4096);
        CompletableFuture<ByteBuf> cf5 = objectStorage.rangeRead(ReadOptions.DEFAULT, s3ObjectMetadata1.key(), 33554432, 33554944);

        objectStorage.tryMergeRead();

        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(eq(s3ObjectMetadata1.key()), eq(0L), eq(4096L));
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(eq(s3ObjectMetadata2.key()), eq(1024L), eq(3072L));
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(eq(s3ObjectMetadata1.key()), eq(31457280L), eq(31461376L));
        verify(objectStorage, timeout(1000L).times(1)).mergedRangeRead(eq(s3ObjectMetadata1.key()), eq(33554432L), eq(33554944L));

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
}
