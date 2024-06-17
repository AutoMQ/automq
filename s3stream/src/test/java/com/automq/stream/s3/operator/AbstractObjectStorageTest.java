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


import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class AbstractObjectStorageTest {

    @Test
    public void testMergeTask() {
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 3000, S3ObjectType.STREAM);
        AbstractObjectStorage.MergedReadTask mergedReadTask = new AbstractObjectStorage.MergedReadTask(
            new AbstractObjectStorage.ReadTask(s3ObjectMetadata, 0, 1024, new CompletableFuture<>()), 0);
        boolean ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata, 1024, 2048, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata, 2049, 3000, new CompletableFuture<>()));
        assertFalse(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
    }

    @Test
    public void testMergeTask2() {
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 4096, S3ObjectType.STREAM);
        AbstractObjectStorage.MergedReadTask mergedReadTask = new AbstractObjectStorage.MergedReadTask(
            new AbstractObjectStorage.ReadTask(s3ObjectMetadata, 0, 1024, new CompletableFuture<>()), 0.5f);
        boolean ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata, 2048, 4096, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.25, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new AbstractObjectStorage.ReadTask(s3ObjectMetadata, 1024, 1536, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.125, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
    }
}
