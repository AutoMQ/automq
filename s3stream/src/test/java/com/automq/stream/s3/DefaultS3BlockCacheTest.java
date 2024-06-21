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

package com.automq.stream.s3;

import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.DefaultS3BlockCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.Threads;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class DefaultS3BlockCacheTest {
    ObjectManager objectManager;
    ObjectStorage objectStorage;
    DefaultS3BlockCache s3BlockCache;
    Config config;

    @BeforeEach
    public void setup() {
        config = new Config();
        config.blockCacheSize(0);
        objectManager = Mockito.mock(ObjectManager.class);
        objectStorage = new MemoryObjectStorage();
        s3BlockCache = new DefaultS3BlockCache(config, objectManager, objectStorage);
    }

    @Test
    public void testRead() throws Exception {
        ObjectWriter objectWriter = ObjectWriter.writer(0, objectStorage, 1024, 1024);
        objectWriter.write(233, List.of(
            newRecord(233, 10, 5, 512),
            newRecord(233, 15, 10, 512),
            newRecord(233, 25, 5, 512)
        ));
        objectWriter.write(234, List.of(newRecord(234, 0, 5, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata1 = new S3ObjectMetadata(0, objectWriter.size(), S3ObjectType.STREAM_SET);

        objectWriter = ObjectWriter.writer(1, objectStorage, 1024, 1024);
        objectWriter.write(233, List.of(newRecord(233, 30, 10, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata2 = new S3ObjectMetadata(1, objectWriter.size(), S3ObjectType.STREAM_SET);

        objectWriter = ObjectWriter.writer(2, objectStorage, 1024, 1024);
        objectWriter.write(233, List.of(newRecord(233, 40, 20, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata3 = new S3ObjectMetadata(2, objectWriter.size(), S3ObjectType.STREAM_SET);

        when(objectManager.getObjects(eq(233L), eq(11L), eq(60L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(
            metadata1, metadata2
        )));
        when(objectManager.getObjects(eq(233L), eq(40L), eq(60L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(
            metadata3
        )));

        ReadDataBlock rst = s3BlockCache.read(233L, 11L, 60L, 10000).get(3000, TimeUnit.SECONDS);
        assertEquals(5, rst.getRecords().size());
        assertEquals(10, rst.getRecords().get(0).getBaseOffset());
        assertEquals(60, rst.getRecords().get(4).getLastOffset());
    }

    @Test
    public void testRead_readAhead() throws ExecutionException, InterruptedException {
        objectManager = Mockito.mock(ObjectManager.class);
        objectStorage = Mockito.spy(new MemoryObjectStorage());
        config.blockCacheSize(1024 * 1024);
        s3BlockCache = new DefaultS3BlockCache(config, objectManager, objectStorage);

        ObjectWriter objectWriter = ObjectWriter.writer(0, objectStorage, 1024, 1024);
        objectWriter.write(233, List.of(
            newRecord(233, 10, 5, 512),
            newRecord(233, 15, 5, 4096)
        ));
        objectWriter.close();
        S3ObjectMetadata metadata1 = new S3ObjectMetadata(0, objectWriter.size(), S3ObjectType.STREAM_SET);

        objectWriter = ObjectWriter.writer(1, objectStorage, 1024, 1024);
        objectWriter.write(233, List.of(newRecord(233, 20, 10, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata2 = new S3ObjectMetadata(1, objectWriter.size(), S3ObjectType.STREAM_SET);

        when(objectManager.getObjects(eq(233L), eq(10L), eq(11L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(metadata1)));

        s3BlockCache.read(233L, 10L, 11L, 10000).get();
        // range read index and range read data
        verify(objectStorage, Mockito.times(2)).rangeRead(any(), eq(ObjectUtils.genKey(0, 0)), ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong());
        verify(objectStorage, Mockito.times(0)).rangeRead(any(), eq(ObjectUtils.genKey(0, 1)), ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong());
        // trigger read ahead
        when(objectManager.getObjects(eq(233L), eq(20L), eq(-1L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(metadata2)));
        when(objectManager.getObjects(eq(233L), eq(30L), eq(-1L), eq(2))).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
        s3BlockCache.read(233L, 15L, 16L, 10000).get();
        verify(objectStorage, timeout(1000).times(2)).rangeRead(any(), eq(ObjectUtils.genKey(0, 1)), ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong());
        verify(objectManager, timeout(1000).times(1)).getObjects(eq(233L), eq(30L), eq(-1L), eq(2));

        Threads.sleep(1000);

        // expect read ahead already cached the records
        ReadDataBlock ret = s3BlockCache.read(233L, 20L, 30L, 10000).get();
        assertEquals(CacheAccessType.BLOCK_CACHE_HIT, ret.getCacheAccessType());
        List<StreamRecordBatch> records = ret.getRecords();
        assertEquals(1, records.size());
        assertEquals(20L, records.get(0).getBaseOffset());
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }

}
