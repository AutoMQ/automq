/*
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

package com.automq.stream.s3;

import com.automq.stream.s3.cache.DefaultS3BlockCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class DefaultS3BlockCacheTest {
    ObjectManager objectManager;
    S3Operator s3Operator;
    DefaultS3BlockCache s3BlockCache;

    @BeforeEach
    public void setup() {
        objectManager = Mockito.mock(ObjectManager.class);
        s3Operator = new MemoryS3Operator();
        s3BlockCache = new DefaultS3BlockCache(0, objectManager, s3Operator);
    }

    @Test
    public void testRead() throws Exception {
        ObjectWriter objectWriter = ObjectWriter.writer(0, s3Operator, 1024, 1024);
        objectWriter.write(233, List.of(
                newRecord(233, 10, 5, 512),
                newRecord(233, 15, 10, 512),
                newRecord(233, 25, 5, 512)
        ));
        objectWriter.write(234, List.of(newRecord(234, 0, 5, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata1 = new S3ObjectMetadata(0, objectWriter.size(), S3ObjectType.WAL);

        objectWriter = ObjectWriter.writer(1, s3Operator, 1024, 1024);
        objectWriter.write(233, List.of(newRecord(233, 30, 10, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata2 = new S3ObjectMetadata(1, objectWriter.size(), S3ObjectType.WAL);

        objectWriter = ObjectWriter.writer(2, s3Operator, 1024, 1024);
        objectWriter.write(233, List.of(newRecord(233, 40, 20, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata3 = new S3ObjectMetadata(2, objectWriter.size(), S3ObjectType.WAL);

        when(objectManager.getObjects(eq(233L), eq(11L), eq(60L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(
                metadata1, metadata2
        )));
        when(objectManager.getObjects(eq(233L), eq(40L), eq(60L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(
                metadata3
        )));

        ReadDataBlock rst = s3BlockCache.read(233L, 11L, 60L, 10000).get(3, TimeUnit.SECONDS);
        assertEquals(5, rst.getRecords().size());
        assertEquals(10, rst.getRecords().get(0).getBaseOffset());
        assertEquals(60, rst.getRecords().get(4).getLastOffset());
    }

    @Test
    public void testRead_readahead() throws ExecutionException, InterruptedException {
        objectManager = Mockito.mock(ObjectManager.class);
        s3Operator = Mockito.spy(new MemoryS3Operator());
        s3BlockCache = new DefaultS3BlockCache(1024 * 1024, objectManager, s3Operator);

        ObjectWriter objectWriter = ObjectWriter.writer(0, s3Operator, 1024, 1024);
        objectWriter.write(233, List.of(
                newRecord(233, 10, 5, 512),
                newRecord(233, 15, 5, 512)
        ));
        objectWriter.close();
        S3ObjectMetadata metadata1 = new S3ObjectMetadata(0, objectWriter.size(), S3ObjectType.WAL);

        objectWriter = ObjectWriter.writer(1, s3Operator, 1024, 1024);
        objectWriter.write(233, List.of(newRecord(233, 20, 10, 512)));
        objectWriter.close();
        S3ObjectMetadata metadata2 = new S3ObjectMetadata(1, objectWriter.size(), S3ObjectType.WAL);

        when(objectManager.getObjects(eq(233L), eq(10L), eq(11L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(metadata1)));

        s3BlockCache.read(233L, 10L, 11L, 10000).get();
        // range read index and range read data
        Mockito.verify(s3Operator, Mockito.times(2)).rangeRead(eq(ObjectUtils.genKey(0, 0)), ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(), ArgumentMatchers.any());
        Mockito.verify(s3Operator, Mockito.times(0)).rangeRead(eq(ObjectUtils.genKey(0, 1)), ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(), ArgumentMatchers.any());
        // trigger readahead
        when(objectManager.getObjects(eq(233L), eq(20L), eq(-1L), eq(2))).thenReturn(CompletableFuture.completedFuture(List.of(metadata2)));
        s3BlockCache.read(233L, 15L, 16L, 10000).get();
        Mockito.verify(s3Operator, timeout(1000).times(2)).rangeRead(eq(ObjectUtils.genKey(0, 1)), ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(), ArgumentMatchers.any());
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }


}
