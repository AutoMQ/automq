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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Tag("S3Unit")
public class ObjectReaderLRUCacheTest {

    private void writeStream(int streamCount, ObjectWriter objectWriter) {
        for (int i = 0; i < streamCount; i++) {
            StreamRecordBatch r = StreamRecordBatch.of(i, 0, i, 1, TestUtils.random(1));
            objectWriter.write(i, List.of(r));
        }
    }

    @Test
    public void testGetPut() throws ExecutionException, InterruptedException {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        ObjectWriter objectWriter = ObjectWriter.writer(233L, objectStorage, 1024, 1024);
        writeStream(1000, objectWriter);
        objectWriter.close().get();

        ObjectWriter objectWriter2 = ObjectWriter.writer(234L, objectStorage, 1024, 1024);
        writeStream(2000, objectWriter2);
        objectWriter2.close().get();

        ObjectWriter objectWriter3 = ObjectWriter.writer(235L, objectStorage, 1024, 1024);
        writeStream(3000, objectWriter3);
        objectWriter3.close().get();

        ObjectReader objectReader = ObjectReader.reader(new S3ObjectMetadata(233L, objectWriter.size(), S3ObjectType.STREAM_SET), objectStorage);
        ObjectReader objectReader2 = ObjectReader.reader(new S3ObjectMetadata(234L, objectWriter2.size(), S3ObjectType.STREAM_SET), objectStorage);
        ObjectReader objectReader3 = ObjectReader.reader(new S3ObjectMetadata(235L, objectWriter3.size(), S3ObjectType.STREAM_SET), objectStorage);
        Assertions.assertEquals(36000, objectReader.basicObjectInfo().get().size());
        Assertions.assertEquals(72000, objectReader2.basicObjectInfo().get().size());
        Assertions.assertEquals(108000, objectReader3.basicObjectInfo().get().size());

        ObjectReaderLRUCache cache = new ObjectReaderLRUCache("", 100000);
        cache.put(235L, objectReader3);
        cache.put(234L, objectReader2);
        cache.put(233L, objectReader);
        Assertions.assertEquals(1, cache.size());
        Map.Entry<Long, ObjectReader> entry = cache.pop();
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(objectReader, entry.getValue());
    }

    @Test
    public void testConcurrentGetPut() throws InterruptedException {
        ObjectReaderLRUCache cache = new ObjectReaderLRUCache("", 5000);
        List<CompletableFuture<Integer>> cfs = new ArrayList<>();
        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            ObjectReader reader = Mockito.mock(ObjectReader.class);
            CompletableFuture<Integer> cf = CompletableFuture.supplyAsync(() -> 100,
                CompletableFuture.delayedExecutor(r.nextInt(1000), TimeUnit.MILLISECONDS));
            cfs.add(cf);
            Mockito.doAnswer(invocation -> cf).when(reader).size();
            cache.put((long) i, reader);
        }
        CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0])).join();
        Thread.sleep(1000);
        Assertions.assertEquals(50, cache.size());
    }
}
