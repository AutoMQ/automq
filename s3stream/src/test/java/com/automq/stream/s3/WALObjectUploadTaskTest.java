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

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.CommitWALObjectResponse;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.automq.stream.s3.TestUtils.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class WALObjectUploadTaskTest {
    ObjectManager objectManager;
    S3Operator s3Operator;
    WALObjectUploadTask walObjectUploadTask;

    @BeforeEach
    public void setup() {
        objectManager = mock(ObjectManager.class);
        s3Operator = new MemoryS3Operator();
    }

    @Test
    public void testUpload() throws Exception {
        AtomicLong objectIdAlloc = new AtomicLong(10);
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitWALObject(any())).thenReturn(CompletableFuture.completedFuture(new CommitWALObjectResponse()));

        Map<Long, List<StreamRecordBatch>> map = new HashMap<>();
        map.put(233L, List.of(
                new StreamRecordBatch(233, 0, 10, 2, random(512)),
                new StreamRecordBatch(233, 0, 12, 2, random(128)),
                new StreamRecordBatch(233, 0, 14, 2, random(512))
        ));
        map.put(234L, List.of(
                new StreamRecordBatch(234, 0, 20, 2, random(128)),
                new StreamRecordBatch(234, 0, 22, 2, random(128))
        ));

        Config config = new Config()
                .s3ObjectBlockSize(16 * 1024 * 1024)
                .s3ObjectPartSize(16 * 1024 * 1024)
                .s3StreamSplitSize(1000);
        walObjectUploadTask = WALObjectUploadTask.of(config, map, objectManager, s3Operator, ForkJoinPool.commonPool());

        walObjectUploadTask.prepare().get();
        walObjectUploadTask.upload().get();
        walObjectUploadTask.commit().get();

        // Release all the buffers
        map.values().forEach(batches -> batches.forEach(StreamRecordBatch::release));

        ArgumentCaptor<CommitWALObjectRequest> reqArg = ArgumentCaptor.forClass(CommitWALObjectRequest.class);
        verify(objectManager, times(1)).commitWALObject(reqArg.capture());
        // expect
        // - stream233 split
        // - stream234 write to one stream range
        CommitWALObjectRequest request = reqArg.getValue();
        assertEquals(10, request.getObjectId());
        assertEquals(1, request.getStreamRanges().size());
        assertEquals(234, request.getStreamRanges().get(0).getStreamId());
        assertEquals(20, request.getStreamRanges().get(0).getStartOffset());
        assertEquals(24, request.getStreamRanges().get(0).getEndOffset());

        assertEquals(1, request.getStreamObjects().size());
        StreamObject streamObject = request.getStreamObjects().get(0);
        assertEquals(233, streamObject.getStreamId());
        assertEquals(11, streamObject.getObjectId());
        assertEquals(10, streamObject.getStartOffset());
        assertEquals(16, streamObject.getEndOffset());

        {
            S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(request.getObjectId(), request.getObjectSize(), S3ObjectType.WAL);
            ObjectReader objectReader = new ObjectReader(s3ObjectMetadata, s3Operator);
            ObjectReader.DataBlockIndex blockIndex = objectReader.find(234, 20, 24).get().get(0);
            ObjectReader.DataBlock dataBlock = objectReader.read(blockIndex).get();
            try (CloseableIterator<StreamRecordBatch> it = dataBlock.iterator()) {
                StreamRecordBatch record = it.next();
                assertEquals(20, record.getBaseOffset());
                record = it.next();
                assertEquals(24, record.getLastOffset());
                record.release();
            }
        }

        {
            S3ObjectMetadata streamObjectMetadata = new S3ObjectMetadata(11, request.getStreamObjects().get(0).getObjectSize(), S3ObjectType.STREAM);
            ObjectReader objectReader = new ObjectReader(streamObjectMetadata, s3Operator);
            ObjectReader.DataBlockIndex blockIndex = objectReader.find(233, 10, 16).get().get(0);
            ObjectReader.DataBlock dataBlock = objectReader.read(blockIndex).get();
            try (CloseableIterator<StreamRecordBatch> it = dataBlock.iterator()) {
                StreamRecordBatch r1 = it.next();
                assertEquals(10, r1.getBaseOffset());
                r1.release();
                StreamRecordBatch r2 = it.next();
                assertEquals(12, r2.getBaseOffset());
                r2.release();
                StreamRecordBatch r3 = it.next();
                assertEquals(14, r3.getBaseOffset());
                r3.release();
            }
        }
    }

    @Test
    public void testUpload_oneStream() throws Exception {
        AtomicLong objectIdAlloc = new AtomicLong(10);
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitWALObject(any())).thenReturn(CompletableFuture.completedFuture(new CommitWALObjectResponse()));

        Map<Long, List<StreamRecordBatch>> map = new HashMap<>();
        map.put(233L, List.of(
                new StreamRecordBatch(233, 0, 10, 2, random(512)),
                new StreamRecordBatch(233, 0, 12, 2, random(128)),
                new StreamRecordBatch(233, 0, 14, 2, random(512))
        ));
        Config config = new Config()
                .s3ObjectBlockSize(16 * 1024 * 1024)
                .s3ObjectPartSize(16 * 1024 * 1024)
                .s3StreamSplitSize(16 * 1024 * 1024);
        walObjectUploadTask = WALObjectUploadTask.of(config, map, objectManager, s3Operator, ForkJoinPool.commonPool());

        walObjectUploadTask.prepare().get();
        walObjectUploadTask.upload().get();
        walObjectUploadTask.commit().get();

        // Release all the buffers
        map.values().forEach(batches -> batches.forEach(StreamRecordBatch::release));


        ArgumentCaptor<CommitWALObjectRequest> reqArg = ArgumentCaptor.forClass(CommitWALObjectRequest.class);
        verify(objectManager, times(1)).commitWALObject(reqArg.capture());
        CommitWALObjectRequest request = reqArg.getValue();
        assertEquals(0, request.getObjectSize());
        assertEquals(0, request.getStreamRanges().size());
        assertEquals(1, request.getStreamObjects().size());
    }

    @Test
    public void test_emptyWALData() throws ExecutionException, InterruptedException, TimeoutException {
        AtomicLong objectIdAlloc = new AtomicLong(10);
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitWALObject(any())).thenReturn(CompletableFuture.completedFuture(new CommitWALObjectResponse()));

        Map<Long, List<StreamRecordBatch>> map = new HashMap<>();
        map.put(233L, List.of(
                new StreamRecordBatch(233, 0, 10, 2, random(512))
        ));
        map.put(234L, List.of(
                new StreamRecordBatch(234, 0, 20, 2, random(128))
        ));

        Config config = new Config()
                .s3ObjectBlockSize(16 * 1024 * 1024)
                .s3ObjectPartSize(16 * 1024 * 1024)
                .s3StreamSplitSize(64);
        walObjectUploadTask = WALObjectUploadTask.of(config, map, objectManager, s3Operator, ForkJoinPool.commonPool());
        assertTrue(walObjectUploadTask.forceSplit);
    }
}
