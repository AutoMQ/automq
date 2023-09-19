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
import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.CommitWALObjectResponse;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.MemoryWriteAheadLog;
import com.automq.stream.s3.wal.WriteAheadLog;
import org.apache.kafka.metadata.stream.StreamMetadata;
import org.apache.kafka.metadata.stream.StreamState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.automq.stream.s3.TestUtils.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
public class S3StorageTest {
    StreamManager streamManager;
    ObjectManager objectManager;
    S3Storage storage;

    @BeforeEach
    public void setup() {
        objectManager = mock(ObjectManager.class);
        streamManager = mock(StreamManager.class);
        S3Operator s3Operator = new MemoryS3Operator();
        storage = new S3Storage(new Config(), new MemoryWriteAheadLog(),
                streamManager, objectManager, new DefaultS3BlockCache(0L, objectManager, s3Operator), s3Operator);
    }

    @Test
    public void testAppend() throws Exception {
        Mockito.when(objectManager.prepareObject(eq(1), anyLong())).thenReturn(CompletableFuture.completedFuture(16L));
        CommitWALObjectResponse resp = new CommitWALObjectResponse();
        Mockito.when(objectManager.commitWALObject(any())).thenReturn(CompletableFuture.completedFuture(resp));

        CompletableFuture<Void> cf1 = storage.append(
                new StreamRecordBatch(233, 1, 10, 1, random(100))
        );
        CompletableFuture<Void> cf2 = storage.append(
                new StreamRecordBatch(233, 1, 11, 2, random(100))
        );
        CompletableFuture<Void> cf3 = storage.append(
                new StreamRecordBatch(234, 3, 100, 1, random(100))
        );

        cf1.get(3, TimeUnit.SECONDS);
        cf2.get(3, TimeUnit.SECONDS);
        cf3.get(3, TimeUnit.SECONDS);

        ReadDataBlock readRst = storage.read(233, 10, 13, 90).get();
        assertEquals(1, readRst.getRecords().size());
        readRst = storage.read(233, 10, 13, 200).get();
        assertEquals(2, readRst.getRecords().size());

        storage.forceUpload(233L).get();
        ArgumentCaptor<CommitWALObjectRequest> commitArg = ArgumentCaptor.forClass(CommitWALObjectRequest.class);
        verify(objectManager).commitWALObject(commitArg.capture());
        CommitWALObjectRequest commitReq = commitArg.getValue();
        assertEquals(16L, commitReq.getObjectId());
        List<ObjectStreamRange> streamRanges = commitReq.getStreamRanges();
        assertEquals(2, streamRanges.size());
        assertEquals(233, streamRanges.get(0).getStreamId());
        assertEquals(10, streamRanges.get(0).getStartOffset());
        assertEquals(13, streamRanges.get(0).getEndOffset());
        assertEquals(234, streamRanges.get(1).getStreamId());
        assertEquals(100, streamRanges.get(1).getStartOffset());
        assertEquals(101, streamRanges.get(1).getEndOffset());
    }

    @Test
    public void testWALCallbackSequencer() {
        S3Storage.WALCallbackSequencer seq = new S3Storage.WALCallbackSequencer();
        WalWriteRequest r0 = new WalWriteRequest(newRecord(233L, 10L), 100L, new CompletableFuture<>());
        seq.before(r0);
        WalWriteRequest r1 = new WalWriteRequest(newRecord(233L, 11L), 101L, new CompletableFuture<>());
        seq.before(r1);
        WalWriteRequest r2 = new WalWriteRequest(newRecord(234L, 20L), 102L, new CompletableFuture<>());
        seq.before(r2);
        WalWriteRequest r3 = new WalWriteRequest(newRecord(234L, 21L), 103L, new CompletableFuture<>());
        seq.before(r3);

        assertEquals(Collections.emptyList(), seq.after(r3));
        assertEquals(-1L, seq.getWALConfirmOffset());
        assertEquals(List.of(r2, r3), seq.after(r2));
        assertEquals(-1L, seq.getWALConfirmOffset());
        assertEquals(List.of(r0), seq.after(r0));
        assertEquals(100L, seq.getWALConfirmOffset());
        assertEquals(List.of(r1), seq.after(r1));
        assertEquals(103L, seq.getWALConfirmOffset());
    }

    @Test
    public void testUploadWALObject_sequence() throws ExecutionException, InterruptedException, TimeoutException {
        List<CompletableFuture<Long>> objectIdCfList = List.of(new CompletableFuture<>(), new CompletableFuture<>());
        AtomicInteger objectCfIndex = new AtomicInteger();
        Mockito.doAnswer(invocation -> objectIdCfList.get(objectCfIndex.getAndIncrement())).when(objectManager).prepareObject(ArgumentMatchers.anyInt(), anyLong());

        List<CompletableFuture<CommitWALObjectResponse>> commitCfList = List.of(new CompletableFuture<>(), new CompletableFuture<>());
        AtomicInteger commitCfIndex = new AtomicInteger();
        Mockito.doAnswer(invocation -> commitCfList.get(commitCfIndex.getAndIncrement())).when(objectManager).commitWALObject(any());

        LogCache.LogCacheBlock logCacheBlock1 = new LogCache.LogCacheBlock(1024);
        logCacheBlock1.put(newRecord(233L, 10L));
        logCacheBlock1.put(newRecord(234L, 10L));
        logCacheBlock1.confirmOffset(10L);
        CompletableFuture<Void> cf1 = storage.uploadWALObject(logCacheBlock1);

        LogCache.LogCacheBlock logCacheBlock2 = new LogCache.LogCacheBlock(1024);
        logCacheBlock2.put(newRecord(233L, 20L));
        logCacheBlock2.put(newRecord(234L, 20L));
        logCacheBlock2.confirmOffset(20L);
        CompletableFuture<Void> cf2 = storage.uploadWALObject(logCacheBlock2);

        // sequence get objectId
        verify(objectManager, Mockito.timeout(1000).times(1)).prepareObject(ArgumentMatchers.anyInt(), anyLong());

        objectIdCfList.get(0).complete(1L);
        // trigger next upload prepare objectId
        verify(objectManager, Mockito.timeout(1000).times(2)).prepareObject(ArgumentMatchers.anyInt(), anyLong());
        verify(objectManager, Mockito.timeout(1000).times(1)).commitWALObject(any());

        objectIdCfList.get(1).complete(2L);
        Thread.sleep(10);
        verify(objectManager, Mockito.times(1)).commitWALObject(any());

        commitCfList.get(0).complete(new CommitWALObjectResponse());
        verify(objectManager, Mockito.timeout(1000).times(2)).commitWALObject(any());
        commitCfList.get(1).complete(new CommitWALObjectResponse());
        cf1.get(1, TimeUnit.SECONDS);
        cf2.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testRecoverContinuousRecords() {
        List<WriteAheadLog.RecoverResult> recoverResults = List.of(
                new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 10L)).nioBuffer()),
                new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 11L)).nioBuffer()),
                new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 12L)).nioBuffer()),
                new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(233L, 15L)).nioBuffer()),
                new TestRecoverResult(StreamRecordBatchCodec.encode(newRecord(234L, 20L)).nioBuffer())
        );

        List<StreamMetadata> openingStreams = List.of(new StreamMetadata(233L, 0L, 0L, 11L, StreamState.OPENED));
        LogCache.LogCacheBlock cacheBlock = storage.recoverContinuousRecords(recoverResults.iterator(), openingStreams);
        // ignore closed stream and noncontinuous records.
        assertEquals(1, cacheBlock.records().size());
        List<StreamRecordBatch> streamRecords = cacheBlock.records().get(233L);
        assertEquals(2, streamRecords.size());
        assertEquals(11L, streamRecords.get(0).getBaseOffset());
        assertEquals(12L, streamRecords.get(1).getBaseOffset());


        //
        openingStreams = List.of(
                new StreamMetadata(233L, 0L, 0L, 5L, StreamState.OPENED));
        boolean exception = false;
        try {
            storage.recoverContinuousRecords(recoverResults.iterator(), openingStreams);
        } catch (IllegalStateException e) {
            exception = true;
        }
        Assertions.assertTrue(exception);
    }

    private static StreamRecordBatch newRecord(long streamId, long offset) {
        return new StreamRecordBatch(streamId, 0, offset, 1, random(1));
    }

    static class TestRecoverResult implements WriteAheadLog.RecoverResult {
        private final ByteBuffer record;

        public TestRecoverResult(ByteBuffer record) {
            this.record = record;
        }

        @Override
        public ByteBuffer record() {
            return record;
        }

        @Override
        public long recordOffset() {
            return 0;
        }
    }
}
