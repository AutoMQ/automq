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

package kafka.log.s3;

import kafka.log.s3.cache.DefaultS3BlockCache;
import kafka.log.s3.cache.LogCache;
import kafka.log.s3.cache.ReadDataBlock;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.CommitWALObjectResponse;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.wal.MemoryWriteAheadLog;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class S3StorageTest {
    ObjectManager objectManager;
    S3Storage storage;

    @BeforeEach
    public void setup() {
        objectManager = mock(ObjectManager.class);
        S3Operator s3Operator = new MemoryS3Operator();
        storage = new S3Storage(KafkaConfig.fromProps(TestUtils.defaultBrokerConfig()), new MemoryWriteAheadLog(), objectManager, new DefaultS3BlockCache(objectManager, s3Operator), s3Operator);
    }

    @Test
    public void testAppend() throws Exception {
        when(objectManager.prepareObject(eq(1), anyLong())).thenReturn(CompletableFuture.completedFuture(16L));
        CommitWALObjectResponse resp = new CommitWALObjectResponse();
        when(objectManager.commitWALObject(any())).thenReturn(CompletableFuture.completedFuture(resp));

        CompletableFuture<Void> cf1 = storage.append(new StreamRecordBatch(233, 1, 10, DefaultRecordBatch.of(1, 100)));
        CompletableFuture<Void> cf2 = storage.append(new StreamRecordBatch(233, 1, 11, DefaultRecordBatch.of(2, 100)));
        CompletableFuture<Void> cf3 = storage.append(new StreamRecordBatch(234, 3, 100, DefaultRecordBatch.of(1, 100)));

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
        List<CompletableFuture<Long>> objectIdCfList = new LinkedList<>();
        doAnswer(invocation -> {
            CompletableFuture<Long> objectIdCf = new CompletableFuture<>();
            objectIdCfList.add(objectIdCf);
            return objectIdCf;
        }).when(objectManager).prepareObject(anyInt(), anyLong());

        LogCache.LogCacheBlock logCacheBlock1 = new LogCache.LogCacheBlock(1024);
        logCacheBlock1.put(newRecord(233L, 10L));
        logCacheBlock1.confirmOffset(10L);
        CompletableFuture<Void> cf1 = storage.uploadWALObject(logCacheBlock1);

        LogCache.LogCacheBlock logCacheBlock2 = new LogCache.LogCacheBlock(1024);
        logCacheBlock2.put(newRecord(233L, 20L));
        logCacheBlock2.confirmOffset(20L);
        CompletableFuture<Void> cf2 = storage.uploadWALObject(logCacheBlock2);

        // sequence get objectId
        verify(objectManager, timeout(1000).times(1)).prepareObject(anyInt(), anyLong());

        List<CompletableFuture<CommitWALObjectResponse>> commitCfList = new LinkedList<>();
        doAnswer(invocation -> {
            CompletableFuture<CommitWALObjectResponse> cf = new CompletableFuture<>();
            commitCfList.add(cf);
            return cf;
        }).when(objectManager).commitWALObject(any());

        objectIdCfList.get(0).complete(1L);
        // trigger next upload prepare objectId
        verify(objectManager, timeout(1000).times(2)).prepareObject(anyInt(), anyLong());
        verify(objectManager, times(1)).commitWALObject(any());

        objectIdCfList.get(1).complete(2L);
        Thread.sleep(10);
        verify(objectManager, times(1)).commitWALObject(any());

        commitCfList.get(0).complete(new CommitWALObjectResponse());
        verify(objectManager, timeout(1000).times(2)).commitWALObject(any());
        commitCfList.get(1).complete(new CommitWALObjectResponse());
        cf1.get(1, TimeUnit.SECONDS);
        cf2.get(1, TimeUnit.SECONDS);
    }

    private static FlatStreamRecordBatch newRecord(long streamId, long offset) {
        StreamRecordBatch recordBatch = new StreamRecordBatch(streamId, 0, offset, DefaultRecordBatch.of(1, 1024));
        return FlatStreamRecordBatch.from(recordBatch);
    }
}
