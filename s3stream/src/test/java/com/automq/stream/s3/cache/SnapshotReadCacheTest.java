/*
 * Copyright 2026, AutoMQ HK Limited.
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

import com.automq.stream.api.LinkRecordDecoder;
import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3ReadPurposeContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.threads.EventLoop;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Verifies replay purpose is scoped at decoding after an asynchronous WAL read. */
@Tag("S3Unit")
@Timeout(10)
class SnapshotReadCacheTest {
    /** Delayed WAL completion establishes replay purpose only for decoding and restores the callback thread's context. */
    @Test
    void scopesPurposeInsideReplayCallback() throws Exception {
        try (MockedConstruction<EventLoop> ignored = mockConstruction(EventLoop.class)) {
            WriteAheadLog wal = mock(WriteAheadLog.class);
            RecordOffset start = mock(RecordOffset.class);
            RecordOffset end = mock(RecordOffset.class);
            CompletableFuture<List<StreamRecordBatch>> walRead = new CompletableFuture<>();
            when(wal.get(start, end)).thenReturn(walRead);
            StreamRecordBatch link = mock(StreamRecordBatch.class);
            when(link.getCount()).thenReturn(-1);
            StreamRecordBatch decoded = mock(StreamRecordBatch.class);
            LinkRecordDecoder decoder = mock(LinkRecordDecoder.class);
            CompletableFuture<StreamRecordBatch> payloadRead = new CompletableFuture<>();
            when(decoder.decode(eq(link), any())).thenAnswer(invocation -> {
                assertEquals(S3ReadPurpose.SNAPSHOT_REPLAY, S3ReadPurposeContext.current());
                return payloadRead;
            });
            SnapshotReadCache cache = new SnapshotReadCache(mock(StreamManager.class), mock(LogCache.class),
                mock(ObjectStorage.class), decoder);
            SnapshotReadCache.WalReplayTask task = cache.new WalReplayTask(wal, start, end, null);
            task.run();
            verifyNoInteractions(decoder);
            S3ReadPurposeContext.withPurpose(S3ReadPurpose.ROUTER_PRODUCE, () -> {
                walRead.complete(List.of(link));
                assertEquals(S3ReadPurpose.ROUTER_PRODUCE, S3ReadPurposeContext.current());
                return null;
            });
            assertEquals(S3ReadPurpose.OTHER, S3ReadPurposeContext.current());
            assertFalse(task.loadCf.isDone());
            payloadRead.complete(decoded);
            task.loadCf.get(5, TimeUnit.SECONDS);
            verify(link).release();
            assertEquals(List.of(decoded), task.records);
            assertEquals(S3ReadPurpose.OTHER, S3ReadPurposeContext.current());
        }
    }
}
