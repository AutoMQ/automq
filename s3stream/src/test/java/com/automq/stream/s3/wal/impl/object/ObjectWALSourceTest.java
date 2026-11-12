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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3ReadPurposeContext;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Covers source ownership at the shared WAL append/read and durability-verification boundaries. */
@Tag("S3Unit")
@Timeout(10)
class ObjectWALSourceTest {
    /** Both WALs retain ownership, and RouterWAL retains read purpose across asynchronous reads and layout fallback. */
    @ParameterizedTest
    @CsvSource({"ROUTER_WAL,0,ROUTER_PRODUCE", "ROUTER_WAL,1,ROUTER_PRODUCE",
        "ROUTER_WAL,0,SNAPSHOT_REPLAY", "ROUTER_WAL,1,SNAPSHOT_REPLAY", "DELTA_WAL,0,OTHER", "DELTA_WAL,1,OTHER"})
    void appendAndFallbackRead(S3RequestSource source, int version, S3ReadPurpose readPurpose) throws Exception {
        MemoryObjectStorage storage = spy(new MemoryObjectStorage());
        ObjectWALConfig config = config(source, version).build();
        ObjectWALService wal = new ObjectWALService(Time.SYSTEM, storage, config);
        wal.start();
        try {
            CompletableFuture<AppendResult> append = wal.append(TraceContext.DEFAULT, record());
            assertFalse(append.isDone());
            wal.flush().get(5, TimeUnit.SECONDS);
            AppendResult result = append.get();
            verify(storage).write(argThat(o -> o.source() == source && o.enableFastRetry()), anyString(), any());
            verify(storage, never()).writer(any(), anyString());
            DefaultReader reader = new DefaultReader(storage,
                config(source, 1 - version).withOpenMode(OpenMode.READ_ONLY).build(), Time.SYSTEM);
            CompletableFuture<StreamRecordBatch> pending = S3ReadPurposeContext.withPurpose(readPurpose,
                () -> reader.get(result.recordOffset()));
            assertEquals(S3ReadPurpose.OTHER, S3ReadPurposeContext.current());
            StreamRecordBatch read = pending.get(5, TimeUnit.SECONDS);
            try {
                assertEquals(1, read.getCount());
            } finally {
                read.release();
            }
            verify(storage, times(2)).rangeRead(argThat(o -> o.source() == source && o.readPurpose() == readPurpose),
                anyString(), anyLong(), anyLong());
        } finally {
            wal.shutdownGracefully();
            storage.close();
        }
    }

    /** Delta append completion waits for its labeled ownership GET, including the fenced result. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void appendWaitsForReservation(boolean valid) throws Exception {
        MemoryObjectStorage storage = spy(new MemoryObjectStorage());
        ObjectReservationService reservation = new ObjectReservationService("cluster", storage, (short) 0, S3RequestSource.DELTA_WAL);
        reservation.acquire(1, 2, false).get();
        ObjectWALService wal = new ObjectWALService(Time.SYSTEM, storage,
            config(S3RequestSource.DELTA_WAL, 0).withReservationService(reservation).build());
        wal.start();
        clearInvocations(storage);
        CompletableFuture<ByteBuf> verification = new CompletableFuture<>();
        String path = "reservation/_kafka_cluster/1";
        doReturn(verification).when(storage).rangeRead(any(), eq(path), eq(0L), eq((long) ObjectReservationService.S3_RESERVATION_OBJECT_LENGTH));
        try {
            CompletableFuture<AppendResult> append = wal.append(TraceContext.DEFAULT, record());
            CompletableFuture<Void> flush = wal.flush();
            verify(storage, timeout(1000)).rangeRead(argThat(o -> o.source() == S3RequestSource.DELTA_WAL),
                eq(path), eq(0L), eq((long) ObjectReservationService.S3_RESERVATION_OBJECT_LENGTH));
            assertFalse(append.isDone());
            verification.complete(Unpooled.buffer(ObjectReservationService.S3_RESERVATION_OBJECT_LENGTH)
                .writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE).writeLong(1)
                .writeLong(valid ? 2 : 3).writeBoolean(false));
            if (valid) {
                append.get(5, TimeUnit.SECONDS);
                flush.get(5, TimeUnit.SECONDS);
            } else {
                assertThrows(ExecutionException.class, () -> append.get(5, TimeUnit.SECONDS));
            }
        } finally {
            wal.shutdownGracefully();
            storage.close();
        }
    }

    private static ObjectWALConfig.Builder config(S3RequestSource source, int version) {
        return ObjectWALConfig.builder().withSource(source).withVersion(version).withClusterId("cluster")
            .withNodeId(1).withEpoch(2).withManualMode(true);
    }

    private static StreamRecordBatch record() {
        return StreamRecordBatch.of(1, 0, 0, 1, Unpooled.buffer(16).writeZero(16), DefaultByteBufSupplier.INSTANCE);
    }
}
