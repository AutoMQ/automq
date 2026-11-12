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

package kafka.automq.zerozone;

import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3ReadPurposeContext;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Covers metric-context capture while a router read waits for asynchronous WAL startup. */
@Tag("S3Unit")
@Timeout(10)
class ObjectRouterChannelTest {
    /** Reads queued before startup retain each caller's purpose after its initiating scope has ended. */
    @ParameterizedTest
    @EnumSource(value = S3ReadPurpose.class, names = {"OTHER", "ROUTER_PRODUCE", "SNAPSHOT_REPLAY"})
    void retainsPurposeAcrossStartup(S3ReadPurpose purpose) throws Exception {
        ObjectWALService wal = mock(ObjectWALService.class);
        CountDownLatch starting = new CountDownLatch(1);
        CountDownLatch allowStart = new CountDownLatch(1);
        doAnswer(invocation -> {
            starting.countDown();
            assertTrue(allowStart.await(5, TimeUnit.SECONDS));
            return null;
        }).when(wal).start();
        CompletableFuture<S3ReadPurpose> observed = new CompletableFuture<>();
        IllegalStateException failure = new IllegalStateException("missing payload");
        when(wal.get(any(RecordOffset.class))).thenAnswer(invocation -> {
            observed.complete(S3ReadPurposeContext.current());
            return CompletableFuture.failedFuture(failure);
        });
        ObjectRouterChannel channel = new ObjectRouterChannel(1, (short) 0, wal);
        ByteBuf offset = ChannelOffset.of((short) 0, (short) 1, 1, 1, DefaultRecordOffset.of(1, 1, 1).buffer()).byteBuf();
        try {
            assertTrue(starting.await(5, TimeUnit.SECONDS));
            CompletableFuture<ByteBuf> read = S3ReadPurposeContext.withPurpose(purpose, () -> channel.get(offset));
            assertEquals(S3ReadPurpose.OTHER, S3ReadPurposeContext.current());
            assertFalse(read.isDone());
            allowStart.countDown();
            assertEquals(purpose, observed.get(5, TimeUnit.SECONDS));
            assertSame(failure, assertThrows(ExecutionException.class, () -> read.get(5, TimeUnit.SECONDS)).getCause());
        } finally {
            allowStart.countDown();
            channel.close().get(5, TimeUnit.SECONDS);
            offset.release();
        }
    }
}
