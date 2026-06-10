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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.exceptions.ObjectStorageConditionNotMetException;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.FutureUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectReservationServiceTest {
    private ObjectReservationService reservationService;
    private ObjectStorage objectStorage;

    @BeforeEach
    public void setUp() {
        objectStorage = new MemoryObjectStorage();
        reservationService = new ObjectReservationService("cluster", objectStorage, (short) 0);
    }

    @Test
    void verifyKeepsBodyCompareSemantics() {
        reservationService.acquire(1, 2, false).join();
        assertTrue(reservationService.verify(1, 2, false).join());
        assertFalse(reservationService.verify(1, 2, true).join());

        assertFalse(reservationService.verify(1, 1, false).join());
        assertFalse(reservationService.verify(1, 3, false).join());
        assertFalse(reservationService.verify(2, 2, false).join());
    }

    @Test
    void staleFailoverCannotFenceNewerOwner() {
        reservationService.acquire(1, 3, false).join();

        assertThrows(CompletionException.class, () -> reservationService.acquire(1, 2, true).join());

        assertTrue(reservationService.verify(1, 3, false).join());
        assertFalse(reservationService.verify(1, 2, true).join());
    }

    @Test
    void validFailoverFencesSameEpochNormalOwnerOnce() {
        reservationService.acquire(1, 2, false).join();

        reservationService.acquire(1, 2, true).join();
        reservationService.acquire(1, 2, true).join();

        assertTrue(reservationService.verify(1, 2, true).join());
        assertFalse(reservationService.verify(1, 2, false).join());
    }

    @Test
    void normalOwnerMovesEpochForwardButNeverBackward() {
        reservationService.acquire(1, 2, false).join();
        reservationService.acquire(1, 2, true).join();

        reservationService.acquire(1, 3, false).join();
        assertTrue(reservationService.verify(1, 3, false).join());

        assertThrows(CompletionException.class, () -> reservationService.acquire(1, 2, false).join());
        assertTrue(reservationService.verify(1, 3, false).join());
    }

    @Test
    void sameEpochNormalAcquireIsIdempotentAndDoesNotClearFailover() {
        reservationService.acquire(1, 2, false).join();
        reservationService.acquire(1, 2, false).join();
        assertTrue(reservationService.verify(1, 2, false).join());

        reservationService.acquire(1, 2, true).join();
        assertThrows(CompletionException.class, () -> reservationService.acquire(1, 2, false).join());
        assertTrue(reservationService.verify(1, 2, true).join());
    }

    @Test
    void missingReservationOnlyNormalAcquireCanCreate() {
        assertThrows(CompletionException.class, () -> reservationService.acquire(1, 2, true).join());

        reservationService.acquire(1, 2, false).join();
        assertTrue(reservationService.verify(1, 2, false).join());
    }

    @Test
    void missingReservationIfAbsentConflictTriggersBoundedReread() {
        AtomicBoolean conflictInjected = new AtomicBoolean();
        ObjectStorage storage = new MemoryObjectStorage() {
            @Override
            public CompletableFuture<WriteResult> conditionalWrite(WriteOptions options, String objectPath,
                ByteBuf buf, WriteCondition condition) {
                if (condition instanceof WriteCondition.IfAbsent && conflictInjected.compareAndSet(false, true)) {
                    buf.release();
                    write(options, objectPath, reservationBody(1, 2, false)).join();
                    return FutureUtil.failedFuture(new ObjectStorageConditionNotMetException("injected if-absent conflict"));
                }
                return super.conditionalWrite(options, objectPath, buf, condition);
            }
        };
        ObjectReservationService service = new ObjectReservationService("cluster", storage, (short) 0);

        service.acquire(1, 2, false).join();

        assertTrue(service.verify(1, 2, false).join());
    }

    @Test
    void malformedNodeIdFailsClosed() {
        objectStorage.write(new ObjectStorage.WriteOptions().throttleStrategy(ThrottleStrategy.BYPASS),
            path(1), reservationBody(2, 2, false)).join();

        assertThrows(CompletionException.class, () -> reservationService.acquire(1, 3, false).join());
        assertTrue(verifyBody(1, 2, 2, false).join());
    }

    @Test
    void conditionalWriteConflictTriggersBoundedReread() {
        AtomicBoolean conflictInjected = new AtomicBoolean();
        ObjectStorage storage = new MemoryObjectStorage() {
            @Override
            public CompletableFuture<WriteResult> conditionalWrite(WriteOptions options, String objectPath,
                ByteBuf buf, WriteCondition condition) {
                if (condition instanceof WriteCondition.IfMatch && conflictInjected.compareAndSet(false, true)) {
                    buf.release();
                    write(options, objectPath, reservationBody(1, 2, true)).join();
                    return FutureUtil.failedFuture(new ObjectStorageConditionNotMetException("injected condition conflict"));
                }
                return super.conditionalWrite(options, objectPath, buf, condition);
            }
        };
        ObjectReservationService service = new ObjectReservationService("cluster", storage, (short) 0);

        service.acquire(1, 2, false).join();
        service.acquire(1, 2, true).join();

        assertTrue(service.verify(1, 2, true).join());
    }

    @Test
    void missingReservationDowngradesWhenConditionalWriteUnsupported() {
        ObjectStorage storage = new MemoryObjectStorage() {
            @Override
            public boolean supportsConditionalWrite() {
                return false;
            }

            @Override
            public CompletableFuture<WriteResult> conditionalWrite(WriteOptions options, String objectPath,
                ByteBuf buf, WriteCondition condition) {
                buf.release();
                return FutureUtil.failedFuture(new AssertionError("conditionalWrite should not be used when unsupported"));
            }
        };
        ObjectReservationService service = new ObjectReservationService("cluster", storage, (short) 0);

        service.acquire(1, 2, false).join();

        assertTrue(service.verify(1, 2, false).join());
    }

    @Test
    void readWithMetadataUnexpectedFailureFailsAcquire() {
        ObjectStorage storage = new MemoryObjectStorage() {
            @Override
            public CompletableFuture<ReadResult> readWithMetadata(ReadOptions options, String objectPath) {
                return FutureUtil.failedFuture(new IllegalStateException("metadata read failed"));
            }
        };
        ObjectReservationService service = new ObjectReservationService("cluster", storage, (short) 0);

        assertThrows(CompletionException.class, () -> service.acquire(1, 2, false).join());
    }

    @Test
    void etagEmptyDowngradesExistingReservationModification() {
        ObjectStorage storage = new MemoryObjectStorage() {
            @Override
            public CompletableFuture<ReadResult> readWithMetadata(ReadOptions options, String objectPath) {
                return rangeRead(options, objectPath, 0, RANGE_READ_TO_END)
                    .thenApply(ReadResult::of);
            }

            @Override
            public CompletableFuture<WriteResult> conditionalWrite(WriteOptions options, String objectPath,
                ByteBuf buf, WriteCondition condition) {
                if (condition instanceof WriteCondition.IfMatch) {
                    buf.release();
                    return FutureUtil.failedFuture(new AssertionError("IfMatch should not be used without etag"));
                }
                return super.conditionalWrite(options, objectPath, buf, condition);
            }
        };
        ObjectReservationService service = new ObjectReservationService("cluster", storage, (short) 0);

        service.acquire(1, 2, false).join();
        service.acquire(1, 2, true).join();

        assertTrue(service.verify(1, 2, true).join());
    }

    @Test
    void existingReservationDowngradesWhenConditionalWriteUnsupported() {
        ObjectStorage storage = new MemoryObjectStorage() {
            @Override
            public boolean supportsConditionalWrite() {
                return false;
            }

            @Override
            public CompletableFuture<WriteResult> conditionalWrite(WriteOptions options, String objectPath,
                ByteBuf buf, WriteCondition condition) {
                buf.release();
                return FutureUtil.failedFuture(new AssertionError("conditionalWrite should not be used when unsupported"));
            }
        };
        ObjectReservationService service = new ObjectReservationService("cluster", storage, (short) 0);

        service.acquire(1, 2, false).join();
        service.acquire(1, 2, true).join();

        assertTrue(service.verify(1, 2, true).join());
    }

    private CompletableFuture<Boolean> verifyBody(long pathNodeId, long bodyNodeId, long epoch, boolean failover) {
        return reservationService.verify(pathNodeId, reservationBody(bodyNodeId, epoch, failover));
    }

    private static ByteBuf reservationBody(long nodeId, long epoch, boolean failover) {
        ByteBuf target = ByteBufAlloc.byteBuffer(ObjectReservationService.S3_RESERVATION_OBJECT_LENGTH);
        target.writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(nodeId);
        target.writeLong(epoch);
        target.writeBoolean(failover);
        return target;
    }

    private static String path(long nodeId) {
        return "reservation/_kafka_cluster/" + nodeId;
    }
}
