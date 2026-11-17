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

import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class ObjectReservationServiceTest {
    private ObjectReservationService reservationService;
    private ObjectStorage objectStorage;

    @BeforeEach
    public void setUp() {
        objectStorage = new MemoryObjectStorage();
        reservationService = new ObjectReservationService("cluster", objectStorage, (short) 0);
    }

    /** Ownership verification requests an independent GET instead of reusing an older reservation snapshot. */
    @Test
    void verifyRequiresFreshRead() {
        ObjectStorage storage = mock(ObjectStorage.class);
        ByteBuf reservation = Unpooled.buffer(ObjectReservationService.S3_RESERVATION_OBJECT_LENGTH);
        reservation.writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE);
        reservation.writeLong(1);
        reservation.writeLong(2);
        reservation.writeBoolean(false);
        when(storage.rangeRead(argThat(ObjectStorage.ReadOptions::freshRead), anyString(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(reservation));
        assertTrue(new ObjectReservationService("cluster", storage, (short) 0).verify(1, 2, false).join());
    }

    @Test
    void verify() {
        reservationService.acquire(1, 2, false).join();
        assertTrue(reservationService.verify(1, 2, false).join());
        assertFalse(reservationService.verify(1, 2, true).join());

        assertFalse(reservationService.verify(1, 1, false).join());
        assertFalse(reservationService.verify(1, 3, false).join());
        assertFalse(reservationService.verify(2, 2, false).join());

        reservationService.acquire(1, 2, true).join();
        assertFalse(reservationService.verify(1, 2, false).join());
        assertTrue(reservationService.verify(1, 2, true).join());
    }

    @Test
    void acquire() {
        ByteBuf target = Unpooled.buffer(Long.BYTES * 10);
        target.writeLong(Long.MAX_VALUE);
        target.writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(1);
        target.writeLong(2);
        target.writeBoolean(false);
        target.writeLong(Long.MAX_VALUE);
        target.readerIndex(Long.BYTES);
        target.writerIndex(Long.BYTES + ObjectReservationService.S3_RESERVATION_OBJECT_LENGTH);
        reservationService.acquire(1, 2, false).join();
        assertTrue(reservationService.verify(1, target).join());

        target = Unpooled.buffer(Long.BYTES * 10);
        target.writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(1);
        target.writeLong(2);
        target.writeBoolean(false);
        reservationService.acquire(1, 2, true).join();
        assertFalse(reservationService.verify(1, target).join());

        target = Unpooled.buffer(Long.BYTES * 10);
        target.writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(1);
        target.writeLong(2);
        target.writeBoolean(true);
        reservationService.acquire(1, 2, true).join();
        assertTrue(reservationService.verify(1, target).join());

        target = Unpooled.buffer(Long.BYTES * 10);
        target.writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(1);
        target.writeLong(2);
        target.writeBoolean(true);
        reservationService.acquire(1, 2, false).join();
        assertFalse(reservationService.verify(1, target).join());
    }
}
