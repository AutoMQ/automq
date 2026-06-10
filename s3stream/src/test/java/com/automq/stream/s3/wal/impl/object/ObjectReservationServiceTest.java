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
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
    void writeBody() {
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
    }

    @Test
    void failoverTakesNormal() {
        ByteBuf normal = reservationRecord(1, 2, false);
        reservationService.acquire(1, 2, false).join();
        reservationService.acquire(1, 2, true).join();

        assertFalse(reservationService.verify(1, normal).join());
        assertTrue(reservationService.verify(1, 2, true).join());
    }

    @Test
    void failoverTakesOlderReservation() {
        reservationService.acquire(1, 2, false).join();

        reservationService.acquire(1, 3, true).join();

        assertTrue(reservationService.verify(1, 3, true).join());
    }

    @Test
    void normalRejectsFailover() {
        reservationService.acquire(1, 2, false).join();
        reservationService.acquire(1, 2, true).join();
        assertAcquireRejected(1, 2, false, "Normal acquire rejected");

        assertTrue(reservationService.verify(1, 2, true).join());
    }

    @Test
    void stateMachine() {
        reservationService.acquire(1, 2, false).join();
        assertTrue(reservationService.verify(1, 2, false).join());

        reservationService.acquire(1, 2, false).join();
        assertTrue(reservationService.verify(1, 2, false).join());

        reservationService.acquire(1, 2, true).join();
        assertTrue(reservationService.verify(1, 2, true).join());

        assertAcquireRejected(1, 2, false, "Normal acquire rejected");
        assertTrue(reservationService.verify(1, 2, true).join());

        reservationService.acquire(1, 3, false).join();
        assertTrue(reservationService.verify(1, 3, false).join());
    }

    @Test
    void failoverCannotCreateMissingReservation() {
        assertAcquireRejected(2, 1, true, "Failover acquire cannot create missing reservation");
        assertFalse(reservationService.verify(2, 1, true).join());
    }

    @Test
    void overwriteInvalid() {
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions();
        objectStorage.write(options, reservationPath(3), invalidReservationRecord(3, 1, false)).join();

        reservationService.acquire(3, 1, false).join();

        assertTrue(reservationService.verify(3, 1, false).join());
    }

    @Test
    void overwriteMismatchedNode() {
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions();
        objectStorage.write(options, reservationPath(4), reservationRecord(5, 1, false)).join();

        reservationService.acquire(4, 1, false).join();

        assertTrue(reservationService.verify(4, 1, false).join());
    }

    private void assertAcquireRejected(long nodeId, long epoch, boolean failover, String messagePart) {
        CompletionException ex = assertThrows(CompletionException.class,
            () -> reservationService.acquire(nodeId, epoch, failover).join());
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains(messagePart));
    }

    private ByteBuf reservationRecord(long nodeId, long epoch, boolean failover) {
        ByteBuf target = Unpooled.buffer(ObjectReservationService.S3_RESERVATION_OBJECT_LENGTH);
        target.writeInt(ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(nodeId);
        target.writeLong(epoch);
        target.writeBoolean(failover);
        return target;
    }

    private ByteBuf invalidReservationRecord(long nodeId, long epoch, boolean failover) {
        ByteBuf target = reservationRecord(nodeId, epoch, failover);
        target.setInt(0, ObjectReservationService.S3_RESERVATION_OBJECT_MAGIC_CODE + 1);
        return target;
    }

    private String reservationPath(long nodeId) {
        return "reservation/_kafka_cluster/" + nodeId;
    }
}
