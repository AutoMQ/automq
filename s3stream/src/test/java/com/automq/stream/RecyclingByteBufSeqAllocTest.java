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

package com.automq.stream;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.ByteBufAllocPolicy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
class RecyclingByteBufSeqAllocTest {
    private static final int SLICE_SIZE = 3 << 20;
    private static final long TTL_NANOS = 100;

    private ByteBufAllocPolicy previousPolicy;

    @BeforeEach
    void setUp() {
        previousPolicy = ByteBufAlloc.getPolicy();
        ByteBufAlloc.setPolicy(ByteBufAllocPolicy.POOLED_HEAP);
    }

    @AfterEach
    void tearDown() {
        ByteBufAlloc.setPolicy(previousPolicy);
    }

    /**
     * Given a slab-sized request, when a slice is allocated, then it has exact writable bounds and zero indexes.
     */
    @Test
    void testAllocateExactWritableSlice() {
        try (RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1)) {
            ByteBuf buf = alloc.alloc(32);
            assertEquals(32, buf.capacity());
            assertEquals(32, buf.maxCapacity());
            assertEquals(0, buf.readerIndex());
            assertEquals(0, buf.writerIndex());
            assertFalse(buf.isDirect());
            buf.release();
        }
    }

    /**
     * Given a retired slab with no live slices, when another slab is needed, then the retired slab is reused.
     */
    @Test
    void testReuseReleasedSlab() {
        try (RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1)) {
            ByteBuf first = alloc.alloc(SLICE_SIZE);
            ByteBuf firstOwner = first.unwrap();
            ByteBuf second = alloc.alloc(SLICE_SIZE);
            first.release();

            ByteBuf third = alloc.alloc(SLICE_SIZE);
            assertSame(firstOwner, third.unwrap());

            second.release();
            third.release();
        }
    }

    /**
     * Given a retired slab with a live slice, when another slab is needed, then the pinned slab is not reused.
     */
    @Test
    void testDoNotReusePinnedSlab() {
        try (RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1)) {
            ByteBuf first = alloc.alloc(SLICE_SIZE);
            ByteBuf firstOwner = first.unwrap();
            ByteBuf second = alloc.alloc(SLICE_SIZE);

            ByteBuf third = alloc.alloc(SLICE_SIZE);
            assertNotSame(firstOwner, third.unwrap());

            first.release();
            second.release();
            third.release();
        }
    }

    /**
     * Given a request larger than a heap slab, when allocated, then a standalone exact-size buffer is returned.
     */
    @Test
    void testAllocateStandaloneBuffer() {
        try (RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1)) {
            int capacity = 4 << 20;
            ByteBuf buf = alloc.alloc(capacity);
            assertEquals(capacity, buf.capacity());
            assertEquals(0, buf.readerIndex());
            assertEquals(0, buf.writerIndex());
            buf.release();
            assertEquals(0, buf.refCnt());
        }
    }

    /**
     * Given the direct policy, when a slice is allocated, then its slab backing is direct memory.
     */
    @Test
    void testAllocateDirectSlab() {
        ByteBufAlloc.setPolicy(ByteBufAllocPolicy.POOLED_DIRECT);
        try (RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1)) {
            ByteBuf buf = alloc.alloc(32);
            assertTrue(buf.isDirect());
            assertEquals(32, buf.capacity());
            buf.release();
        }
    }

    /**
     * Given a live slice, when its allocator is closed, then the caller-owned slice remains valid.
     */
    @Test
    void testCloseKeepsCallerSliceValid() {
        RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1);
        ByteBuf buf = alloc.alloc(Integer.BYTES);
        buf.writeInt(42);

        alloc.close();
        assertEquals(1, buf.refCnt());
        assertEquals(42, buf.readInt());
        buf.release();
        assertEquals(0, buf.refCnt());
    }

    /**
     * Given a closed allocator, when another allocation is requested, then the request is rejected.
     */
    @Test
    void testRejectAllocationAfterClose() {
        RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1);
        alloc.close();
        assertThrows(IllegalStateException.class, () -> alloc.alloc(1));
    }

    /**
     * Given an expired free slab, when scheduled maintenance runs, then the recycler owner is released.
     */
    @Test
    void testScheduledMaintenanceReleasesExpiredFreeSlab() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<Object> future = mock(ScheduledFuture.class);
        doReturn(future).when(scheduler)
            .scheduleWithFixedDelay(any(), anyLong(), anyLong(), eq(TimeUnit.NANOSECONDS));
        AtomicLong now = new AtomicLong();

        RecyclingByteBufSeqAlloc alloc = new RecyclingByteBufSeqAlloc(ByteBufAlloc.DEFAULT, 1, TTL_NANOS,
            scheduler, now::get);
        ByteBuf first = alloc.alloc(SLICE_SIZE);
        ByteBuf firstOwner = first.unwrap();
        ByteBuf second = alloc.alloc(SLICE_SIZE);
        first.release();
        second.release();
        ByteBuf current = alloc.alloc(SLICE_SIZE);

        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).scheduleWithFixedDelay(taskCaptor.capture(), eq(TTL_NANOS), eq(TTL_NANOS),
            eq(TimeUnit.NANOSECONDS));
        now.set(TTL_NANOS);
        taskCaptor.getValue().run();
        assertEquals(0, firstOwner.refCnt());

        current.release();
        alloc.close();
    }
}
