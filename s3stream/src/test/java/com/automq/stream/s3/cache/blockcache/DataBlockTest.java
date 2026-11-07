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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@Tag("S3Unit")
public class DataBlockTest {

    /**
     * Regression test for the race in registerFreeListener: a block can be freed with EXPIRED
     * before a listener (e.g. StreamReader) has a chance to register, e.g. when
     * DataBlockCache.Cache#getBlock0 calls tryEvictExpired() right after completing the load
     * future but before the caller registers its free listener. The already-freed branch used to
     * hardcode EvictReason.NONE, which made handleBlockFree fall back to the misleading capacity
     * warning even though the real cause was TTL expiry.
     */
    @Test
    public void testRegisterFreeListenerAfterFree_repliesActualEvictReason() {
        DataBlock block = new DataBlock(1L, new DataBlockIndex(1L, 0L, 100, 1, 0L, 100),
            mock(ReadStatusChangeListener.class), Time.SYSTEM);

        // Simulate the block already being evicted for TTL expiry before any listener registers.
        block.free(DataBlock.EvictReason.EXPIRED);

        AtomicReference<DataBlock.EvictReason> observed = new AtomicReference<>();
        block.registerFreeListener((db, evictReason) -> observed.set(evictReason));

        assertEquals(DataBlock.EvictReason.EXPIRED, observed.get());
    }

    @Test
    public void testRegisterFreeListenerAfterFree_capacityReasonPreserved() {
        DataBlock block = new DataBlock(1L, new DataBlockIndex(1L, 0L, 100, 1, 0L, 100),
            mock(ReadStatusChangeListener.class), Time.SYSTEM);

        block.free(DataBlock.EvictReason.CAPACITY);

        AtomicReference<DataBlock.EvictReason> observed = new AtomicReference<>();
        block.registerFreeListener((db, evictReason) -> observed.set(evictReason));

        assertEquals(DataBlock.EvictReason.CAPACITY, observed.get());
    }
}
