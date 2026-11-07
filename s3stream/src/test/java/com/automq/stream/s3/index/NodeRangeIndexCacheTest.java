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

package com.automq.stream.s3.index;

import com.automq.stream.utils.MockTime;
import com.google.common.base.Ticker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NodeRangeIndexCacheTest {

    @BeforeEach
    public void setUp() {
        NodeRangeIndexCache.getInstance().clear();
    }

    @Test
    public void testIndex() {
        int node0 = 32;
        int node1 = 33;
        long stream0 = 0;
        long stream1 = 1;
        int object0 = 99;
        int object1 = 100;
        int object2 = 101;
        int object3 = 102;
        int object4 = 103;

        Map<Long, List<RangeIndex>> streamRangeMap0 = Map.of(stream0, List.of(
                new RangeIndex(50, 100, object0),
                new RangeIndex(150, 250, object1),
                new RangeIndex(300, 400, object2)));

        MockTime time = new MockTime();
        // refresh cache
        NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time);

        Assertions.assertTrue(NodeRangeIndexCache.getInstance().isValid(node0));
        Assertions.assertFalse(NodeRangeIndexCache.getInstance().isValid(node1));
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node1, stream0, 50,
            () -> CompletableFuture.completedFuture(Collections.emptyMap()), time).join());
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream1, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time).join());
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 0,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time).join());
        Assertions.assertEquals(object0, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time).join());
        Assertions.assertEquals(object0, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 100,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time).join());
        Assertions.assertEquals(object1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 200,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time).join());
        Assertions.assertEquals(object2, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 300,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time).join());
        Assertions.assertEquals(object2, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 500,
            () -> CompletableFuture.completedFuture(streamRangeMap0), time).join());

        NodeRangeIndexCache.getInstance().invalidate(node0);
        time.setCurrentTimeMs(time.milliseconds() + NodeRangeIndexCache.MIN_CACHE_UPDATE_INTERVAL_MS);
        Map<Long, List<RangeIndex>> streamRangeMap1 = Map.of(stream0, List.of(
            new RangeIndex(50, 300, object3),
            new RangeIndex(500, 600, object4)));
        Assertions.assertFalse(NodeRangeIndexCache.getInstance().isValid(node0));
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 0,
            () -> CompletableFuture.completedFuture(streamRangeMap1), time).join());
        Assertions.assertEquals(object3, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap1), time).join());
        Assertions.assertEquals(object3, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 400,
            () -> CompletableFuture.completedFuture(streamRangeMap1), time).join());
        Assertions.assertEquals(object4, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 500,
            () -> CompletableFuture.completedFuture(streamRangeMap1), time).join());
        Assertions.assertEquals(object4, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 1000,
            () -> CompletableFuture.completedFuture(streamRangeMap1), time).join());
    }

    @Test
    public void testLRUCache() throws InterruptedException {
        Random r = new Random();
        List<CompletableFuture<Long>> cfs = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            CompletableFuture.runAsync(() -> cfs.add(NodeRangeIndexCache.getInstance().searchObjectId(finalI, 0, 0,
                () -> CompletableFuture.supplyAsync(() -> Map.of(0L, createRangeIndex(1024 * 1024)),
                    CompletableFuture.delayedExecutor(r.nextInt(1000), TimeUnit.MILLISECONDS)))));
        }
        CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0])).join();
        Thread.sleep(1000);
        Assertions.assertTrue(NodeRangeIndexCache.getInstance().cache().totalSize() - 100 * 1024 * 1024 <= 1000 * Long.BYTES);
    }

    @Test
    public void testUpdateCacheFrequency() {
        int node0 = 32;
        long stream0 = 0;
        int object0 = 99;
        Map<Long, List<RangeIndex>> streamRangeMap0 = Map.of(stream0, List.of(
            new RangeIndex(50, 100, object0)));

        MockTime mockTime = new MockTime();
        // refresh cache
        Assertions.assertEquals(object0, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0), mockTime).join());
        NodeRangeIndexCache.getInstance().invalidate(node0);

        CompletableFuture<Long> cf = NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0), mockTime);
        Assertions.assertTrue(cf.isDone());
        Assertions.assertEquals(-1L, cf.join());

        mockTime.setCurrentTimeMs(mockTime.milliseconds() + NodeRangeIndexCache.MIN_CACHE_UPDATE_INTERVAL_MS);
        cf = NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0), mockTime);
        Assertions.assertTrue(cf.isDone());
        Assertions.assertEquals(object0, cf.join());
    }

    @Test
    public void testExpireCache() {
        AtomicLong time = new AtomicLong(System.nanoTime());
        Ticker mockTicker = new Ticker() {
            @Override
            public long read() {
                return time.get();
            }
        };
        NodeRangeIndexCache.ExpireLRUCache cache = new NodeRangeIndexCache.ExpireLRUCache(1024, 1000, mockTicker);
        Assertions.assertNull(cache.get(0L));
        cache.put(0L, new NodeRangeIndexCache.StreamRangeIndexCache(CompletableFuture.completedFuture(Collections.emptyMap())));
        Assertions.assertNotNull(cache.get(0L));
        time.addAndGet(TimeUnit.MILLISECONDS.toNanos(1001));
        Assertions.assertNull(cache.get(0L));
    }

    private List<RangeIndex> createRangeIndex(int size) {
        List<RangeIndex> index = new ArrayList<>();
        int curr = 0;
        while (curr < size) {
            index.add(new RangeIndex(0, 0, 0));
            curr += RangeIndex.OBJECT_SIZE;
        }
        return index;
    }
}
