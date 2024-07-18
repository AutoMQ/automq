/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.index;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeRangeIndexCacheTest {

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
        // refresh cache
        NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50, () -> CompletableFuture.completedFuture(streamRangeMap0));

        Assertions.assertTrue(NodeRangeIndexCache.getInstance().isValid(node0));
        Assertions.assertFalse(NodeRangeIndexCache.getInstance().isValid(node1));
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node1, stream0, 50,
            () -> CompletableFuture.completedFuture(Collections.emptyMap())).join());
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream1, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0)).join());
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 0,
            () -> CompletableFuture.completedFuture(streamRangeMap0)).join());
        Assertions.assertEquals(object0, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap0)).join());
        Assertions.assertEquals(object0, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 100,
            () -> CompletableFuture.completedFuture(streamRangeMap0)).join());
        Assertions.assertEquals(object1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 200,
            () -> CompletableFuture.completedFuture(streamRangeMap0)).join());
        Assertions.assertEquals(object2, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 300,
            () -> CompletableFuture.completedFuture(streamRangeMap0)).join());
        Assertions.assertEquals(object2, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 500,
            () -> CompletableFuture.completedFuture(streamRangeMap0)).join());

        NodeRangeIndexCache.getInstance().invalidate(node0);
        Map<Long, List<RangeIndex>> streamRangeMap1 = Map.of(stream0, List.of(
            new RangeIndex(50, 300, object3),
            new RangeIndex(500, 600, object4)));
        Assertions.assertFalse(NodeRangeIndexCache.getInstance().isValid(node0));
        Assertions.assertEquals(-1, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 0,
            () -> CompletableFuture.completedFuture(streamRangeMap1)).join());
        Assertions.assertEquals(object3, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 50,
            () -> CompletableFuture.completedFuture(streamRangeMap1)).join());
        Assertions.assertEquals(object3, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 400,
            () -> CompletableFuture.completedFuture(streamRangeMap1)).join());
        Assertions.assertEquals(object4, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 500,
            () -> CompletableFuture.completedFuture(streamRangeMap1)).join());
        Assertions.assertEquals(object4, NodeRangeIndexCache.getInstance().searchObjectId(node0, stream0, 1000,
            () -> CompletableFuture.completedFuture(streamRangeMap1)).join());
    }
}
