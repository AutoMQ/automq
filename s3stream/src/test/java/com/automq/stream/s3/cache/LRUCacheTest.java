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

package com.automq.stream.s3.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("S3Unit")
public class LRUCacheTest {
    @Test
    public void testPeek() {
        LRUCache<String, String> cache = new LRUCache<>();
        cache.put("a", "a1");
        cache.put("b", "b1");
        cache.put("c", "c1");

        Assertions.assertEquals("a1", cache.peek().getValue());
        // peek should not change the order
        Assertions.assertEquals("a1", cache.peek().getValue());

        cache.put("a", "a2");
        Assertions.assertEquals("b1", cache.peek().getValue());
    }

}
