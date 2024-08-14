/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
public class AsyncLRUCacheTest {

    @Test
    public void test_evictUncompleted() throws Exception {
        AsyncLRUCache<String, MockValue> cache = new AsyncLRUCache<>(10);
        MockValue v1 = spy(new MockValue());
        cache.put("v1", v1);
        assertEquals(0, cache.completedSet.size());
        assertEquals(0, cache.removedSet.size());

        MockValue v2 = spy(new MockValue());
        cache.put("v2", v2);
        v2.cf.complete(5);
        assertEquals(1, cache.completedSet.size());
        assertEquals(0, cache.removedSet.size());

        MockValue v3 = spy(new MockValue());
        cache.put("v3", v3);
        v3.cf.complete(6);

        // expect evict v1 and v2
        assertEquals(1, cache.completedSet.size());
        assertEquals(1, cache.removedSet.size());
        assertEquals(v1, cache.removedSet.iterator().next());
        assertEquals(6, cache.totalSize.get());

        v1.cf.complete(11);
        assertEquals(1, cache.completedSet.size());
        assertEquals(0, cache.removedSet.size());
        assertEquals(6, cache.totalSize.get());
        verify(v1, times(1)).close();
        verify(v2, times(1)).close();
        verify(v3, times(0)).close();
    }

    @Test
    public void testPut_repeat() throws Exception {
        AsyncLRUCache<String, MockValue> cache = new AsyncLRUCache<>(10);
        MockValue v1 = spy(new MockValue());
        cache.put("v1", v1);
        v1.cf.complete(10);

        MockValue v1b = spy(new MockValue());
        cache.put("v1", v1b);
        v1b.cf.complete(12);

        verify(v1, times(1)).close();
        verify(v1b, times(0)).close();
        assertEquals(12, cache.totalSize.get());
    }

    @Test
    public void testRemove() throws Exception {
        AsyncLRUCache<String, MockValue> cache = new AsyncLRUCache<>(10);
        MockValue v1 = spy(new MockValue());
        cache.put("v1", v1);
        v1.cf.complete(10);


        cache.remove("v1");
        verify(v1, times(1)).close();
        assertEquals(0, cache.totalSize.get());
        assertEquals(0, cache.removedSet.size());

        MockValue v2 = spy(new MockValue());
        cache.put("v2", v2);
        v2.cf.complete(5);
        assertEquals(5, cache.totalSize.get());
        assertEquals(1, cache.completedSet.size());

        MockValue v3 = spy(new MockValue());
        cache.put("v3", v3);
        cache.remove("v3");
        assertEquals(5, cache.totalSize.get());
        assertEquals(1, cache.removedSet.size());
        verify(v3, times(0)).close();
        v3.cf.complete(5);
        assertEquals(5, cache.totalSize.get());
        assertEquals(0, cache.removedSet.size());
        assertEquals(1, cache.completedSet.size());
        verify(v3, times(1)).close();
    }


    @Test
    public void test_asyncFail() {
        AsyncLRUCache<String, MockValue> cache = new AsyncLRUCache<>(10);
        MockValue v1 = new MockValue();
        cache.put("v1", v1);
        Assertions.assertEquals(1, cache.size());
        v1.cf.completeExceptionally(new RuntimeException());
        Assertions.assertEquals(0, cache.size());
        Assertions.assertEquals(0, cache.totalSize());
        assertEquals(0, cache.completedSet.size());
        assertEquals(0, cache.removedSet.size());
    }

    static class MockValue implements AsyncMeasurable {
        CompletableFuture<Integer> cf = new CompletableFuture<>();

        @Override
        public CompletableFuture<Integer> size() {
            return cf;
        }

        @Override
        public void close() throws Exception {

        }
    }

}
