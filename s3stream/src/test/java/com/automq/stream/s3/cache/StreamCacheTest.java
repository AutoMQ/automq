/*
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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.model.StreamRecordBatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.NavigableMap;

public class StreamCacheTest {

    @Test
    public void testTailMap() {
        StreamCache streamCache = new StreamCache();
        BlockCache.CacheBlock block1 = new BlockCache.CacheBlock(List.of(
                new StreamRecordBatch(0, 0, 0, 10, TestUtils.random(8)),
                new StreamRecordBatch(0, 0, 10, 20, TestUtils.random(8))), null);
        streamCache.put(block1);

        BlockCache.CacheBlock block2 = new BlockCache.CacheBlock(List.of(
                new StreamRecordBatch(0, 0, 50, 20, TestUtils.random(8)),
                new StreamRecordBatch(0, 0, 70, 30, TestUtils.random(8))), null);
        streamCache.put(block2);

        BlockCache.CacheBlock block3 = new BlockCache.CacheBlock(List.of(
                new StreamRecordBatch(0, 0, 30, 20, TestUtils.random(8))), null);
        streamCache.put(block3);

        NavigableMap<Long, BlockCache.CacheBlock> tailBlocks = streamCache.tailBlocks(5);
        Assertions.assertEquals(streamCache.blocks(), tailBlocks);

        tailBlocks = streamCache.tailBlocks(80);
        Assertions.assertEquals(1, tailBlocks.size());
        Assertions.assertEquals(50, tailBlocks.firstKey());
        Assertions.assertEquals(block2, tailBlocks.firstEntry().getValue());
    }

    @Test
    public void testRemove() {
        StreamCache streamCache = new StreamCache();
        streamCache.put(new BlockCache.CacheBlock(List.of(
                new StreamRecordBatch(0, 0, 0, 10, TestUtils.random(8)),
                new StreamRecordBatch(0, 0, 10, 20, TestUtils.random(8))), null));

        streamCache.put(new BlockCache.CacheBlock(List.of(
                new StreamRecordBatch(0, 0, 50, 20, TestUtils.random(8)),
                new StreamRecordBatch(0, 0, 70, 30, TestUtils.random(8))), null));

        streamCache.put(new BlockCache.CacheBlock(List.of(
                new StreamRecordBatch(0, 0, 30, 20, TestUtils.random(8))), null));

        streamCache.remove(30);

    }
}
