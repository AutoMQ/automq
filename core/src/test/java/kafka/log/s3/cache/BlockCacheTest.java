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

package kafka.log.s3.cache;

import kafka.log.s3.TestUtils;
import kafka.log.s3.model.StreamRecordBatch;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BlockCacheTest {

    @Test
    public void testPutGet() {
        BlockCache blockCache = new BlockCache(1024);

        blockCache.put(233L, List.of(
                newRecord(233L, 10L, 2, 1),
                newRecord(233L, 12L, 2, 1)
        ));

        blockCache.put(233L, List.of(
                newRecord(233L, 16L, 4, 1),
                newRecord(233L, 20L, 2, 1)
        ));

        // overlap
        blockCache.put(233L, List.of(
                newRecord(233L, 12L, 2, 1),
                newRecord(233L, 14L, 2, 1),
                newRecord(233L, 16L, 4, 1)
        ));

        BlockCache.GetCacheResult rst = blockCache.get(233L, 10L, 20L, 1024);
        List<StreamRecordBatch> records = rst.getRecords();
        assertEquals(4, records.size());
        assertEquals(10L, records.get(0).getBaseOffset());
        assertEquals(12L, records.get(1).getBaseOffset());
        assertEquals(14L, records.get(2).getBaseOffset());
        assertEquals(16L, records.get(3).getBaseOffset());
    }

    @Test
    public void testEvict() {
        BlockCache blockCache = new BlockCache(4);
        blockCache.put(233L, List.of(
                newRecord(233L, 10L, 2, 2),
                newRecord(233L, 12L, 2, 1)
        ));

        assertEquals(2, blockCache.get(233L, 10L, 20L, 1000).getRecords().size());

        blockCache.put(233L, List.of(
                newRecord(233L, 16L, 4, 1),
                newRecord(233L, 20L, 2, 1)
        ));
        assertEquals(0, blockCache.get(233L, 10L, 20L, 1000).getRecords().size());
        assertEquals(2, blockCache.get(233L, 16, 21L, 1000).getRecords().size());
    }

    @Test
    public void testLRU() {
        LRUCache<Long, Boolean> lru = new LRUCache<>();
        lru.put(1L, true);
        lru.put(2L, true);
        lru.put(3L, true);
        lru.touch(2L);
        assertEquals(1, lru.pop().getKey());
        assertEquals(3, lru.pop().getKey());
        assertEquals(2, lru.pop().getKey());
        assertNull(lru.pop());
    }

    private static StreamRecordBatch newRecord(long streamId, long offset, int count, int size) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(size));
    }

}
