/*
 * Copyright 2026, AutoMQ HK Limited.
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

import com.automq.stream.s3.operator.ObjectStorage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.automq.stream.s3.wal.impl.object.ObjectUtils.DATA_FILE_ALIGN_SIZE;
import static com.automq.stream.s3.wal.impl.object.ObjectUtils.genShardedObjectPathV1;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Protects the deterministic shard layout and the parser's compatibility with older keys. */
@Tag("S3Unit")
public class ObjectUtilsTest {
    /** Shards rotate at 64 MiB boundaries, wrap after all 256 prefixes, and precede the epoch. */
    @Test
    public void testShardPaths() {
        String prefix = ObjectUtils.nodePrefix("clusterA", 1001, "rc");
        assertEquals(prefix + "s00/42/wal/0-67108864", genShardedObjectPathV1(prefix, 42, 0, DATA_FILE_ALIGN_SIZE));
        assertEquals(prefix + "s01/42/wal/67108864-134217728",
            genShardedObjectPathV1(prefix, 42, DATA_FILE_ALIGN_SIZE, 2 * DATA_FILE_ALIGN_SIZE));
        assertShard(prefix, 63, "s3f");
        assertShard(prefix, 64, "s40");
        assertShard(prefix, 255, "sff");
        assertShard(prefix, 256, "s00");
        assertShard(prefix, 257, "s01");
        assertShard(prefix, 511, "sff");
    }

    /** Listing may mix v0, legacy v1, and sharded v1 objects; parsing preserves exact paths and ordering. */
    @Test
    public void testMixedLayoutParsing() {
        String prefix = ObjectUtils.nodePrefix("clusterA", 1001);
        String v0 = prefix + "40/wal/0";
        String legacy = prefix + "41/wal/67108864-134217728";
        String sharded = prefix + "s02/42/wal/134217728-201326592";
        List<WALObject> objects = ObjectUtils.parse(List.of(
            new ObjectStorage.ObjectInfo((short) 7, sharded, 0, 100),
            new ObjectStorage.ObjectInfo((short) 7, v0, 0, 100),
            new ObjectStorage.ObjectInfo((short) 7, legacy, 0, 100)));
        assertEquals(List.of(v0, legacy, sharded), objects.stream().map(WALObject::path).toList());
        assertEquals(List.of(40L, 41L, 42L), objects.stream().map(WALObject::epoch).toList());
        assertEquals(2 * DATA_FILE_ALIGN_SIZE, objects.get(2).startOffset());
        assertEquals(3 * DATA_FILE_ALIGN_SIZE, objects.get(2).endOffset());
        assertEquals(7, objects.get(2).bucketId());
    }

    private static void assertShard(String prefix, long objectIndex, String shard) {
        long start = objectIndex * DATA_FILE_ALIGN_SIZE;
        long end = start + DATA_FILE_ALIGN_SIZE;
        assertEquals(prefix + shard + "/42/wal/" + start + "-" + end,
            genShardedObjectPathV1(prefix, 42, start, end));
    }
}
