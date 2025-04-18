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

import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Timeout(30)
public class LocalStreamRangeIndexCacheTest {
    private static final int NODE_0 = 10;
    private static final long STREAM_0 = 0;
    private static final long STREAM_1 = 1;

    @Test
    public void testInit() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        // init with empty index
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());

        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        request.setObjectId(88);
        request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, 50, 100, 100)));
        cache.updateIndexFromRequest(request);
        cache.upload().join();

        cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        cache.initCf().join();
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, cache.totalSize());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 70).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 100).join());
    }

    @Test
    public void testAppend() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        long startOffset = 50;
        for (int i = 0; i < 10; i++) {
            request.setObjectId(88 + i);
            request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, startOffset, startOffset + 100, 100)));
            cache.updateIndexFromRequest(request).join();
            startOffset += 100;
        }
        Assertions.assertEquals(10, cache.getStreamRangeIndexMap().get(STREAM_0).length());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 100).join());
        Assertions.assertEquals(89, cache.searchObjectId(STREAM_0, 150).join());
        Assertions.assertEquals(93, cache.searchObjectId(STREAM_0, 600).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 950).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 1500).join());
    }

    @Test
    public void testPrune() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        long startOffset = 50;
        for (int i = 0; i < 10; i++) {
            request.setObjectId(88 + i);
            request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, startOffset, startOffset + 100, 100)));
            cache.updateIndexFromRequest(request).join();
            startOffset += 100;
        }
        Assertions.assertEquals(10, cache.getStreamRangeIndexMap().get(STREAM_0).length());
        Assertions.assertEquals(400, cache.totalSize());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 100).join());
        Assertions.assertEquals(89, cache.searchObjectId(STREAM_0, 150).join());
        Assertions.assertEquals(93, cache.searchObjectId(STREAM_0, 600).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 950).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 1500).join());

        CompletableFuture<Void> cf = cache.asyncPrune(() -> Set.of(94L, 95L, 96L, 97L));
        Assertions.assertDoesNotThrow(cf::join);
        Assertions.assertEquals(4, cache.getStreamRangeIndexMap().get(STREAM_0).length());
        Assertions.assertEquals(160, cache.totalSize());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 100).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 150).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 600).join());
        Assertions.assertEquals(94, cache.searchObjectId(STREAM_0, 700).join());
        Assertions.assertEquals(95, cache.searchObjectId(STREAM_0, 800).join());
        Assertions.assertEquals(96, cache.searchObjectId(STREAM_0, 900).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 950).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 1500).join());

        // test load from object storage
        cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        cache.initCf().join();
        Assertions.assertEquals(4, cache.getStreamRangeIndexMap().get(STREAM_0).length());
        Assertions.assertEquals(160, cache.totalSize());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 100).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 150).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 600).join());
        Assertions.assertEquals(94, cache.searchObjectId(STREAM_0, 700).join());
        Assertions.assertEquals(95, cache.searchObjectId(STREAM_0, 800).join());
        Assertions.assertEquals(96, cache.searchObjectId(STREAM_0, 900).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 950).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 1500).join());

        cache.asyncPrune(Collections::emptySet).join();
        Assertions.assertEquals(0, cache.getStreamRangeIndexMap().size());
        Assertions.assertEquals(0, cache.totalSize());
    }

    @Test
    public void testEvict() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        int streamNum = 500;
        int maxRangeIndexNum = 2000;
        MockRandom r = new MockRandom();
        int objectId = 0;
        for (int i = 0; i < streamNum; i++) {
            int rangeIndexNum = r.nextInt(maxRangeIndexNum);
            int startOffset = 0;
            for (int j = 0; j < rangeIndexNum; j++) {
                CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
                request.setObjectId(objectId++);
                request.setStreamRanges(List.of(new ObjectStreamRange(i, 0, startOffset, startOffset + 100, 100)));
                cache.updateIndexFromRequest(request).join();
                startOffset += 100;
            }
        }
        Assertions.assertEquals(streamNum, cache.getStreamRangeIndexMap().size());
        Assertions.assertTrue(cache.totalSize() <= LocalStreamRangeIndexCache.MAX_INDEX_SIZE);
    }

    @Test
    public void testCompact() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        long startOffset = 50;
        for (int i = 0; i < 10; i++) {
            request.setObjectId(88 + i);
            request.setStreamRanges(List.of(
                new ObjectStreamRange(STREAM_0, 0, startOffset, startOffset + 100, 100),
                new ObjectStreamRange(STREAM_1, 0, startOffset, startOffset + 100, 100)));
            cache.updateIndexFromRequest(request).join();
            startOffset += 100;
        }
        Assertions.assertEquals(10, cache.getStreamRangeIndexMap().get(STREAM_0).length());
        Assertions.assertEquals(10, cache.getStreamRangeIndexMap().get(STREAM_1).length());
        Assertions.assertEquals(20 * RangeIndex.OBJECT_SIZE, cache.totalSize());
        request.setObjectId(256);
        request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, 50, 650, 1000)));
        request.setCompactedObjectIds(List.of(88L, 89L, 90L, 91L, 92L, 93L));
        request.setStreamObjects(List.of(
            newStreamObject(257, 0, STREAM_1, 50, 150),
            newStreamObject(258, 0, STREAM_1, 150, 250),
            newStreamObject(259, 0, STREAM_1, 250, 350),
            newStreamObject(260, 0, STREAM_1, 350, 450),
            newStreamObject(261, 0, STREAM_1, 450, 550),
            newStreamObject(262, 0, STREAM_1, 550, 650)
            ));
        cache.updateIndexFromRequest(request).join();
        Assertions.assertEquals(5, cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().size());
        Assertions.assertEquals(4, cache.getStreamRangeIndexMap().get(STREAM_1).getRangeIndexList().size());
        Assertions.assertEquals(9 * RangeIndex.OBJECT_SIZE, cache.totalSize());
        Assertions.assertEquals(new RangeIndex(50, 650, 256),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(0));
        Assertions.assertEquals(new RangeIndex(650, 750, 94),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(1));
        Assertions.assertEquals(new RangeIndex(750, 850, 95),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(2));
        Assertions.assertEquals(new RangeIndex(850, 950, 96),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(3));
        Assertions.assertEquals(new RangeIndex(950, 1050, 97),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(4));
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(256, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(256, cache.searchObjectId(STREAM_0, 300).join());
        Assertions.assertEquals(94, cache.searchObjectId(STREAM_0, 650).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 950).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 1500).join());

        Assertions.assertEquals(new RangeIndex(650, 750, 94),
            cache.getStreamRangeIndexMap().get(STREAM_1).getRangeIndexList().get(0));
        Assertions.assertEquals(new RangeIndex(750, 850, 95),
            cache.getStreamRangeIndexMap().get(STREAM_1).getRangeIndexList().get(1));
        Assertions.assertEquals(new RangeIndex(850, 950, 96),
            cache.getStreamRangeIndexMap().get(STREAM_1).getRangeIndexList().get(2));
        Assertions.assertEquals(new RangeIndex(950, 1050, 97),
            cache.getStreamRangeIndexMap().get(STREAM_1).getRangeIndexList().get(3));

        request.setObjectId(-1);
        request.setStreamRanges(Collections.emptyList());
        request.setCompactedObjectIds(List.of(256L, 94L, 95L, 96L, 97L));
        request.setStreamObjects(List.of(
            newStreamObject(259, 0, STREAM_0, 50, 650),
            newStreamObject(260, 0, STREAM_0, 650, 750),
            newStreamObject(261, 0, STREAM_0, 750, 850),
            newStreamObject(262, 0, STREAM_0, 850, 950),
            newStreamObject(263, 0, STREAM_0, 950, 1050),
            newStreamObject(264, 0, STREAM_1, 650, 750),
            newStreamObject(265, 0, STREAM_1, 750, 850),
            newStreamObject(266, 0, STREAM_1, 850, 950),
            newStreamObject(267, 0, STREAM_1, 950, 1050)
        ));
        cache.updateIndexFromRequest(request).join();
        Assertions.assertTrue(cache.getStreamRangeIndexMap().isEmpty());
        Assertions.assertEquals(0, cache.totalSize());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 300).join());
    }

    @Test
    public void testCompactWithStreamDeleted() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.start();
        cache.init(NODE_0, objectStorage);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        long startOffset = 50;
        for (int i = 0; i < 10; i++) {
            request.setObjectId(88 + i);
            request.setStreamRanges(List.of(
                new ObjectStreamRange(STREAM_0, 0, startOffset, startOffset + 100, 100),
                new ObjectStreamRange(STREAM_1, 0, startOffset, startOffset + 100, 100)));
            cache.updateIndexFromRequest(request).join();
            startOffset += 100;
        }
        Assertions.assertEquals(10, cache.getStreamRangeIndexMap().get(STREAM_0).length());
        Assertions.assertEquals(10, cache.getStreamRangeIndexMap().get(STREAM_1).length());
        Assertions.assertEquals(20 * RangeIndex.OBJECT_SIZE, cache.totalSize());

        // mock STREAM_0 deleted
        request.setObjectId(256);
        request.setStreamRanges(List.of(
            new ObjectStreamRange(STREAM_1, 0, 50, 1050, 1000)
        ));
        request.setCompactedObjectIds(List.of(88L, 89L, 90L, 91L, 92L, 93L, 94L, 95L, 96L, 97L));
        request.setStreamObjects(Collections.emptyList());
        cache.updateIndexFromRequest(request).join();

        Assertions.assertNull(cache.getStreamRangeIndexMap().get(STREAM_0));
        Assertions.assertEquals(1, cache.getStreamRangeIndexMap().get(STREAM_1).getRangeIndexList().size());
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, cache.totalSize());
        Assertions.assertEquals(new RangeIndex(50, 1050, 256),
            cache.getStreamRangeIndexMap().get(STREAM_1).getRangeIndexList().get(0));
    }

    private StreamObject newStreamObject(long objectId, long objectSize, long streamId, long startOffset, long endOffset) {
        StreamObject streamObject = new StreamObject();
        streamObject.setObjectId(objectId);
        streamObject.setObjectSize(objectSize);
        streamObject.setStreamId(streamId);
        streamObject.setStartOffset(startOffset);
        streamObject.setEndOffset(endOffset);
        return streamObject;
    }
}
