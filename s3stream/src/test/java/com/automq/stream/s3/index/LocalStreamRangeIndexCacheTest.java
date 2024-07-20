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

import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
public class LocalStreamRangeIndexCacheTest {
    private static final int NODE_0 = 10;
    private static final long STREAM_0 = 0;

    @Test
    public void testInit() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        // init with empty index
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.init(NODE_0, objectStorage);
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());

        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        request.setObjectId(88);
        request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, 50, 100, 100)));
        cache.updateIndexFromRequest(request);
        cache.upload().join();

        cache = new LocalStreamRangeIndexCache();
        cache.init(NODE_0, objectStorage);
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 70).join());
        Assertions.assertEquals(88, cache.searchObjectId(STREAM_0, 100).join());
    }

    @Test
    public void testAppend() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.init(NODE_0, objectStorage);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        long startOffset = 50;
        for (int i = 0; i < 10; i++) {
            request.setObjectId(88 + i);
            request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, startOffset, startOffset + 100, 100)));
            cache.updateIndexFromRequest(request).join();
            startOffset += 100;
        }
        Assertions.assertEquals(7, cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().size());
        Assertions.assertEquals(new RangeIndex(150, 250, 89),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(0));
        Assertions.assertEquals(new RangeIndex(350, 450, 91),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(1));
        Assertions.assertEquals(new RangeIndex(550, 650, 93),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(2));
        Assertions.assertEquals(new RangeIndex(650, 750, 94),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(3));
        Assertions.assertEquals(new RangeIndex(750, 850, 95),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(4));
        Assertions.assertEquals(new RangeIndex(850, 950, 96),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(5));
        Assertions.assertEquals(new RangeIndex(950, 1050, 97),
            cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().get(6));
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 0).join());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 50).join());
        Assertions.assertEquals(89, cache.searchObjectId(STREAM_0, 150).join());
        Assertions.assertEquals(93, cache.searchObjectId(STREAM_0, 600).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 950).join());
        Assertions.assertEquals(97, cache.searchObjectId(STREAM_0, 1500).join());
    }

    @Test
    public void testCompact() {
        ObjectStorage objectStorage = new MemoryObjectStorage();
        LocalStreamRangeIndexCache cache = new LocalStreamRangeIndexCache();
        cache.init(NODE_0, objectStorage);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        long startOffset = 50;
        for (int i = 0; i < 10; i++) {
            request.setObjectId(88 + i);
            request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, startOffset, startOffset + 100, 100)));
            cache.updateIndexFromRequest(request).join();
            startOffset += 100;
        }
        Assertions.assertEquals(7, cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().size());
        request.setObjectId(256);
        request.setStreamRanges(List.of(new ObjectStreamRange(STREAM_0, 0, 50, 650, 1000)));
        request.setCompactedObjectIds(List.of(88L, 89L, 90L, 91L, 92L, 93L));
        cache.updateIndexFromRequest(request).join();
        Assertions.assertEquals(5, cache.getStreamRangeIndexMap().get(STREAM_0).getRangeIndexList().size());
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

        request.setObjectId(-1);
        request.setStreamRanges(Collections.emptyList());
        request.setCompactedObjectIds(List.of(256L, 94L, 95L, 96L, 97L));
        cache.updateIndexFromRequest(request).join();
        Assertions.assertTrue(cache.getStreamRangeIndexMap().isEmpty());
        Assertions.assertEquals(-1, cache.searchObjectId(STREAM_0, 300).join());
    }
}
