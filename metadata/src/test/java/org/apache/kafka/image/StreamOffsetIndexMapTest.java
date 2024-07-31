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

package org.apache.kafka.image;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamOffsetIndexMapTest {

    @Test
    public void testPutAndEvict() {
        StreamOffsetIndexMap map = new StreamOffsetIndexMap(10);
        for (int i = 0; i < 20; i++) {
            map.put(i % 2, i, i * i);
        }
        Assertions.assertEquals(10, map.cacheSize());
        Assertions.assertEquals(10, map.entrySize());
        Assertions.assertEquals(0, map.floorIndex(0, 5));
        Assertions.assertEquals(100, map.floorIndex(0, 11));
        Assertions.assertEquals(225, map.floorIndex(1, 15));
    }
}
