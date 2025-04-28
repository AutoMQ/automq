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
