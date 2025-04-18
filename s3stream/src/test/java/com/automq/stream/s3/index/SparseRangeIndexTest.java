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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class SparseRangeIndexTest {

    @Test
    public void testAppend() {
        int totalSize = 6;
        int compactNum = 2;
        SparseRangeIndex sparseRangeIndex = new SparseRangeIndex(compactNum);
        int nextStartOffset = 0;
        List<RangeIndex> originList = new ArrayList<>();
        for (int i = 0; i < totalSize; i++) {
            RangeIndex rangeIndex = new RangeIndex(nextStartOffset, nextStartOffset + 10, i);
            sparseRangeIndex.append(rangeIndex);
            originList.add(rangeIndex);
            nextStartOffset += 10;
        }

        // test append out of order range
        int delta = sparseRangeIndex.append(new RangeIndex(0, 10, 0));
        Assertions.assertEquals(-RangeIndex.OBJECT_SIZE * (totalSize - 1), delta);
        Assertions.assertEquals(1, sparseRangeIndex.length());
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, sparseRangeIndex.size());

        nextStartOffset = 10;
        for (int i = 1; i < totalSize; i++) {
            RangeIndex rangeIndex = new RangeIndex(nextStartOffset, nextStartOffset + 10, i);
            sparseRangeIndex.append(rangeIndex);
            nextStartOffset += 10;
        }

        Assertions.assertEquals(totalSize, sparseRangeIndex.length());
        Assertions.assertEquals(originList, sparseRangeIndex.getRangeIndexList());

        // init: 0, 1, 2, 3, 4, 5
        // test evict 1rst
        // 0, 2, 3, 4, 5
        int expectedLength = totalSize - 1;
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, sparseRangeIndex.evictOnce());
        Assertions.assertEquals(expectedLength, sparseRangeIndex.length());
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE * expectedLength, sparseRangeIndex.size());
        List<RangeIndex> expectedList = new ArrayList<>(originList);
        expectedList.remove(originList.get(1));
        Assertions.assertEquals(expectedList, sparseRangeIndex.getRangeIndexList());

        // test evict 2nd
        // 0, 2, 4, 5
        expectedLength = totalSize - 2;
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, sparseRangeIndex.evictOnce());
        Assertions.assertEquals(expectedLength, sparseRangeIndex.length());
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE * expectedLength, sparseRangeIndex.size());
        expectedList = new ArrayList<>(originList);
        expectedList.remove(originList.get(1));
        expectedList.remove(originList.get(3));
        Assertions.assertEquals(expectedList, sparseRangeIndex.getRangeIndexList());

        // test evict 3rd
        // 0, 4, 5
        expectedLength = totalSize - 3;
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, sparseRangeIndex.evictOnce());
        Assertions.assertEquals(expectedLength, sparseRangeIndex.length());
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE * expectedLength, sparseRangeIndex.size());
        expectedList = new ArrayList<>(originList);
        expectedList.remove(originList.get(1));
        expectedList.remove(originList.get(3));
        expectedList.remove(originList.get(2));
        Assertions.assertEquals(expectedList, sparseRangeIndex.getRangeIndexList());

        // test evict 4th
        // 0, 5
        expectedLength = totalSize - 4;
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, sparseRangeIndex.evictOnce());
        Assertions.assertEquals(expectedLength, sparseRangeIndex.length());
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE * expectedLength, sparseRangeIndex.size());
        expectedList = new ArrayList<>(originList);
        expectedList.remove(originList.get(1));
        expectedList.remove(originList.get(3));
        expectedList.remove(originList.get(2));
        expectedList.remove(originList.get(4));
        Assertions.assertEquals(expectedList, sparseRangeIndex.getRangeIndexList());

        // test evict 5th
        // 0
        expectedLength = totalSize - 5;
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, sparseRangeIndex.evictOnce());
        Assertions.assertEquals(expectedLength, sparseRangeIndex.length());
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE * expectedLength, sparseRangeIndex.size());
        expectedList = new ArrayList<>(originList);
        expectedList.remove(originList.get(1));
        expectedList.remove(originList.get(3));
        expectedList.remove(originList.get(2));
        expectedList.remove(originList.get(4));
        expectedList.remove(originList.get(5));
        Assertions.assertEquals(expectedList, sparseRangeIndex.getRangeIndexList());

        // test evict 6th
        expectedLength = 0;
        Assertions.assertEquals(RangeIndex.OBJECT_SIZE, sparseRangeIndex.evictOnce());
        Assertions.assertEquals(expectedLength, sparseRangeIndex.length());
        Assertions.assertEquals(0, sparseRangeIndex.size());
        expectedList = new ArrayList<>(originList);
        expectedList.remove(originList.get(1));
        expectedList.remove(originList.get(3));
        expectedList.remove(originList.get(2));
        expectedList.remove(originList.get(4));
        expectedList.remove(originList.get(5));
        expectedList.remove(originList.get(0));
        Assertions.assertEquals(expectedList, sparseRangeIndex.getRangeIndexList());

        Assertions.assertEquals(0, sparseRangeIndex.evictOnce());
    }
}
