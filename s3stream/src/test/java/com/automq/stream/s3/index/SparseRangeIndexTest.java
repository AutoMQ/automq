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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparseRangeIndexTest {

    @Test
    public void testAppend() {
        int compactNum = 5;
        int sparsePadding = 1;
        SparseRangeIndex sparseRangeIndex = new SparseRangeIndex(compactNum, sparsePadding);
        int nextStartOffset = 0;
        List<RangeIndex> originList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            RangeIndex rangeIndex = new RangeIndex(nextStartOffset, nextStartOffset + 10, i);
            sparseRangeIndex.append(rangeIndex);
            originList.add(rangeIndex);
            nextStartOffset += 10;
        }
        // test append out of order range
        sparseRangeIndex.append(new RangeIndex(0, 10, 0));
        Assertions.assertEquals(1, sparseRangeIndex.size());

        nextStartOffset = 10;
        for (int i = 1; i < 10; i++) {
            RangeIndex rangeIndex = new RangeIndex(nextStartOffset, nextStartOffset + 10, i);
            sparseRangeIndex.append(rangeIndex);
            nextStartOffset += 10;
        }

        Assertions.assertEquals(7, sparseRangeIndex.size());
        List<RangeIndex> rangeIndexList = sparseRangeIndex.getRangeIndexList();
        checkOrder(rangeIndexList);
        for (int i = 0; i < originList.size(); i++) {
            if (i >= originList.size() - compactNum || i % 2 != 0) {
                Assertions.assertTrue(rangeIndexList.contains(originList.get(i)));
            } else {
                Assertions.assertFalse(rangeIndexList.contains(originList.get(i)));
            }
        }

        RangeIndex index0 = rangeIndexList.get(0);
        RangeIndex index1 = rangeIndexList.get(1);
        RangeIndex newRangeIndex = new RangeIndex(index0.getStartOffset(), index1.getEndOffset(), 10);
        sparseRangeIndex.compact(newRangeIndex, Set.of(index0.getObjectId(), index1.getObjectId()));
        rangeIndexList = sparseRangeIndex.getRangeIndexList();
        Assertions.assertEquals(6, rangeIndexList.size());
        checkOrder(rangeIndexList);
    }

    private void checkOrder(List<RangeIndex> rangeIndexList) {
        for (int i = 0; i < rangeIndexList.size() - 1; i++) {
            Assertions.assertTrue(rangeIndexList.get(i).compareTo(rangeIndexList.get(i + 1)) < 0);
        }
    }
}
