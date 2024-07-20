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

package com.automq.stream.utils;

import com.automq.stream.utils.biniarysearch.AbstractOrderedCollection;
import com.automq.stream.utils.biniarysearch.ComparableItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("S3Unit")
public class AbstractOrderedCollectionTest {

    @Test
    public void test() {
        IntCollection c = new IntCollection(new Integer[] {1, 3, 5, 7, 9});
        Assertions.assertEquals(0, c.search(1));
        Assertions.assertEquals(4, c.search(9));
        Assertions.assertEquals(-3, c.search(4));
    }

    static class IntCollection extends AbstractOrderedCollection<Integer> {
        private final Integer[] data;

        public IntCollection(Integer[] data) {
            this.data = data;
        }

        @Override
        protected int size() {
            return data.length;
        }

        @Override
        protected ComparableItem<Integer> get(int index) {
            return new ComparableItem<Integer>() {
                @Override
                public boolean isLessThan(Integer target) {
                    return data[index] < target;
                }

                @Override
                public boolean isGreaterThan(Integer target) {
                    return data[index] > target;
                }
            };
        }
    }

}
