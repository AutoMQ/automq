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
