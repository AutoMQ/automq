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

package com.automq.stream.utils.biniarysearch;

import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.metadata.StreamOffsetRange;

import java.util.List;

public class StreamOffsetRangeSearchList extends AbstractOrderedCollection<StreamDataBlock> {
    private final List<StreamOffsetRange> rangeList;

    public StreamOffsetRangeSearchList(List<StreamOffsetRange> rangeList) {
        this.rangeList = rangeList;
    }

    @Override
    protected int size() {
        return rangeList.size();
    }

    @Override
    protected ComparableItem<StreamDataBlock> get(int index) {
        return new ComparableStreamRange(rangeList.get(index));
    }

    private static class ComparableStreamRange implements ComparableItem<StreamDataBlock> {
        private final StreamOffsetRange range;

        public ComparableStreamRange(StreamOffsetRange range) {
            this.range = range;
        }

        @Override
        public boolean isLessThan(StreamDataBlock value) {
            return value.getStartOffset() > range.endOffset() || value.getEndOffset() > range.endOffset();
        }

        @Override
        public boolean isGreaterThan(StreamDataBlock value) {
            return value.getStartOffset() < range.startOffset();
        }
    }
}
