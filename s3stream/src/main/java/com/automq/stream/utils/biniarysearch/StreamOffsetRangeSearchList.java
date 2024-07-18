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
