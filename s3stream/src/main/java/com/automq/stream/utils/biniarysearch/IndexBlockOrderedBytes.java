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

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import java.util.Objects;

public class IndexBlockOrderedBytes extends AbstractOrderedCollection<IndexBlockOrderedBytes.TargetStreamOffset> {
    private final ObjectReader.IndexBlock indexBlock;

    public IndexBlockOrderedBytes(ObjectReader.IndexBlock indexBlock) {
        this.indexBlock = indexBlock;
    }

    @Override
    protected int size() {
        return this.indexBlock.count();
    }

    @Override
    protected ComparableItem<TargetStreamOffset> get(int index) {
        return new ComparableStreamRange(indexBlock.get(index));
    }

    public static final class TargetStreamOffset {
        private final long streamId;
        private final long offset;

        public TargetStreamOffset(long streamId, long offset) {
            this.streamId = streamId;
            this.offset = offset;
        }

        public long streamId() {
            return streamId;
        }

        public long offset() {
            return offset;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (TargetStreamOffset) obj;
            return this.streamId == that.streamId &&
                this.offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, offset);
        }

        @Override
        public String toString() {
            return "TargetStreamOffset[" +
                "streamId=" + streamId + ", " +
                "offset=" + offset + ']';
        }

    }

    private static final class ComparableStreamRange
        implements ComparableItem<TargetStreamOffset> {
        private final DataBlockIndex index;

        private ComparableStreamRange(DataBlockIndex index) {
            this.index = index;
        }

        public long endOffset() {
            return index.endOffset();
        }

        @Override
        public boolean isLessThan(TargetStreamOffset value) {
            if (this.index().streamId() < value.streamId) {
                return true;
            } else if (this.index().streamId() > value.streamId) {
                return false;
            } else {
                return this.endOffset() <= value.offset;
            }
        }

        @Override
        public boolean isGreaterThan(TargetStreamOffset value) {
            if (this.index().streamId() > value.streamId) {
                return true;
            } else if (this.index().streamId() < value.streamId) {
                return false;
            } else {
                return this.index().startOffset() > value.offset;
            }
        }

        public DataBlockIndex index() {
            return index;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (ComparableStreamRange) obj;
            return Objects.equals(this.index, that.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }

        @Override
        public String toString() {
            return "ComparableStreamRange[" +
                "index=" + index + ']';
        }

    }
}
