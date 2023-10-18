/*
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

import io.netty.buffer.ByteBuf;

public class IndexBlockOrderedBytes extends AbstractOrderedCollection<IndexBlockOrderedBytes.TargetStreamOffset> {
    private final ByteBuf byteBuf;

    public IndexBlockOrderedBytes(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    @Override
    int size() {
        return this.byteBuf.readableBytes() / ComparableStreamRange.SIZE;
    }

    @Override
    ComparableItem<TargetStreamOffset> get(int index) {
        int start = index * ComparableStreamRange.SIZE;
        long streamId = this.byteBuf.getLong(start);
        long startOffset = this.byteBuf.getLong(start + 8);
        int recordCount = this.byteBuf.getInt(start + 16);
        int blockId = this.byteBuf.getInt(start + 20);
        return new ComparableStreamRange(streamId, startOffset, recordCount, blockId);
    }

    public record TargetStreamOffset(long streamId, long offset) {

    }

    private record ComparableStreamRange(long streamId, long startOffset, int recordCount, int blockIndex)
            implements ComparableItem<TargetStreamOffset> {
        private static final int SIZE = 8 + 8 + 4 + 4;

        public long endOffset() {
            return startOffset + recordCount;
        }

        @Override
        public boolean isLessThan(TargetStreamOffset value) {
            if (this.streamId < value.streamId) {
                return true;
            } else if (this.streamId > value.streamId) {
                return false;
            } else {
                return this.endOffset() <= value.offset;
            }
        }

        @Override
        public boolean isGreaterThan(TargetStreamOffset value) {
            if (this.streamId > value.streamId) {
                return true;
            } else if (this.streamId < value.streamId) {
                return false;
            } else {
                return this.startOffset > value.offset;
            }
        }
    }
}
