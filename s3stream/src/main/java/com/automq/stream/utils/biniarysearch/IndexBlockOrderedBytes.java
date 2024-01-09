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

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;

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

    public record TargetStreamOffset(long streamId, long offset) {

    }

    private record ComparableStreamRange(DataBlockIndex index)
        implements ComparableItem<TargetStreamOffset> {

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
    }
}
