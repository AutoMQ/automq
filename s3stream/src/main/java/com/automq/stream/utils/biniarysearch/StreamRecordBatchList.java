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

import com.automq.stream.s3.model.StreamRecordBatch;
import java.util.List;

public class StreamRecordBatchList extends AbstractOrderedCollection<Long> {

    private final List<ComparableStreamRecordBatch> records;
    private final int size;

    public StreamRecordBatchList(List<StreamRecordBatch> records) {
        this.records = records.stream().map(ComparableStreamRecordBatch::new).toList();
        this.size = records.size();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    protected ComparableItem<Long> get(int index) {
        return records.get(index);
    }

    private record ComparableStreamRecordBatch(StreamRecordBatch recordBatch) implements ComparableItem<Long> {
        @Override
        public boolean isLessThan(Long value) {
            return recordBatch.getLastOffset() <= value;
        }

        @Override
        public boolean isGreaterThan(Long value) {
            return recordBatch.getBaseOffset() > value;
        }
    }
}
