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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StreamRecordBatchList extends AbstractOrderedCollection<Long> {

    private final List<ComparableStreamRecordBatch> records;
    private final int size;

    public StreamRecordBatchList(List<StreamRecordBatch> records) {
        this.records = new ArrayList<>(records.size());
        for (StreamRecordBatch record : records) {
            this.records.add(new ComparableStreamRecordBatch(record));
        }
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

    private static final class ComparableStreamRecordBatch implements ComparableItem<Long> {
        private final StreamRecordBatch recordBatch;

        private ComparableStreamRecordBatch(StreamRecordBatch recordBatch) {
            this.recordBatch = recordBatch;
        }

        @Override
        public boolean isLessThan(Long value) {
            return recordBatch.getLastOffset() <= value;
        }

        @Override
        public boolean isGreaterThan(Long value) {
            return recordBatch.getBaseOffset() > value;
        }

        public StreamRecordBatch recordBatch() {
            return recordBatch;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (ComparableStreamRecordBatch) obj;
            return Objects.equals(this.recordBatch, that.recordBatch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordBatch);
        }

        @Override
        public String toString() {
            return "ComparableStreamRecordBatch[" +
                "recordBatch=" + recordBatch + ']';
        }

    }
}
