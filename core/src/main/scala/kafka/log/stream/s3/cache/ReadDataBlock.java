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

package kafka.log.stream.s3.cache;

import kafka.log.stream.s3.model.StreamRecordBatch;

import java.util.List;
import java.util.OptionalLong;

public class ReadDataBlock {
    private List<StreamRecordBatch> records;

    public ReadDataBlock(List<StreamRecordBatch> records) {
        this.records = records;
    }

    public List<StreamRecordBatch> getRecords() {
        return records;
    }

    public void setRecords(List<StreamRecordBatch> records) {
        this.records = records;
    }

    public OptionalLong startOffset() {
        if (records.isEmpty()) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(records.get(0).getBaseOffset());
        }
    }

    public OptionalLong endOffset() {
        if (records.isEmpty()) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(records.get(records.size() - 1).getLastOffset());
        }
    }

    public int sizeInBytes() {
        return records.stream().mapToInt(r -> r.getRecordBatch().rawPayload().remaining()).sum();
    }
}
