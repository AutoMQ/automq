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

package com.automq.stream.s3.wal.common;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;

import java.util.Objects;

public class RecoverResultImpl implements RecoverResult {
    private final StreamRecordBatch record;
    private final RecordOffset recordOffset;

    public RecoverResultImpl(StreamRecordBatch record, RecordOffset recordOffset) {
        this.record = record;
        this.recordOffset = recordOffset;
    }

    @Override
    public String toString() {
        return "RecoverResultImpl{"
               + "record=" + record
               + ", recordOffset=" + recordOffset
               + '}';
    }

    @Override
    public StreamRecordBatch record() {
        return record;
    }

    @Override
    public RecordOffset recordOffset() {
        return recordOffset;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (RecoverResultImpl) obj;
        return Objects.equals(this.record, that.record) && Objects.equals(this.recordOffset, that.recordOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, recordOffset);
    }

}
