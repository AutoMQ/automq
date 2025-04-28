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

package com.automq.stream.s3;

import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.concurrent.CompletableFuture;

public class WalWriteRequest implements Comparable<WalWriteRequest> {
    final StreamRecordBatch record;
    final AppendContext context;
    final CompletableFuture<Void> cf;
    long offset;
    /**
     * Whether the record has been persisted to the {@link WriteAheadLog}
     * When a continuous series of records IN A STREAM have been persisted to the WAL, they can be uploaded to S3.
     *
     * @see S3Storage.WALCallbackSequencer
     */
    boolean persisted;

    /**
     * Whether the record has been put to the {@link LogCache}
     * When a continuous series of records have been persisted to the WAL and uploaded to S3, they can be trimmed.
     *
     * @see S3Storage.WALConfirmOffsetCalculator
     */
    boolean confirmed;

    public WalWriteRequest(StreamRecordBatch record, long offset, CompletableFuture<Void> cf) {
        this(record, offset, cf, AppendContext.DEFAULT);
    }

    public WalWriteRequest(StreamRecordBatch record, long offset, CompletableFuture<Void> cf, AppendContext context) {
        this.record = record;
        this.offset = offset;
        this.cf = cf;
        this.context = context;
    }

    @Override
    public int compareTo(WalWriteRequest o) {
        return record.compareTo(o.record);
    }

    @Override
    public String toString() {
        return "WalWriteRequest{" +
            "record=" + record +
            ", offset=" + offset +
            ", persisted=" + persisted +
            ", confirmed=" + confirmed +
            '}';
    }
}
