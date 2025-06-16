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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.WALFencedException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class NoopWriter implements Writer {

    @Override
    public void start() {
        // No-op
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public CompletableFuture<AppendResult> append(StreamRecordBatch streamRecordBatch) throws OverCapacityException {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("append is not supported"));
    }

    @Override
    public RecordOffset confirmOffset() {
        return DefaultRecordOffset.of(0, 0, 0);
    }

    @Override
    public CompletableFuture<Void> reset() throws WALFencedException {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("reset is not supported"));
    }

    @Override
    public CompletableFuture<Void> trim(RecordOffset recordOffset) throws WALFencedException {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("trim is not supported"));
    }

    @Override
    public Iterator<RecoverResult> recover() {
        throw new UnsupportedOperationException("recover is not supported");
    }
}
