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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public interface Writer {
    void start();

    void close();

    CompletableFuture<AppendResult> append(StreamRecordBatch streamRecordBatch) throws OverCapacityException;

    RecordOffset confirmOffset();

    CompletableFuture<Void> reset() throws WALFencedException;

    CompletableFuture<Void> trim(RecordOffset recordOffset) throws WALFencedException;

    Iterator<RecoverResult> recover();
}
