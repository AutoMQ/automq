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

package com.automq.stream.s3.wal;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.RuntimeIOException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

public interface WriteAheadLog {

    WriteAheadLog start() throws IOException;

    void shutdownGracefully();

    /**
     * Get write ahead log metadata
     *
     * @return {@link WALMetadata}
     */
    WALMetadata metadata();

    /**
     * Append data to log, note append may be out of order.
     * ex. when sequence append R1 R2 , R2 maybe complete before R1.
     * {@link ByteBuf#release()} will be called whatever append success or not.
     *
     * @return The data position will be written.
     */
    // TODO: change the doc
    CompletableFuture<AppendResult> append(TraceContext context, StreamRecordBatch streamRecordBatch) throws OverCapacityException;

    CompletableFuture<StreamRecordBatch> get(RecordOffset recordOffset);

    CompletableFuture<List<StreamRecordBatch>> get(RecordOffset startOffset, RecordOffset endOffset);

    RecordOffset confirmOffset();

    /**
     * Recover log from the beginning. The iterator will return the recovered result in order.
     * It may throw a {@link RuntimeIOException} if the recovery fails.
     *
     * @return iterator of recover result.
     */
    Iterator<RecoverResult> recover();

    /**
     * Reset all data in log.
     * Equivalent to trim to the end of the log.
     *
     * @return future complete when reset done.
     */
    CompletableFuture<Void> reset();

    /**
     * Trim {@code data <= offset} in log.
     *
     * @param offset inclusive trim offset.
     * @return future complete when trim done.
     */
    CompletableFuture<Void> trim(RecordOffset offset);
}
