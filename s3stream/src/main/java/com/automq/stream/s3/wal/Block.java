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

package com.automq.stream.s3.wal;


import com.automq.stream.s3.wal.util.WALUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.automq.stream.s3.wal.WriteAheadLog.AppendResult;

/**
 * A Block contains multiple records, and will be written to the WAL in one batch.
 */
public interface Block {
    /**
     * The start offset of this block.
     * Align to {@link WALUtil#BLOCK_SIZE}
     */
    long startOffset();

    /**
     * Append a record to this block.
     *
     * @param recordSize The size of this record.
     * @param recordSupplier The supplier of this record which receives the start offset of this record as the parameter.
     * @param future The future of this record, which will be completed when the record is written to the WAL.
     * @return The start offset of this record. If the size of this block exceeds the limit, return -1.
     */
    long addRecord(long recordSize, Function<Long, ByteBuffer> recordSupplier, CompletableFuture<AppendResult.CallbackResult> future);

    /**
     * Futures of all records in this block.
     */
    List<CompletableFuture<AppendResult.CallbackResult>> futures();

    default boolean isEmpty() {
        return futures().isEmpty();
    }

    /**
     * The content of this block, which contains multiple records.
     */
    ByteBuffer data();
}
