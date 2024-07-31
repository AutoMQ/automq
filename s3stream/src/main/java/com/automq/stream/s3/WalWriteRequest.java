/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
