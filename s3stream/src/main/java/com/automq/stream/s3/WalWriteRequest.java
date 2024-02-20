/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import java.util.concurrent.CompletableFuture;

public class WalWriteRequest implements Comparable<WalWriteRequest> {
    final StreamRecordBatch record;
    final AppendContext context;
    final CompletableFuture<Void> cf;
    long offset;
    boolean persisted;

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
            '}';
    }
}
