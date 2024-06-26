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

package com.automq.stream.s3.wal;

import java.util.concurrent.CompletableFuture;

public interface AppendResult {
    // The pre-allocated starting offset of the record
    long recordOffset();

    CompletableFuture<CallbackResult> future();

    interface CallbackResult {
        // The record before this offset has been flushed to disk
        long flushedOffset();
    }
}
