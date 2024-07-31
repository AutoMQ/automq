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

package com.automq.stream.s3.wal;

import java.util.concurrent.CompletableFuture;

public interface AppendResult {
    // The pre-allocated starting offset of the record
    long recordOffset();

    CompletableFuture<CallbackResult> future();

    interface CallbackResult {

        // The record before this offset (exclusive) has been flushed to disk
        long flushedOffset();
    }
}
