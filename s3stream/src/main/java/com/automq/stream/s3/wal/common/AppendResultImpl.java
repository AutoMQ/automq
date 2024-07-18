/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.common;

import com.automq.stream.s3.wal.AppendResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class AppendResultImpl implements AppendResult {
    private final long recordOffset;
    private final CompletableFuture<CallbackResult> future;

    public AppendResultImpl(long recordOffset, CompletableFuture<CallbackResult> future) {
        this.recordOffset = recordOffset;
        this.future = future;
    }

    @Override
    public String toString() {
        return "AppendResultImpl{" + "recordOffset=" + recordOffset + '}';
    }

    @Override
    public long recordOffset() {
        return recordOffset;
    }

    @Override
    public CompletableFuture<CallbackResult> future() {
        return future;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (AppendResultImpl) obj;
        return this.recordOffset == that.recordOffset &&
               Objects.equals(this.future, that.future);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordOffset, future);
    }

}
