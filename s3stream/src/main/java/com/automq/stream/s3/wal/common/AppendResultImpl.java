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
