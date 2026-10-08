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

package com.automq.stream.s3.operator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;

/** Owns a retained input buffer and the completion of a speculative object write. */
final class FastRetryWriteTask implements FastRetryTask {
    private final CompletableFuture<Void> attemptCf;
    private final CompletableFuture<Void> finalCf;
    private final Supplier<CompletableFuture<Void>> operation;
    private final ByteBuf data;
    private final FastRetryResultCallback resultCallback;
    private volatile long enqueuedNanos;

    FastRetryWriteTask(CompletableFuture<Void> attemptCf, CompletableFuture<Void> finalCf,
        Supplier<CompletableFuture<Void>> operation, ByteBuf data,
        FastRetryResultCallback resultCallback) {
        this.attemptCf = attemptCf;
        this.finalCf = finalCf;
        this.operation = operation;
        this.data = data.retain();
        this.resultCallback = resultCallback;
    }

    @Override
    public void markEnqueued() {
        enqueuedNanos = System.nanoTime();
    }

    @Override
    public boolean isDone() {
        return attemptCf.isDone() || finalCf.isDone();
    }

    @Override
    public CompletableFuture<Void> execute() {
        long startNanos = System.nanoTime();
        long limiterAwaitTimeMillis = TimeUnit.NANOSECONDS.toMillis(startNanos - enqueuedNanos);
        CompletableFuture<Void> retryCf;
        try {
            retryCf = operation.get();
        } catch (Throwable e) {
            retryCf = CompletableFuture.failedFuture(e);
        }
        return retryCf.handle((nil, ex) -> {
            long apiCostMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            data.release();
            boolean isUsefulRetry = false;
            if (ex == null) {
                isUsefulRetry = finalCf.complete(null);
            }
            resultCallback.onResult(isUsefulRetry, apiCostMillis, limiterAwaitTimeMillis);
            return null;
        });
    }

    @Override
    public void discard() {
        data.release();
    }
}
