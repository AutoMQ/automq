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

/** Transfers the winning read buffer to the caller and releases duplicate results. */
final class FastRetryReadTask implements FastRetryTask {
    private final CompletableFuture<ByteBuf> attemptCf;
    private final CompletableFuture<ByteBuf> finalCf;
    private final Supplier<CompletableFuture<ByteBuf>> operation;
    private final FastRetryResultCallback resultCallback;
    private volatile long enqueuedNanos;

    FastRetryReadTask(CompletableFuture<ByteBuf> attemptCf, CompletableFuture<ByteBuf> finalCf,
        Supplier<CompletableFuture<ByteBuf>> operation, FastRetryResultCallback resultCallback) {
        this.attemptCf = attemptCf;
        this.finalCf = finalCf;
        this.operation = operation;
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
        CompletableFuture<ByteBuf> retryCf;
        try {
            retryCf = operation.get();
        } catch (Throwable e) {
            retryCf = CompletableFuture.failedFuture(e);
        }
        return retryCf.handle((buf, ex) -> {
            long apiCostMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            boolean isUsefulRetry = false;
            if (ex == null) {
                try {
                    isUsefulRetry = finalCf.complete(buf);
                } finally {
                    if (!isUsefulRetry) {
                        buf.release();
                    }
                }
            }
            resultCallback.onResult(isUsefulRetry, apiCostMillis, limiterAwaitTimeMillis);
            return null;
        });
    }

    @Override
    public void discard() {
        // No input buffer is retained for a read.
    }
}
