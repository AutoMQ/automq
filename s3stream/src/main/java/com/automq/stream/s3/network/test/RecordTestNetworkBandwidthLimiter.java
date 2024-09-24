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

package com.automq.stream.s3.network.test;

import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

public class RecordTestNetworkBandwidthLimiter implements NetworkBandwidthLimiter {
    private LongAdder consumedCounter = new LongAdder();
    private ThreadLocal<Boolean> throwExceptionWhenConsume = new ThreadLocal<>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    @Override
    public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
        if (throwExceptionWhenConsume.get()) {
            throwExceptionWhenConsume.set(false);
            return CompletableFuture.failedFuture(new RuntimeException("mock consume fail"));
        }
        consumedCounter.add(size);

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getMaxTokens() {
        return Long.MAX_VALUE;
    }

    @Override
    public long getAvailableTokens() {
        return Long.MAX_VALUE;
    }

    @Override
    public void shutdown() {

    }

    public long getConsumed() {
        return consumedCounter.sum();
    }

    public long getConsumedAndReset() {
        return consumedCounter.sumThenReset();
    }

    public void setThrowExceptionWhenConsume(boolean throwException) {
        throwExceptionWhenConsume.set(throwException);
    }
}
