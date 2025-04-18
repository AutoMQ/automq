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
