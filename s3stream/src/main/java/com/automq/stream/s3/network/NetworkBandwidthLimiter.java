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

package com.automq.stream.s3.network;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface NetworkBandwidthLimiter {
    NetworkBandwidthLimiter NOOP = new Noop();

    CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size);

    default void consumeBlocking(ThrottleStrategy throttleStrategy, long size)
        throws InterruptedException, ExecutionException {
        CompletableFuture<Void> future = consume(throttleStrategy, size);
        if (future == null) {
            return;
        }
        try {
            future.get();
        } catch (InterruptedException e) {
            future.cancel(true);
            throw e;
        }
    }

    long getMaxTokens();

    long getAvailableTokens();

    void shutdown();


    class Noop implements NetworkBandwidthLimiter {
        @Override
        public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
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
    }
}
