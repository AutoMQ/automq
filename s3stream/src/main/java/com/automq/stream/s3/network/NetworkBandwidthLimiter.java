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

package com.automq.stream.s3.network;

import java.util.concurrent.CompletableFuture;

public interface NetworkBandwidthLimiter {
    NetworkBandwidthLimiter NOOP = new Noop();

    CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size);

    long getMaxTokens();

    long getAvailableTokens();


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
    }
}
