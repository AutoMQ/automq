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

import java.util.HashMap;
import java.util.Map;

public class GlobalNetworkBandwidthLimiters {
    private final Map<AsyncNetworkBandwidthLimiter.Type, AsyncNetworkBandwidthLimiter> limiters = new HashMap<>();

    private static final GlobalNetworkBandwidthLimiters INSTANCE = new GlobalNetworkBandwidthLimiters();

    public static GlobalNetworkBandwidthLimiters instance() {
        return INSTANCE;
    }

    public void setup(AsyncNetworkBandwidthLimiter.Type type, long tokenSize, int refillIntervalMs, long maxTokens) {
        if (limiters.containsKey(type)) {
            throw new IllegalArgumentException(type + " is already setup");
        }
        limiters.put(type, new AsyncNetworkBandwidthLimiter(type, tokenSize, refillIntervalMs, maxTokens));
    }

    public NetworkBandwidthLimiter get(AsyncNetworkBandwidthLimiter.Type type) {
        AsyncNetworkBandwidthLimiter limiter = limiters.get(type);
        if (limiter == null) {
            return AsyncNetworkBandwidthLimiter.NOOP;
        }
        return limiter;
    }

}
