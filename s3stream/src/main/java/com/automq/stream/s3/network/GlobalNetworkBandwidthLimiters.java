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

import java.util.HashMap;
import java.util.Map;

public class GlobalNetworkBandwidthLimiters {
    private final Map<AsyncNetworkBandwidthLimiter.Type, AsyncNetworkBandwidthLimiter> limiters = new HashMap<>();

    private static final GlobalNetworkBandwidthLimiters INSTANCE = new GlobalNetworkBandwidthLimiters();
    private NetworkBandwidthLimiter inboundLimiter = AsyncNetworkBandwidthLimiter.NOOP;
    private NetworkBandwidthLimiter outboundLimiter = AsyncNetworkBandwidthLimiter.NOOP;

    public static GlobalNetworkBandwidthLimiters instance() {
        return INSTANCE;
    }

    public void setup(AsyncNetworkBandwidthLimiter.Type type, long tokenSize, int refillIntervalMs, long maxTokens) {
        if (limiters.containsKey(type)) {
            throw new IllegalArgumentException(type + " is already setup");
        }
        limiters.put(type, new AsyncNetworkBandwidthLimiter(type, tokenSize, refillIntervalMs, maxTokens));
        if (type == AsyncNetworkBandwidthLimiter.Type.INBOUND) {
            inboundLimiter = limiters.get(type);
        } else if (type == AsyncNetworkBandwidthLimiter.Type.OUTBOUND) {
            outboundLimiter = limiters.get(type);
        }
    }

    public NetworkBandwidthLimiter get(AsyncNetworkBandwidthLimiter.Type type) {
        AsyncNetworkBandwidthLimiter limiter = limiters.get(type);
        if (limiter == null) {
            return AsyncNetworkBandwidthLimiter.NOOP;
        }
        return limiter;
    }

    public NetworkBandwidthLimiter inbound() {
        return inboundLimiter;
    }

    public  NetworkBandwidthLimiter outbound() {
        return outboundLimiter;
    }

}
