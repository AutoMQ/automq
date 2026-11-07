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

import com.automq.stream.s3.metrics.wrapper.Counter;

/**
 * A simple traffic monitor that counts the number of bytes transferred over time.
 */
class TrafficMonitor {
    private final Counter trafficCounter = new Counter();
    private long lastTime = System.nanoTime();
    private long lastTraffic = 0;

    /**
     * Record the number of bytes transferred.
     * This method is thread-safe and can be called from multiple threads.
     */
    public void record(long bytes) {
        trafficCounter.inc(bytes);
    }

    /**
     * Calculate the rate of bytes transferred since the last call to this method, and reset the counter.
     * Note: This method is not thread-safe.
     */
    public double getRateAndReset() {
        long currentTime = System.nanoTime();
        long deltaTime = currentTime - lastTime;
        if (deltaTime <= 0) {
            return 0;
        }

        long currentTraffic = trafficCounter.get();
        long deltaTraffic = currentTraffic - lastTraffic;

        lastTime = currentTime;
        lastTraffic = currentTraffic;

        return deltaTraffic * 1e9 / deltaTime;
    }
}
