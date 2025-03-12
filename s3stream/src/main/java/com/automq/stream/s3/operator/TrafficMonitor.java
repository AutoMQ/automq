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
