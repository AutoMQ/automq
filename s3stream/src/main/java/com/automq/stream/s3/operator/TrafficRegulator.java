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

/**
 * A traffic regulator that adjusts the rate of network traffic based on the success and failure rates.
 */
class TrafficRegulator {
    private static final double FAILED_RATE_THRESHOLD = 0.1; // 10%
    private static final long MIN = 10L << 20;  // 10 MB/s
    private static final long MAX = 1L << 40;  // 1 TB/s
    private static final double INCREMENT_RATIO = 0.5; // 50%
    private static final double DECREMENT_RATIO = 0.0; // 0%

    private final String operation;
    private final TrafficMonitor success;
    private final TrafficMonitor failed;
    private final TrafficLimiter limiter;

    public TrafficRegulator(String operation, TrafficMonitor success, TrafficMonitor failed,
        TrafficLimiter limiter) {
        this.operation = operation;
        this.success = success;
        this.failed = failed;
        this.limiter = limiter;
    }

    public void regulate() {
        regulate0();
    }

    private void regulate0() {
        double successRate = success.getRateAndReset();
        double failedRate = failed.getRateAndReset();
        double totalRate = successRate + failedRate;

        long newRate;
        if (totalRate <= 0 || failedRate / totalRate < FAILED_RATE_THRESHOLD) {
            newRate = increase(limiter.currentRate());
            if (newRate < MAX) {
                AbstractObjectStorage.LOGGER.info("Increase {} rate to {}, success rate: {}, failed rate: {}",
                    operation, formatRate(newRate), formatRate(successRate), formatRate(failedRate));
            }
        } else {
            newRate = decrease(successRate);
            AbstractObjectStorage.LOGGER.info("Decrease {} rate to {}, success rate: {}, failed rate: {}",
                operation, formatRate(newRate), formatRate(successRate), formatRate(failedRate));
        }
        limiter.update(newRate);
    }

    private long increase(double currentLimit) {
        double newRate = currentLimit * (1 + INCREMENT_RATIO);
        return (long) Math.min(newRate, MAX);
    }

    private long decrease(double successRate) {
        double newRate = successRate * (1 - DECREMENT_RATIO);
        return (long) Math.max(newRate, MIN);
    }

    private static String formatRate(double bytesPerSecond) {
        String[] units = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s"};
        int unitIndex = 0;

        while (bytesPerSecond >= 1024 && unitIndex < units.length - 1) {
            bytesPerSecond /= 1024;
            unitIndex++;
        }

        return String.format("%.2f %s", bytesPerSecond, units[unitIndex]);
    }
}
