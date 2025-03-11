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

import com.google.common.collect.Comparators;
import com.google.common.collect.EvictingQueue;
import com.google.common.math.Stats;

import org.slf4j.Logger;

import java.util.List;

/**
 * A traffic regulator that adjusts the rate of network traffic based on the success and failure rates.
 */
class TrafficRegulator {
    private static final long MIN = 10L << 20;  // 10 MB/s
    private static final long MAX = TrafficRateLimiter.MAX_BUCKET_TOKENS_PER_SECOND * 1024;  // 953.67 GB/s

    private static final int HISTORY_SIZE = 64;
    private static final int TOP_SUCCESS_RATE_COUNT = 4;
    private static final double FAST_INCREMENT_RATIO = 0.5;
    private static final double SLOW_INCREMENT_RATIO = 0.05;

    private static final int WINDOW_SIZE = 5; // 5 seconds

    private final String operation;
    private final Logger logger;
    /**
     * A queue to store the success rate (with a zero failure rate) history.
     */
    private final EvictingQueue<Double> successRateQueue = EvictingQueue.create(HISTORY_SIZE);
    private final TrafficMonitor success;
    private final TrafficMonitor failure;
    private final TrafficRateLimiter rateLimiter;
    private final TrafficVolumeLimiter volumeLimiter;

    public TrafficRegulator(String operation, TrafficMonitor success, TrafficMonitor failure,
        TrafficRateLimiter rateLimiter, TrafficVolumeLimiter volumeLimiter, Logger logger) {
        this.operation = operation;
        this.success = success;
        this.failure = failure;
        this.rateLimiter = rateLimiter;
        this.volumeLimiter = volumeLimiter;
        this.logger = logger;
    }

    /**
     * The maximum request size allowed by the regulator.
     * Any request larger than this size should be downscaled to this size.
     */
    public long maxRequestSize() {
        return MIN * WINDOW_SIZE;
    }

    public void regulate() {
        double successRate = success.getRateAndReset();
        double failureRate = failure.getRateAndReset();
        double totalRate = successRate + failureRate;

        maybeRecord(successRate, failureRate);
        long newRate = calculateNewRate(rateLimiter.currentRate(), successRate, failureRate, totalRate);
        rateLimiter.update(newRate);
        volumeLimiter.update(newRate * WINDOW_SIZE);
    }

    private void maybeRecord(double successRate, double failureRate) {
        if (successRate > 0 && failureRate <= 0) {
            successRateQueue.add(successRate);
        }
    }

    private double meanOfTopSuccessRates() {
        if (successRateQueue.isEmpty()) {
            return MIN;
        }

        // Reduce the sample count on warmup
        int topCount = ceilDivide(successRateQueue.size() * TOP_SUCCESS_RATE_COUNT, HISTORY_SIZE);
        List<Double> tops = successRateQueue.stream()
            .collect(Comparators.greatest(topCount, Double::compareTo));
        assert !tops.isEmpty();
        return Stats.meanOf(tops);
    }

    private static int ceilDivide(int dividend, int divisor) {
        return (dividend + divisor - 1) / divisor;
    }

    private long calculateNewRate(double currentLimit, double successRate, double failureRate, double totalRate) {
        boolean isIncrease = totalRate <= 0 || failureRate <= 0;
        long newRate = isIncrease ? increase(currentLimit) : decrease(successRate);

        if (MAX == newRate) {
            // skip logging
            return newRate;
        }
        String action = isIncrease ? "Increase" : "Decrease";
        logger.info("{} {} limit, current limit: {}, success rate: {}, failure rate: {}, new rate: {}",
            action, operation, formatRate(currentLimit), formatRate(successRate), formatRate(failureRate), formatRate(newRate));
        return newRate;
    }

    private long increase(double currentLimit) {
        if (MAX == currentLimit) {
            return MAX;
        }
        double historyRate = meanOfTopSuccessRates();
        if (currentLimit > historyRate * (1 + SLOW_INCREMENT_RATIO * 120)) {
            // If the current limit is higher enough, which means there is and will be no throttling,
            //  so we can just increase the limit to the maximum.
            logger.info("{} limit is high enough, current limit: {}, history rate: {}, new rate: {}",
                operation, formatRate(currentLimit), formatRate(historyRate), formatRate(MAX));
            return MAX;
        }

        List<Double> newRates = List.of(
            currentLimit + historyRate * FAST_INCREMENT_RATIO,
            currentLimit + historyRate * SLOW_INCREMENT_RATIO,
            historyRate
        );

        // Find 2nd largest new rate
        double newRate = newRates.stream()
            .sorted()
            .skip(1)
            .findFirst()
            .get();
        return (long) Math.min(newRate, MAX);
    }

    private long decrease(double successRate) {
        return (long) Math.max(successRate, MIN);
    }

    private static String formatRate(double bytesPerSecond) {
        String[] units = {" B/s", "KB/s", "MB/s", "GB/s", "TB/s"};
        int unitIndex = 0;

        while (bytesPerSecond >= 1024 && unitIndex < units.length - 1) {
            bytesPerSecond /= 1024;
            unitIndex++;
        }

        return String.format("%6.2f %s", bytesPerSecond, units[unitIndex]);
    }
}
