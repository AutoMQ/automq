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
    private static final long MAX = 1L << 40;  // 1 TB/s

    private static final int HISTORY_SIZE = 64;
    private static final int TOP_SUCCESS_RATE_COUNT = 4;
    private static final double FAST_INCREMENT_RATIO = 0.5;
    private static final double SLOW_INCREMENT_RATIO = 0.2;

    private final String operation;
    private final Logger logger;
    /**
     * A queue to store the success rate (with a zero failure rate) history.
     */
    private final EvictingQueue<Double> successRateQueue = EvictingQueue.create(HISTORY_SIZE);
    private final TrafficMonitor success;
    private final TrafficMonitor failure;
    private final TrafficLimiter limiter;

    public TrafficRegulator(String operation, TrafficMonitor success, TrafficMonitor failure,
        TrafficLimiter limiter, Logger logger) {
        this.operation = operation;
        this.success = success;
        this.failure = failure;
        this.limiter = limiter;
        this.logger = logger;
    }

    public void regulate() {
        regulate0();
    }

    private void regulate0() {
        double successRate = success.getRateAndReset();
        double failureRate = failure.getRateAndReset();
        double totalRate = successRate + failureRate;

        maybeRecord(successRate, failureRate);
        long newRate = calculateNewRate(limiter.currentRate(), successRate, failureRate, totalRate);
        limiter.update(newRate);
    }

    private void maybeRecord(double successRate, double failureRate) {
        if (successRate > 0 && failureRate <= 0) {
            successRateQueue.add(successRate);
        }
    }

    private double meanOfTopSuccessRates() {
        List<Double> tops = successRateQueue.stream()
            .collect(Comparators.greatest(TOP_SUCCESS_RATE_COUNT, Double::compareTo));
        if (tops.isEmpty()) {
            return MIN;
        }
        return Stats.meanOf(tops);
    }

    private long calculateNewRate(double currentLimit, double successRate, double failureRate, double totalRate) {
        boolean isIncrease = totalRate <= 0 || failureRate <= 0;
        long newRate = isIncrease ? increase(currentLimit) : decrease(successRate);

        if (newRate > MAX * 0.99) {
            // skip logging
            return newRate;
        }
        String action = isIncrease ? "Increase" : "Decrease";
        logger.info("{} {} limit, current limit: {}, success rate: {}, failure rate: {}, new rate: {}",
            action, operation, formatRate(currentLimit), formatRate(successRate), formatRate(failureRate), formatRate(newRate));
        return newRate;
    }

    private long increase(double currentLimit) {
        double historyRate = meanOfTopSuccessRates();
        if (currentLimit > historyRate * SLOW_INCREMENT_RATIO * 20) {
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
