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

import com.google.common.collect.EvictingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class TrafficRegulatorTest {

    private static final long MIN_RATE_LIMITER_RATE = kbpsToBps(1);
    private static final long MAX_RATE_LIMITER_RATE = TrafficRateLimiter.MAX_BUCKET_TOKENS_PER_SECOND << 10;
    private static final long NEAR_MAX_RATE_LIMITER_RATE = (long) (MAX_RATE_LIMITER_RATE * 0.95);

    private static final long FAILURE_RATE = mbpsToBps(10);

    private TrafficMonitor successMonitor;
    private TrafficMonitor failureMonitor;
    private TrafficRateLimiter rateLimiter;
    private TrafficVolumeLimiter volumeLimiter;
    private Logger logger;
    private TrafficRegulator regulator;

    /**
     * Converts a rate given in MB/s to bytes/s.
     */
    private static long mbpsToBps(double mbRate) {
        return (long) (mbRate * (1 << 20));
    }

    /**
     * Converts a rate given in KB/s to bytes/s.
     */
    private static long kbpsToBps(double kbRate) {
        return (long) (kbRate * (1 << 10));
    }

    @BeforeEach
    void setUp() {
        successMonitor = mock(TrafficMonitor.class);
        failureMonitor = mock(TrafficMonitor.class);
        rateLimiter = mock(TrafficRateLimiter.class);
        volumeLimiter = mock(TrafficVolumeLimiter.class);
        logger = mock(Logger.class);
        regulator = new TrafficRegulator("testOperation", successMonitor, failureMonitor, rateLimiter, volumeLimiter, logger);
    }

    // ---------------- Decrease tests (failure rate value does not affect if greater than 0) ----------------

    @Test
    void testRegulateDecreaseSuccessAboveMinWithFailure() {
        long successRate = mbpsToBps(100);
        setRegulatorDecreaseEnv(successRate, MIN_RATE_LIMITER_RATE);
        regulator.regulate();
        checkRegulate(successRate);
    }

    @Test
    void testRegulateDecreaseSuccessBelowMinWithFailure() {
        long successRate = mbpsToBps(5);
        setRegulatorDecreaseEnv(successRate, MIN_RATE_LIMITER_RATE);
        regulator.regulate();
        checkRegulate(getMinRateFromRegulator());
    }

    // ---------------- Increase tests ----------------

    @Test
    void testRegulateIncreaseWithMaxRateLimiter() {
        setRegulatorIncreaseEnv(0, MAX_RATE_LIMITER_RATE);
        regulator.regulate();
        checkRegulate(MAX_RATE_LIMITER_RATE);
    }

    @Test
    void testRegulateIncreaseWithSuccessBelowMinAndRateLimiterMinNoHistory() {
        long successRate = mbpsToBps(5);
        setRegulatorIncreaseEnv(successRate, MIN_RATE_LIMITER_RATE);
        regulator.regulate();
        checkRegulate((long) (MIN_RATE_LIMITER_RATE + successRate * getFastIncrementRatio()));
    }

    @Test
    void testRegulateIncreaseWithSuccessBelowMinAndRateLimiterNearMaxNoHistory() {
        long successRate = mbpsToBps(5);
        setRegulatorIncreaseEnv(successRate, NEAR_MAX_RATE_LIMITER_RATE);
        regulator.regulate();
        checkRegulate(MAX_RATE_LIMITER_RATE);
    }

    @Test
    void testRegulateIncreaseWithSuccessNearMaxAndRateLimiterNearMaxNoHistory() {
        long successRate = (long) (getMaxRateFromRegulator() * 0.95);
        setRegulatorIncreaseEnv(successRate, NEAR_MAX_RATE_LIMITER_RATE);
        regulator.regulate();
        checkRegulate((long) (NEAR_MAX_RATE_LIMITER_RATE + successRate * getSlowIncrementRatio()));
    }

    // ---------------- Tests involving success history ----------------

    @Test
    void testRegulateIncreaseWithHistoryNotFull() {
        // Populate the success history queue with 5 entries
        EvictingQueue<Double> queue = getSuccessRateQueue();
        queue.add((double) mbpsToBps(10.0));
        queue.add((double) mbpsToBps(20.0));
        queue.add((double) mbpsToBps(30.0));
        queue.add((double) mbpsToBps(40.0));
        queue.add((double) mbpsToBps(50.0));
        // Setup current rate to 60 MB/s, failure rate 0, success rate 0
        when(rateLimiter.currentRate()).thenReturn(mbpsToBps(60.0));
        when(successMonitor.getRateAndReset()).thenReturn(0.0);
        when(failureMonitor.getRateAndReset()).thenReturn(0.0);
        regulator.regulate();
        // Expected new rate: second largest of [60 + 50*0.5=85, 60 +50*0.05=62.5, 50] → 62.5 MB/s
        long expectedNewRate = mbpsToBps(62.5);
        verify(rateLimiter).update(expectedNewRate);
        verify(volumeLimiter).update(expectedNewRate * getWindowSize());
    }

    @Test
    void testRegulateIncreaseWithFullHistory() {
        // Inject 64 entries into the queue (top 4: 100, 90, 80, 70)
        EvictingQueue<Double> queue = getSuccessRateQueue();
        queue.add((double) mbpsToBps(100.0));
        queue.add((double) mbpsToBps(90.0));
        queue.add((double) mbpsToBps(80.0));
        queue.add((double) mbpsToBps(70.0));
        for (int i = 0; i < 60; i++) {
            queue.add((double) mbpsToBps(60.0));
        }
        // Setup current rate 80 MB/s, failure rate 0, success rate 0
        when(rateLimiter.currentRate()).thenReturn(mbpsToBps(80.0));
        when(successMonitor.getRateAndReset()).thenReturn(0.0);
        when(failureMonitor.getRateAndReset()).thenReturn(0.0);
        regulator.regulate();
        // Expected new rate: 85 MB/s (mean of top 4 entries)
        long expectedNewRate = mbpsToBps(85.0);
        verify(rateLimiter).update(expectedNewRate);
        verify(volumeLimiter).update(expectedNewRate * getWindowSize());
    }

    @Test
    void testRegulateIncreaseJumpsToMaxWhenCurrentLimitExceedsThreshold() {
        // Setup history with mean 100 MB/s
        EvictingQueue<Double> queue = getSuccessRateQueue();
        for (int i = 0; i < 4; i++) {
            queue.add((double) mbpsToBps(100.0));
        }
        for (int i = 0; i < 60; i++) {
            queue.add((double) mbpsToBps(50.0));
        }
        // Current rate is 701 MB/s (exceeds 7x history rate)
        when(rateLimiter.currentRate()).thenReturn(mbpsToBps(701.0));
        when(successMonitor.getRateAndReset()).thenReturn(0.0);
        when(failureMonitor.getRateAndReset()).thenReturn(0.0);
        regulator.regulate();
        // Verify rate jumps to MAX
        verify(rateLimiter).update(MAX_RATE_LIMITER_RATE);
        verify(volumeLimiter).update(MAX_RATE_LIMITER_RATE * getWindowSize());
    }

    @Test
    void testRegulateFailureDoesNotRecordSuccess() {
        when(successMonitor.getRateAndReset()).thenReturn((double) mbpsToBps(100.0));
        when(failureMonitor.getRateAndReset()).thenReturn((double) mbpsToBps(10.0));
        regulator.regulate();
        EvictingQueue<Double> queue = getSuccessRateQueue();
        assertTrue(queue.isEmpty(), "Queue should be empty as failure occurred");
    }

    @SuppressWarnings("unchecked")
    private EvictingQueue<Double> getSuccessRateQueue() {
        Object field = getField(regulator, "successRateQueue");
        if (field instanceof EvictingQueue) {
            return (EvictingQueue<Double>) field;
        }
        throw new IllegalStateException("Field 'successRateQueue' is not of expected type EvictingQueue<Double>");
    }

    /**
     * Retrieves a static field value via reflection.
     */
    private Object getStaticField(String fieldName) {
        try {
            Field field = TrafficRegulator.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to access " + fieldName + " field", e);
        }
    }

    private Object getField(Object instance, String fieldName) {
        try {
            Field field = TrafficRegulator.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(instance);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getWindowSize() {
        return (int) getStaticField("WINDOW_SIZE");
    }

    private long getMinRateFromRegulator() {
        return (long) getStaticField("MIN");
    }

    private long getMaxRateFromRegulator() {
        return (long) getStaticField("MAX");
    }

    private double getFastIncrementRatio() {
        return (double) getStaticField("FAST_INCREMENT_RATIO");
    }

    private double getSlowIncrementRatio() {
        return (double) getStaticField("SLOW_INCREMENT_RATIO");
    }

    private void setRegulatorDecreaseEnv(double successRate, long limiterRate) {
        setRegulatorEnv(successRate, FAILURE_RATE, limiterRate);
    }

    private void setRegulatorIncreaseEnv(double successRate, long limiterRate) {
        setRegulatorEnv(successRate, 0, limiterRate);
    }

    private void setRegulatorEnv(double successRate, double failureRate, long limiterRate) {
        when(successMonitor.getRateAndReset()).thenReturn(successRate);
        when(failureMonitor.getRateAndReset()).thenReturn(failureRate);
        when(rateLimiter.currentRate()).thenReturn(limiterRate);
    }

    private void checkRegulate(long expectedNewRate) {
        verify(rateLimiter).update(expectedNewRate);
        verify(volumeLimiter).update(expectedNewRate * getWindowSize());
    }
}
