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

package com.automq.stream.s3.backpressure;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DefaultBackPressureManagerTest {
    static String sourceA = "sourceA";
    static String sourceB = "sourceB";
    static String sourceC = "sourceC";

    DefaultBackPressureManager manager;

    Regulator regulator;
    int regulatorIncreaseCalled = 0;
    int regulatorDecreaseCalled = 0;
    int regulatorMinimizeCalled = 0;

    ScheduledExecutorService scheduler;
    int schedulerScheduleCalled = 0;
    long schedulerScheduleDelay = 0;

    @BeforeEach
    public void setup() {
        regulator = mock(Regulator.class);
        scheduler = mock(ScheduledExecutorService.class);

        // Mock the regulator to count the number of times each method is called
        doAnswer(invocation -> {
            regulatorIncreaseCalled++;
            return null;
        }).when(regulator).increase();
        doAnswer(invocation -> {
            regulatorDecreaseCalled++;
            return null;
        }).when(regulator).decrease();
        doAnswer(invocation -> {
            regulatorMinimizeCalled++;
            return null;
        }).when(regulator).minimize();

        // Mock the scheduler to run the scheduled task immediately and only once
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(scheduler).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            schedulerScheduleCalled++;
            schedulerScheduleDelay = invocation.getArgument(1);
            return null;
        }).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testPriority1() {
        initManager(0);

        callChecker(sourceA, LoadLevel.CRITICAL);
        callChecker(sourceB, LoadLevel.HIGH);
        callChecker(sourceC, LoadLevel.NORMAL);

        assertRegulatorCalled(0, 0, 3);
    }

    @Test
    public void testPriority2() {
        initManager(0);

        callChecker(sourceC, LoadLevel.NORMAL);
        callChecker(sourceB, LoadLevel.HIGH);
        callChecker(sourceA, LoadLevel.CRITICAL);

        assertRegulatorCalled(1, 1, 1);
    }

    @Test
    public void testOverride() {
        initManager(0);

        callChecker(sourceA, LoadLevel.NORMAL);
        callChecker(sourceA, LoadLevel.HIGH);
        callChecker(sourceA, LoadLevel.CRITICAL);
        callChecker(sourceA, LoadLevel.NORMAL);

        assertRegulatorCalled(2, 1, 1);
    }

    @Test
    public void testCooldown() {
        final long cooldownMs = Long.MAX_VALUE;
        final long tolerance = 1000;

        initManager(cooldownMs);

        callChecker(sourceA, LoadLevel.CRITICAL);
        assertRegulatorCalled(0, 0, 1);
        assertSchedulerCalled(0);

        callChecker(sourceA, LoadLevel.HIGH);
        assertRegulatorCalled(0, 0, 1);
        assertSchedulerCalled(1);
        assertEquals(cooldownMs, schedulerScheduleDelay, tolerance);

        callChecker(sourceA, LoadLevel.NORMAL);
        assertRegulatorCalled(0, 0, 1);
        assertSchedulerCalled(2);
        assertEquals(cooldownMs, schedulerScheduleDelay, tolerance);

        callChecker(sourceA, LoadLevel.CRITICAL);
        assertRegulatorCalled(0, 0, 2);
        assertSchedulerCalled(2);
        assertEquals(cooldownMs, schedulerScheduleDelay, tolerance);
    }

    /**
     * Should be called at the beginning of each test to initialize the manager.
     */
    private void initManager(long cooldownMs) {
        manager = new DefaultBackPressureManager(regulator, cooldownMs);
        manager.checkerScheduler = scheduler;
    }

    private void callChecker(String source, LoadLevel level) {
        manager.registerChecker(new Checker() {
            @Override
            public String source() {
                return source;
            }

            @Override
            public LoadLevel check() {
                return level;
            }

            @Override
            public long intervalMs() {
                return 1;
            }
        });
    }

    private void assertRegulatorCalled(int increase, int decrease, int minimize) {
        assertEquals(increase, regulatorIncreaseCalled);
        assertEquals(decrease, regulatorDecreaseCalled);
        assertEquals(minimize, regulatorMinimizeCalled);
    }

    private void assertSchedulerCalled(int times) {
        assertEquals(times, schedulerScheduleCalled);
    }
}
