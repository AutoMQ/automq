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

package kafka.automq.backpressure;

import kafka.automq.AutoMQConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DefaultBackPressureManagerTest {
    static String sourceA = "sourceA";
    static String sourceB = "sourceB";
    static String sourceC = "sourceC";

    BackPressureConfig config;
    DefaultBackPressureManager manager;

    Regulator regulator;
    int regulatorIncreaseCalled = 0;
    int regulatorDecreaseCalled = 0;

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

        // Mock the scheduler to run the scheduled task immediately and only once
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(scheduler).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            schedulerScheduleCalled++;
            schedulerScheduleDelay = invocation.getArgument(1);
            return null;
        }).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testDynamicConfig() {
        initManager(false, 0);

        callChecker(sourceC, LoadLevel.NORMAL);
        callChecker(sourceB, LoadLevel.HIGH);
        assertRegulatorCalled(0, 0);

        manager.reconfigure(Map.of(
            AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG, "true"
        ));
        callChecker(sourceC, LoadLevel.NORMAL);
        callChecker(sourceB, LoadLevel.NORMAL);
        assertRegulatorCalled(1, 1);

        manager.reconfigure(Map.of(
            AutoMQConfig.S3_BACK_PRESSURE_ENABLED_CONFIG, "false"
        ));
        callChecker(sourceC, LoadLevel.NORMAL);
        callChecker(sourceB, LoadLevel.HIGH);
        assertRegulatorCalled(1, 1);
    }

    @Test
    public void testPriority1() {
        initManager(0);

        callChecker(sourceB, LoadLevel.HIGH);
        callChecker(sourceC, LoadLevel.NORMAL);

        assertRegulatorCalled(0, 2);
    }

    @Test
    public void testPriority2() {
        initManager(0);

        callChecker(sourceC, LoadLevel.NORMAL);
        callChecker(sourceB, LoadLevel.HIGH);

        assertRegulatorCalled(1, 1);
    }

    @Test
    public void testOverride() {
        initManager(0);

        callChecker(sourceA, LoadLevel.NORMAL);
        callChecker(sourceA, LoadLevel.HIGH);
        callChecker(sourceA, LoadLevel.NORMAL);

        assertRegulatorCalled(2, 1);
    }

    @Test
    public void testCooldown() {
        final long cooldownMs = Long.MAX_VALUE;
        final long tolerance = 1000;

        initManager(cooldownMs);

        callChecker(sourceA, LoadLevel.HIGH);
        assertRegulatorCalled(0, 0);
        assertSchedulerCalled(1);
        assertEquals(cooldownMs, schedulerScheduleDelay, tolerance);

        callChecker(sourceA, LoadLevel.NORMAL);
        assertRegulatorCalled(0, 0);
        assertSchedulerCalled(2);
        assertEquals(cooldownMs, schedulerScheduleDelay, tolerance);
    }

    private void initManager(long cooldownMs) {
        initManager(true, cooldownMs);
    }

    /**
     * Should be called at the beginning of each test to initialize the manager.
     */
    private void initManager(boolean enabled, long cooldownMs) {
        config = new BackPressureConfig(enabled, cooldownMs);
        manager = new DefaultBackPressureManager(config, regulator);
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

    private void assertRegulatorCalled(int increase, int decrease) {
        assertEquals(increase, regulatorIncreaseCalled);
        assertEquals(decrease, regulatorDecreaseCalled);
    }

    private void assertSchedulerCalled(int times) {
        assertEquals(times, schedulerScheduleCalled);
    }
}
