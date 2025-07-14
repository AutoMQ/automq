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
import com.automq.stream.utils.ThreadUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class TrafficRateLimiterTest {

    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void setUp() {
        scheduler = Executors.newScheduledThreadPool(1);
    }

    @AfterEach
    public void tearDown() {
        scheduler.shutdown();
    }

    @Test
    public void testExceedsBoundary() {
        TrafficRateLimiter limiter = new TrafficRateLimiter(scheduler);
        long prev = limiter.currentRate();
        limiter.update(Long.MAX_VALUE);
        assertEquals(prev, limiter.currentRate());
        limiter.update(0);
        assertEquals(1L << 10, limiter.currentRate());
    }

    @Test
    public void testConsumeBeforeUpdate() {
        long rateLimit = 1024 * 1024;
        long totalTraffic = 1024 * 1024 * 5;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch consumeStarted = new CountDownLatch(1);
        TrafficRateLimiter limiter = new TrafficRateLimiter(scheduler, rateLimit);
        Future<Long> future = executor.submit(() -> {
            long startTime = System.currentTimeMillis();
            limiter.consume(totalTraffic).join();
            consumeStarted.countDown();
            long endTime = System.currentTimeMillis();
            return endTime - startTime;
        });

        try {
            consumeStarted.await(); // make sure update after the consume method is called
            long prevRate = limiter.currentRate();
            limiter.update(0);
            long duration = future.get();
            double actualRate = ((double) totalTraffic / 1024 / duration) * 1000;
            assertTrue(actualRate > limiter.currentRate() && actualRate <= prevRate);
            assertTrue(duration / 1000 <= 5);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            ThreadUtils.shutdownExecutor(executor, 1, TimeUnit.SECONDS);
        }
    }
}
