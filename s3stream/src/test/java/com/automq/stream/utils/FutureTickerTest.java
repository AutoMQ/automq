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

package com.automq.stream.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class FutureTickerTest {

    private FutureTicker ticker = new FutureTicker(10, TimeUnit.MILLISECONDS, Executors.newSingleThreadExecutor());

    @BeforeEach
    void setUp() {
        ticker = new FutureTicker(10, TimeUnit.MILLISECONDS, Executors.newSingleThreadExecutor());
    }

    @Test
    void testFirstTick() {
        CompletableFuture<Void> tick = ticker.tick();
        assertNotNull(tick);
        assertFalse(tick.isDone());
    }

    @Test
    void testTwoTicks() {
        CompletableFuture<Void> tick1 = ticker.tick();
        CompletableFuture<Void> tick2 = ticker.tick();
        assertSame(tick1, tick2);
    }

    @Test
    void testDelay() throws InterruptedException {
        CompletableFuture<Void> tick1 = ticker.tick();
        // complete the first tick manually to mock the delay
        // Thread.sleep(20);
        tick1.complete(null);
        CompletableFuture<Void> tick2 = ticker.tick();
        assertFalse(tick2.isDone());
        assertNotSame(tick1, tick2);
    }
}
