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
