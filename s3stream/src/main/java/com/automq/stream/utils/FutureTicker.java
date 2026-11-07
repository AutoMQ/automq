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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A ticker base on {@link CompletableFuture}. It is used to batch operations.
 * <p>
 * For example, if we want to batch operations every 100ms, we can use the following code:
 * <pre>
 * {@code
 * FutureTicker ticker = new FutureTicker(100, TimeUnit.MILLISECONDS, executor);
 * while (true) {
 *     ticker.tick().thenAccept(v -> operation());
 *     Thread.sleep(1);
 * }
 * }
 * </pre>
 * Operations will be batched every 100ms.
 */
public class FutureTicker {
    private final Executor delayedExecutor;

    private CompletableFuture<Void> currentTick = CompletableFuture.completedFuture(null);

    /**
     * Create a ticker with a delay and a executor
     *
     * @param delay    the delay
     * @param unit     the time unit of the delay
     * @param executor the executor, the {@link CompletableFuture} returned by {@link #tick()} will be completed by this executor
     */
    public FutureTicker(long delay, TimeUnit unit, Executor executor) {
        this.delayedExecutor = CompletableFuture.delayedExecutor(delay, unit, executor);
    }

    /**
     * Tick the ticker. It returns a future which will complete after the delay.
     * If the ticker is already ticking, the same future will be returned.
     * It is thread safe to call this method.
     */
    public CompletableFuture<Void> tick() {
        return maybeNextTick();
    }

    /**
     * Generate a new tick if the current tick is done
     */
    private synchronized CompletableFuture<Void> maybeNextTick() {
        if (currentTick.isDone()) {
            // a future which will complete after delay
            currentTick = CompletableFuture.runAsync(() -> {
            }, delayedExecutor);
        }
        return currentTick;
    }
}
