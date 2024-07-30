/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
    public static final Object BURST_UPLOAD_OBJECT = new Object();

    private static long burstUploadTickTime = 100;

    static {
        burstUploadTickTime = Systems.getEnvInt("AUTOMQ_BURST_UPLOAD_DELAY_TICK_TIME", 100);
    }

    private final Executor delayedExecutor;

    private CompletableFuture<Void> currentTick = CompletableFuture.completedFuture(null);
    private long lastTickTime = -1;

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
    public CompletableFuture<Void> tick(boolean burstUpload) {
        return maybeNextTick(burstUpload);
    }

    public CompletableFuture<Void> tick() {
        return maybeNextTick(false);
    }

    /**
     * Generate a new tick if the current tick is done
     */
    private synchronized CompletableFuture<Void> maybeNextTick(boolean burstUpload) {
        if (currentTick.isDone()) {
            lastTickTime = System.currentTimeMillis();

            if (burstUpload) {
                // spot bugs report the null can't pass but this is supported
                currentTick = new CompletableFuture<>()
                    .completeOnTimeout(BURST_UPLOAD_OBJECT, burstUploadTickTime, TimeUnit.MILLISECONDS).thenAccept(__ -> {
                    });
            } else {
                // a future which will complete after delay
                currentTick = CompletableFuture.runAsync(() -> {
                }, delayedExecutor);
            }

            return currentTick;
        }

        if (burstUpload) {
            if (System.currentTimeMillis() - lastTickTime > burstUploadTickTime) {
                currentTick.complete(null);
            }
        }

        return currentTick;
    }
}
