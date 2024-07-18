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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.slf4j.Logger;

public class FutureUtil {
    public static <T> CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        cf.completeExceptionally(ex);
        return cf;
    }

    public static void suppress(ThrowableRunnable run, Logger logger) {
        try {
            run.run();
        } catch (Throwable t) {
            logger.error("Suppress error", t);
        }
    }

    /**
     * Propagate CompleteFuture result / error from source to dest.
     */
    public static <T> void propagate(CompletableFuture<T> source, CompletableFuture<T> dest) {
        source.whenComplete((rst, ex) -> {
            if (ex != null) {
                dest.completeExceptionally(ex);
            } else {
                dest.complete(rst);
            }
        });
    }

    public static <T> void propagateAsync(CompletableFuture<T> source, CompletableFuture<T> dest, ExecutorService executor) {
        source.whenCompleteAsync((rst, ex) -> {
            if (ex != null) {
                dest.completeExceptionally(ex);
            } else {
                dest.complete(rst);
            }
        }, executor);
    }

    /**
     * Catch exceptions as a last resort to avoid unresponsiveness.
     */
    public static <T> CompletableFuture<T> exec(Supplier<CompletableFuture<T>> run, Logger logger, String name) {
        try {
            return run.get();
        } catch (Throwable ex) {
            logger.error("{} run with unexpected exception", name, ex);
            return failedFuture(ex);
        }
    }

    /**
     * Catch exceptions as a last resort to avoid unresponsiveness.
     */
    public static <T> void exec(Runnable run, CompletableFuture<T> cf, Logger logger, String name) {
        try {
            run.run();
        } catch (Throwable ex) {
            logger.error("{} run with unexpected exception", name, ex);
            cf.completeExceptionally(ex);
        }
    }

    public static Throwable cause(Throwable ex) {
        if (ex instanceof ExecutionException) {
            if (ex.getCause() != null) {
                return cause(ex.getCause());
            } else {
                return ex;
            }
        } else if (ex instanceof CompletionException) {
            if (ex.getCause() != null) {
                return cause(ex.getCause());
            } else {
                return ex;
            }
        }
        return ex;
    }

    public static <T> void completeExceptionally(Iterator<CompletableFuture<T>> futures, Throwable ex) {
        while (futures.hasNext()) {
            CompletableFuture<T> future = futures.next();
            future.completeExceptionally(ex);
        }
    }

    public static <T> void complete(Iterator<CompletableFuture<T>> futures, T value) {
        while (futures.hasNext()) {
            CompletableFuture<T> future = futures.next();
            future.complete(value);
        }
    }

}
