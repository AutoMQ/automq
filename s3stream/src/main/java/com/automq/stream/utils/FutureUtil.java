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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

public class FutureUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(FutureUtil.class);
    private static final HashedWheelTimer TIMEOUT_DETECT = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("timeout-detect", true), 1, TimeUnit.SECONDS, 100);
    private static final AtomicLong TIMEOUT_DETECT_ID_ALLOC = new AtomicLong();

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

    public static <T> void propagateAsync(CompletableFuture<T> source, CompletableFuture<T> dest,
        ExecutorService executor) {
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

    public static <T> CompletableFuture<T> timeoutWithNewReturn(CompletableFuture<T> detectCf, int delay,
        TimeUnit timeUnit, Runnable timeoutAction) {
        if (detectCf.isDone()) {
            return detectCf;
        }
        long timeoutDetectId = TIMEOUT_DETECT_ID_ALLOC.incrementAndGet();
        CompletableFuture<T> newCf = new CompletableFuture<>();
        Timeout timeout = TIMEOUT_DETECT.newTimeout(t -> {
            synchronized (t) {
                LOGGER.info("[TIME_DETECT],{}", timeoutDetectId);
                timeoutAction.run();
                newCf.completeExceptionally(new TimeoutException());
            }
        }, delay, timeUnit);
        detectCf.whenComplete((rst, ex) -> {
            synchronized (timeout) {
                if (timeout.isExpired()) {
                    LOGGER.info("[TIME_DETECT_RECOVERED],{}", timeoutDetectId);
                } else {
                    timeout.cancel();
                    if (ex != null) {
                        newCf.completeExceptionally(ex);
                    } else {
                        newCf.complete(rst);
                    }
                }
            }
        });
        return newCf;
    }

    public static List<CompletableFuture<?>> timeoutAndSilence(Stream<CompletableFuture<?>> stream, long timeout, TimeUnit timeUnit) {
        return stream.map(l -> timeoutAndSilence(l, timeout, timeUnit)).collect(Collectors.toList());
    }

    public static <T> CompletableFuture<T> timeoutAndSilence(CompletableFuture<T> cf, long timeout, TimeUnit timeUnit) {
        return cf.orTimeout(timeout, timeUnit).exceptionally(ex -> null);
    }
}
