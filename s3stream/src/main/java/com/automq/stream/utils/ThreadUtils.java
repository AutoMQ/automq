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

import org.slf4j.Logger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.concurrent.FastThreadLocalThread;

/**
 * Utilities for working with threads.
 */
public class ThreadUtils {
    /**
     * Create a new ThreadFactory.
     *
     * @param pattern The pattern to use.  If this contains %d, it will be
     *                replaced with a thread number.  It should not contain more
     *                than one %d.
     * @param daemon  True if we want daemon threads.
     * @return The new ThreadFactory.
     */
    public static ThreadFactory createThreadFactory(final String pattern,
        final boolean daemon) {
        return new ThreadFactory() {
            private final AtomicLong threadEpoch = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                String threadName;
                if (pattern.contains("%d")) {
                    threadName = String.format(pattern, threadEpoch.addAndGet(1));
                } else {
                    threadName = pattern;
                }
                Thread thread = new Thread(r, threadName);
                thread.setDaemon(daemon);
                return thread;
            }
        };
    }

    public static ThreadFactory createFastThreadLocalThreadFactory(String pattern, final boolean daemon) {
        return new ThreadFactory() {
            private final AtomicLong threadEpoch = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                String threadName;
                if (pattern.contains("%d")) {
                    threadName = String.format(pattern, threadEpoch.addAndGet(1));
                } else {
                    threadName = pattern;
                }
                FastThreadLocalThread thread = new FastThreadLocalThread(r, threadName);
                thread.setDaemon(daemon);
                return thread;
            }
        };
    }

    public static Runnable wrapRunnable(Runnable runnable, Logger logger) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable throwable) {
                logger.error("[FATAL] Uncaught exception in executor thread {}", Thread.currentThread().getName(), throwable);
            }
        };
    }
}
