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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

public class LogSuppressor {
    private final Logger logger;
    private final long intervalMills;
    private final AtomicLong lastLogTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicInteger suppressedCount = new AtomicInteger();

    public LogSuppressor(Logger logger, long intervalMills) {
        this.logger = logger;
        this.intervalMills = intervalMills;
    }

    public void warn(String message, Object... args) {
        long now = System.currentTimeMillis();
        for (; ; ) {
            long last = lastLogTime.get();
            if (now - last > intervalMills) {
                if (lastLogTime.compareAndSet(last, now)) {
                    logger.warn("[SUPPRESSED_TIME=" + suppressedCount.getAndSet(0) + "] " + message, args);
                    break;
                }
            } else {
                suppressedCount.incrementAndGet();
                break;
            }
        }
    }

    public void info(String message, Object... args) {
        long now = System.currentTimeMillis();
        for (; ; ) {
            long last = lastLogTime.get();
            if (now - last > intervalMills) {
                if (lastLogTime.compareAndSet(last, now)) {
                    logger.info("[SUPPRESSED_TIME=" + suppressedCount.getAndSet(0) + "] " + message, args);
                    break;
                }
            } else {
                suppressedCount.incrementAndGet();
                break;
            }
        }
    }

}
