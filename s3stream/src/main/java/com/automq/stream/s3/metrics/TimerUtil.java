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

package com.automq.stream.s3.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class TimerUtil {
    private final AtomicLong last = new AtomicLong(System.nanoTime());

    public TimerUtil() {
        reset();
    }

    public void reset() {
        last.set(System.nanoTime());
    }

    public long lastAs(TimeUnit timeUnit) {
        return timeUnit.convert(last.get(), TimeUnit.NANOSECONDS);
    }

    public long elapsedAs(TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime() - last.get(), TimeUnit.NANOSECONDS);
    }

    public long elapsedAndResetAs(TimeUnit timeUnit) {
        long now = System.nanoTime();
        return timeUnit.convert(now - last.getAndSet(now), TimeUnit.NANOSECONDS);
    }

    public static long durationElapsedAs(long statNanoTime, TimeUnit timeUnit) {
        return timeUnit.convert(System.nanoTime() - statNanoTime, TimeUnit.NANOSECONDS);
    }

}
