/*
 * Copyright 2026, AutoMQ HK Limited.
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
import org.slf4j.Marker;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * An interim asynchronous wrapper for INFO logs on latency-sensitive paths.
 *
 * <p>INFO calls are put into a bounded process-wide queue and executed by a daemon thread. When
 * the queue is full, new INFO events are dropped. Other levels retain the delegate logger's
 * synchronous behavior. This wrapper can be removed once the runtime uses a native asynchronous
 * Log4j2 configuration.</p>
 */
public final class AsyncLogger implements Logger {
    private static final int QUEUE_CAPACITY = 10_000;
    private static final Logger INTERNAL_LOGGER = LoggerFactory.getLogger(AsyncLogger.class);
    private static final BlockingQueue<Runnable> EVENTS = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private static final LongAdder DROPPED_EVENTS = new LongAdder();

    static {
        Thread reporter = new Thread(AsyncLogger::run, "automq-async-logger");
        reporter.setDaemon(true);
        reporter.start();
    }

    private final Logger delegate;

    private AsyncLogger(Logger delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /** Wrap a logger so that its INFO calls do not block the caller. */
    public static Logger wrap(Logger logger) {
        if (logger instanceof AsyncLogger) {
            return logger;
        }
        return new AsyncLogger(logger);
    }

    private static void run() {
        long lastDropReportNanos = System.nanoTime();
        while (true) {
            try {
                Runnable event = EVENTS.poll(1, TimeUnit.SECONDS);
                if (event != null) {
                    event.run();
                }
                long nowNanos = System.nanoTime();
                if (nowNanos - lastDropReportNanos >= TimeUnit.SECONDS.toNanos(1)) {
                    long dropped = DROPPED_EVENTS.sumThenReset();
                    if (dropped != 0) {
                        INTERNAL_LOGGER.warn(
                            "Dropped {} asynchronous INFO log events because the queue is full", dropped);
                    }
                    lastDropReportNanos = nowNanos;
                }
            } catch (InterruptedException ignored) {
                // The reporter is process-scoped and only terminates with the JVM.
            } catch (Throwable ex) {
                INTERNAL_LOGGER.warn("Failed to write an asynchronous INFO log event", ex);
            }
        }
    }

    private static void enqueue(Runnable event) {
        if (!EVENTS.offer(event)) {
            DROPPED_EVENTS.increment();
        }
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
        delegate.trace(msg);
    }

    @Override
    public void trace(String format, Object arg) {
        delegate.trace(format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        delegate.trace(format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        delegate.trace(format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        delegate.trace(msg, t);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return delegate.isTraceEnabled(marker);
    }

    @Override
    public void trace(Marker marker, String msg) {
        delegate.trace(marker, msg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        delegate.trace(marker, format, arg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        delegate.trace(marker, format, arg1, arg2);
    }

    @Override
    public void trace(Marker marker, String format, Object... arguments) {
        delegate.trace(marker, format, arguments);
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        delegate.trace(marker, msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
        delegate.debug(msg);
    }

    @Override
    public void debug(String format, Object arg) {
        delegate.debug(format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        delegate.debug(format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        delegate.debug(format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        delegate.debug(msg, t);
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return delegate.isDebugEnabled(marker);
    }

    @Override
    public void debug(Marker marker, String msg) {
        delegate.debug(marker, msg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        delegate.debug(marker, format, arg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        delegate.debug(marker, format, arg1, arg2);
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        delegate.debug(marker, format, arguments);
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        delegate.debug(marker, msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
        if (delegate.isInfoEnabled()) {
            enqueue(() -> delegate.info(msg));
        }
    }

    @Override
    public void info(String format, Object arg) {
        if (delegate.isInfoEnabled()) {
            enqueue(() -> delegate.info(format, arg));
        }
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        if (delegate.isInfoEnabled()) {
            enqueue(() -> delegate.info(format, arg1, arg2));
        }
    }

    @Override
    public void info(String format, Object... arguments) {
        if (delegate.isInfoEnabled()) {
            Object[] argumentsCopy = arguments.clone();
            enqueue(() -> delegate.info(format, argumentsCopy));
        }
    }

    @Override
    public void info(String msg, Throwable t) {
        if (delegate.isInfoEnabled()) {
            enqueue(() -> delegate.info(msg, t));
        }
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return delegate.isInfoEnabled(marker);
    }

    @Override
    public void info(Marker marker, String msg) {
        if (delegate.isInfoEnabled(marker)) {
            enqueue(() -> delegate.info(marker, msg));
        }
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        if (delegate.isInfoEnabled(marker)) {
            enqueue(() -> delegate.info(marker, format, arg));
        }
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        if (delegate.isInfoEnabled(marker)) {
            enqueue(() -> delegate.info(marker, format, arg1, arg2));
        }
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        if (delegate.isInfoEnabled(marker)) {
            Object[] argumentsCopy = arguments.clone();
            enqueue(() -> delegate.info(marker, format, argumentsCopy));
        }
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        if (delegate.isInfoEnabled(marker)) {
            enqueue(() -> delegate.info(marker, msg, t));
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
        delegate.warn(msg);
    }

    @Override
    public void warn(String format, Object arg) {
        delegate.warn(format, arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        delegate.warn(format, arguments);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        delegate.warn(format, arg1, arg2);
    }

    @Override
    public void warn(String msg, Throwable t) {
        delegate.warn(msg, t);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return delegate.isWarnEnabled(marker);
    }

    @Override
    public void warn(Marker marker, String msg) {
        delegate.warn(marker, msg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        delegate.warn(marker, format, arg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        delegate.warn(marker, format, arg1, arg2);
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        delegate.warn(marker, format, arguments);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        delegate.warn(marker, msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public void error(String msg) {
        delegate.error(msg);
    }

    @Override
    public void error(String format, Object arg) {
        delegate.error(format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        delegate.error(format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        delegate.error(format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        delegate.error(msg, t);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return delegate.isErrorEnabled(marker);
    }

    @Override
    public void error(Marker marker, String msg) {
        delegate.error(marker, msg);
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        delegate.error(marker, format, arg);
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        delegate.error(marker, format, arg1, arg2);
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        delegate.error(marker, format, arguments);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        delegate.error(marker, msg, t);
    }
}
