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
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * Suppresses repeated logs for one caller-owned scope. The constructor keeps the original fixed-interval
 * behavior; the builder opts into bounded failure aggregation and callback-driven exponential logging backoff.
 * Aggregating instances synchronize reporting and recovery across threads and never schedule work.
 */
public class LogSuppressor {
    private final Logger logger;
    private final long intervalMills;
    private final AtomicLong lastLogTime;
    private final AtomicInteger suppressedCount = new AtomicInteger();
    private final FailureAggregation aggregation;

    /** Creates a fixed-interval suppressor with the original suppressed-count prefix and timing behavior. */
    public LogSuppressor(Logger logger, long intervalMills) {
        this.logger = logger;
        this.intervalMills = intervalMills;
        this.lastLogTime = new AtomicLong(System.currentTimeMillis());
        this.aggregation = null;
    }

    private LogSuppressor(Builder builder) {
        this.logger = builder.logger;
        this.intervalMills = 0;
        this.lastLogTime = new AtomicLong();
        this.aggregation = new FailureAggregation(builder);
    }

    /**
     * Starts an aggregating builder with ten fingerprints and 1s/2x/60s logging backoff defaults.
     * Create a separate instance for each independent failure/recovery scope. Without a trailing throwable,
     * the message template supplies the fingerprint and no stack trace is retained.
     */
    public static Builder builder(Logger logger) {
        return new Builder(logger);
    }

    /** Logs or aggregates an ERROR; a trailing throwable supplies the fingerprint and full report stack trace. */
    public void error(String message, Object... args) {
        log(Level.ERROR, message, args);
    }

    /** Logs or aggregates a WARN in this instance's shared failure episode. */
    public void warn(String message, Object... args) {
        log(Level.WARN, message, args);
    }

    /** Logs or aggregates an INFO in this instance's shared failure episode. */
    public void info(String message, Object... args) {
        log(Level.INFO, message, args);
    }

    /**
     * Flushes unreported failures at INFO using the supplied context, then clears all episode state and backoff.
     * Only builder-created aggregating instances support recovery. A subsequent failure logs immediately.
     */
    public void recover(String message, Object... args) {
        if (aggregation == null) {
            throw new IllegalStateException("Recovery requires an aggregating LogSuppressor built with builder()");
        }
        aggregation.recover(message, args == null ? new Object[0] : args);
    }

    private void log(Level level, String message, Object[] args) {
        if (aggregation != null) {
            aggregation.log(level, message, args == null ? new Object[0] : args);
            return;
        }
        long now = System.currentTimeMillis();
        for (; ; ) {
            long last = lastLogTime.get();
            if (now - last > intervalMills) {
                if (lastLogTime.compareAndSet(last, now)) {
                    emit(level, "[SUPPRESSED_TIME=" + suppressedCount.getAndSet(0) + "] " + message, args);
                    break;
                }
            } else {
                suppressedCount.incrementAndGet();
                break;
            }
        }
    }

    private void emit(Level level, String message, Object[] args) {
        switch (level) {
            case ERROR:
                logger.error(message, args);
                break;
            case WARN:
                logger.warn(message, args);
                break;
            case INFO:
                logger.info(message, args);
                break;
            default:
                throw new IllegalArgumentException("Unsupported log level: " + level);
        }
    }

    /** Configures opt-in aggregation without changing existing fixed-interval callers. */
    public static final class Builder {
        private static final long INITIAL_LOG_INTERVAL_MS = 1_000;
        private static final int LOG_BACKOFF_MULTIPLIER = 2;
        private static final long MAX_LOG_INTERVAL_MS = 60_000;
        private static final int MAX_FINGERPRINTS = 10;

        private final Logger logger;
        private LongSupplier timeSource = Time.SYSTEM::hiResClockMs;
        private long initialIntervalMs = INITIAL_LOG_INTERVAL_MS;
        private int multiplier = LOG_BACKOFF_MULTIPLIER;
        private long maxIntervalMs = MAX_LOG_INTERVAL_MS;
        private int maxFingerprints = MAX_FINGERPRINTS;

        private Builder(Logger logger) {
            this.logger = Objects.requireNonNull(logger, "logger");
        }

        /** Uses an injected monotonic millisecond clock, for example {@code time::hiResClockMs}. */
        public Builder timeSource(LongSupplier timeSource) {
            this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
            return this;
        }

        /** Sets positive report intervals and a multiplier of at least one; the first failure is always immediate. */
        public Builder exponentialBackoff(long initialIntervalMs, int multiplier, long maxIntervalMs) {
            if (initialIntervalMs <= 0 || multiplier < 1 || maxIntervalMs < initialIntervalMs) {
                throw new IllegalArgumentException("Require 0 < initialIntervalMs <= maxIntervalMs and multiplier >= 1");
            }
            this.initialIntervalMs = initialIntervalMs;
            this.multiplier = multiplier;
            this.maxIntervalMs = maxIntervalMs;
            return this;
        }

        /** Bounds retained fingerprint keys; every occurrence of an untracked key increments overflow. */
        public Builder maxFingerprints(int maxFingerprints) {
            if (maxFingerprints <= 0) {
                throw new IllegalArgumentException("maxFingerprints must be positive");
            }
            this.maxFingerprints = maxFingerprints;
            return this;
        }

        /** Creates an independent aggregation scope with a snapshot of this builder's configuration. */
        public LogSuppressor build() {
            return new LogSuppressor(this);
        }
    }

    private final class FailureAggregation {
        private final LongSupplier timeSource;
        private final long initialIntervalMs;
        private final int multiplier;
        private final long maxIntervalMs;
        private final int maxFingerprints;
        // Keys contain only values, never exception objects. Counts cover the entire failure episode.
        private final Map<FailureFingerprint, Long> fingerprints = new LinkedHashMap<>();
        private long totalFailures;
        private long unreportedFailures;
        private long overflowCount;
        private long episodeStartMs;
        private long lastReportMs;
        private long logIntervalMs;
        private Throwable latestException;

        private FailureAggregation(Builder builder) {
            timeSource = builder.timeSource;
            initialIntervalMs = builder.initialIntervalMs;
            multiplier = builder.multiplier;
            maxIntervalMs = builder.maxIntervalMs;
            maxFingerprints = builder.maxFingerprints;
            logIntervalMs = initialIntervalMs;
        }

        synchronized void log(Level level, String message, Object[] args) {
            long now = timeSource.getAsLong();
            boolean firstFailure = totalFailures == 0;
            if (firstFailure) {
                episodeStartMs = now;
                lastReportMs = now;
            }
            totalFailures++;
            unreportedFailures++;
            latestException = trailingThrowable(args);
            FailureFingerprint fingerprint = FailureFingerprint.of(latestException, message);
            if (fingerprints.containsKey(fingerprint) || fingerprints.size() < maxFingerprints) {
                fingerprints.merge(fingerprint, 1L, Long::sum);
            } else {
                overflowCount++;
            }
            if (firstFailure || now - lastReportMs >= logIntervalMs) {
                emit(level, message + ",totalFailures={},failuresSinceLastReport={},reportPeriodMs={},fingerprints={},overflowCount={}",
                    reportArgs(args, latestException, totalFailures, unreportedFailures, now - lastReportMs, fingerprints.toString(), overflowCount));
                unreportedFailures = 0;
                lastReportMs = now;
                if (!firstFailure) {
                    logIntervalMs = logIntervalMs > maxIntervalMs / multiplier ? maxIntervalMs : logIntervalMs * multiplier;
                }
            }
        }

        synchronized void recover(String message, Object[] args) {
            if (unreportedFailures > 0) {
                emit(Level.INFO, message + ",unreportedFailures={},totalFailures={},failureDurationMs={},fingerprints={},overflowCount={}",
                    reportArgs(args, null, unreportedFailures, totalFailures, timeSource.getAsLong() - episodeStartMs, fingerprints.toString(), overflowCount));
            }
            totalFailures = 0;
            unreportedFailures = 0;
            overflowCount = 0;
            episodeStartMs = 0;
            lastReportMs = 0;
            logIntervalMs = initialIntervalMs;
            latestException = null;
            fingerprints.clear();
        }
    }

    private static Throwable trailingThrowable(Object[] args) {
        return args.length > 0 && args[args.length - 1] instanceof Throwable ? (Throwable) args[args.length - 1] : null;
    }

    private static Object[] reportArgs(Object[] args, Throwable exception, Object... metadata) {
        int contextLength = args.length - (trailingThrowable(args) == null ? 0 : 1);
        Object[] report = Arrays.copyOf(args, contextLength + metadata.length + (exception == null ? 0 : 1), Object[].class);
        System.arraycopy(metadata, 0, report, contextLength, metadata.length);
        if (exception != null) {
            report[report.length - 1] = exception;
        }
        return report;
    }

    private record FailureFingerprint(String exceptionClass, String message, StackTraceElement topFrame) {
        private static final int MAX_MESSAGE_LENGTH = 512;
        private static final Pattern WHITESPACE = Pattern.compile("\\s+");

        static FailureFingerprint of(Throwable exception, String messageTemplate) {
            Throwable cause = exception == null ? null : FutureUtil.cause(exception);
            String message = cause == null ? messageTemplate : cause.getMessage();
            if (message != null) {
                message = WHITESPACE.matcher(message).replaceAll(" ").trim();
                if (message.length() > MAX_MESSAGE_LENGTH) {
                    message = message.substring(0, MAX_MESSAGE_LENGTH);
                }
            }
            StackTraceElement[] stack = cause == null ? new StackTraceElement[0] : cause.getStackTrace();
            return new FailureFingerprint(cause == null ? null : cause.getClass().getName(), message, stack.length == 0 ? null : stack[0]);
        }
    }
}
