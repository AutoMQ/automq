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

package kafka.automq.zerozone;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.Time;

import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Aggregates exceptional subscription completions for one requester/destination. Callbacks may run on the
 * sender thread or the event loop, so all episode state and report emission are guarded by this monitor.
 * Reports are driven only by completions; this class never schedules or delays requests.
 */
class SnapshotSubscriptionFailureLogger {
    private static final long INITIAL_LOG_INTERVAL_MS = 1_000;
    private static final int LOG_BACKOFF_MULTIPLIER = 2;
    private static final long MAX_LOG_INTERVAL_MS = 60_000;
    private static final int MAX_FINGERPRINTS = 10;
    private static final int MAX_MESSAGE_LENGTH = 512;
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private final Node node;
    private final Time time;
    private final Logger logger;
    // Keys contain only values, never exception objects. Counts cover the entire failure episode.
    private final Map<FailureFingerprint, Long> fingerprints = new LinkedHashMap<>();
    private long totalFailures;
    private long unreportedFailures;
    private long overflowCount;
    private long episodeStartMs;
    private long lastReportMs;
    private long logIntervalMs = INITIAL_LOG_INTERVAL_MS;
    private Throwable latestException;

    SnapshotSubscriptionFailureLogger(Node node, Time time, Logger logger) {
        this.node = node;
        this.time = time;
        this.logger = logger;
    }

    synchronized void onFailure(Throwable exception) {
        long now = time.hiResClockMs();
        boolean firstFailure = totalFailures == 0;
        if (firstFailure) {
            episodeStartMs = now;
            lastReportMs = now;
        }
        totalFailures++;
        unreportedFailures++;
        latestException = exception;
        FailureFingerprint fingerprint = FailureFingerprint.of(exception);
        if (fingerprints.containsKey(fingerprint) || fingerprints.size() < MAX_FINGERPRINTS) {
            fingerprints.merge(fingerprint, 1L, Long::sum);
        } else {
            // Count every occurrence of an untracked fingerprint without retaining it or its throwable.
            overflowCount++;
        }
        if (firstFailure || now - lastReportMs >= logIntervalMs) {
            logger.error("[SNAPSHOT_SUBSCRIBE_ERROR],node={},totalFailures={},failuresSinceLastReport={},reportPeriodMs={},fingerprints={},overflowCount={}",
                node, totalFailures, unreportedFailures, now - lastReportMs, fingerprints.toString(), overflowCount, latestException);
            unreportedFailures = 0;
            lastReportMs = now;
            if (!firstFailure) {
                logIntervalMs = Math.min(MAX_LOG_INTERVAL_MS, logIntervalMs * LOG_BACKOFF_MULTIPLIER);
            }
        }
    }

    synchronized void onSuccess() {
        if (unreportedFailures > 0) {
            logger.info("[SNAPSHOT_SUBSCRIBE_RECOVERED],node={},unreportedFailures={},totalFailures={},failureDurationMs={},fingerprints={},overflowCount={}",
                node, unreportedFailures, totalFailures, time.hiResClockMs() - episodeStartMs, fingerprints.toString(), overflowCount);
        }
        totalFailures = 0;
        unreportedFailures = 0;
        overflowCount = 0;
        episodeStartMs = 0;
        lastReportMs = 0;
        logIntervalMs = INITIAL_LOG_INTERVAL_MS;
        latestException = null;
        fingerprints.clear();
    }

    private record FailureFingerprint(String exceptionClass, String message, StackTraceElement topFrame) {
        static FailureFingerprint of(Throwable exception) {
            // CompletableFuture wraps sender failures. Group by the underlying failure but log the full wrapper/cause chain.
            Throwable cause = FutureUtil.cause(exception);
            String message = cause.getMessage();
            // Normalize whitespace only: preserve diagnostic values, and bound each retained message as well as the map.
            if (message != null) {
                message = WHITESPACE.matcher(message).replaceAll(" ").trim();
                if (message.length() > MAX_MESSAGE_LENGTH) {
                    message = message.substring(0, MAX_MESSAGE_LENGTH);
                }
            }
            StackTraceElement[] stack = cause.getStackTrace();
            return new FailureFingerprint(cause.getClass().getName(), message, stack.length == 0 ? null : stack[0]);
        }
    }
}
