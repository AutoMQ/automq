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

package kafka.automq.retrystorm;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RetryStormBackoffStateStore {
    public static final int DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD = 1;
    public static final int DEFAULT_PROTECTIVE_THRESHOLD = 5;
    public static final long DEFAULT_WINDOW_MS = 1000L;
    public static final long DEFAULT_RECOVERY_QUIET_MS = 1000L;
    public static final int DEFAULT_MAX_TRACKED_DIMENSIONS = 100000;

    private static final String DELAYABLE_TRANSIENT = "delayable-transient";
    private static final String PROTECTIVE_ERROR = "protective-error";

    private final long slidingWindowMs;
    private final long recoveryQuietMs;
    private final long delayMs;
    private final int maxTrackedDimensions;
    private final Cache<BackoffKey, BackoffState> states;

    public RetryStormBackoffStateStore() {
        this(DEFAULT_WINDOW_MS, DEFAULT_RECOVERY_QUIET_MS, 1000L, DEFAULT_MAX_TRACKED_DIMENSIONS);
    }

    public RetryStormBackoffStateStore(long slidingWindowMs, long recoveryQuietMs, long delayMs, int maxTrackedDimensions) {
        this.slidingWindowMs = slidingWindowMs;
        this.recoveryQuietMs = recoveryQuietMs;
        this.delayMs = delayMs;
        this.maxTrackedDimensions = maxTrackedDimensions;
        this.states = CacheBuilder.newBuilder()
            .expireAfterAccess(Math.max(slidingWindowMs, recoveryQuietMs), TimeUnit.MILLISECONDS)
            .build();
    }

    public synchronized StateDecision recordAndDecide(BackoffKey key, ErrorClassSet errorClasses, long nowMs) {
        return recordAndDecide(key, errorClasses, nowMs, delayMs);
    }

    public synchronized StateDecision recordAndDecide(BackoffKey key, ErrorClassSet errorClasses, long nowMs, long decisionDelayMs) {
        BackoffState state = states.getIfPresent(key);
        if (state == null) {
            evictIfNeeded();
            state = new BackoffState();
            states.put(key, state);
        } else if (state.mode == Mode.DELAYING) {
            if (nowMs > state.lastFailureMs + recoveryQuietMs) {
                state.reset();
            } else {
                state.lastFailureMs = nowMs + decisionDelayMs;
                state.delayReason = errorClasses.reason();
                return StateDecision.delayed(decisionDelayMs, state.delayReason);
            }
        }

        state.cleanExpired(nowMs, slidingWindowMs);

        boolean delayableReached = errorClasses.delayableTransient()
            && state.delayableTransientWindow.size() >= DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD;
        boolean protectiveReached = errorClasses.protective()
            && state.protectiveWindow.size() >= DEFAULT_PROTECTIVE_THRESHOLD;

        if (delayableReached || protectiveReached) {
            state.mode = Mode.DELAYING;
            state.lastFailureMs = nowMs + decisionDelayMs;
            state.delayReason = joinReasons(delayableReached, protectiveReached);
            return StateDecision.delayed(decisionDelayMs, state.delayReason);
        }

        if (errorClasses.delayableTransient()) {
            appendBounded(state.delayableTransientWindow, nowMs, DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD);
        }
        if (errorClasses.protective()) {
            appendBounded(state.protectiveWindow, nowMs, DEFAULT_PROTECTIVE_THRESHOLD);
        }
        state.lastFailureMs = nowMs;
        state.delayReason = errorClasses.reason();
        return StateDecision.immediate();
    }

    public synchronized void clear(BackoffKey key) {
        states.invalidate(key);
    }

    public synchronized void evictIfNeeded() {
        states.cleanUp();
        if (states.size() < maxTrackedDimensions) {
            return;
        }
        BackoffKey oldestKey = null;
        long oldestFailureMs = Long.MAX_VALUE;
        for (Map.Entry<BackoffKey, BackoffState> entry : states.asMap().entrySet()) {
            if (entry.getValue().lastFailureMs < oldestFailureMs) {
                oldestFailureMs = entry.getValue().lastFailureMs;
                oldestKey = entry.getKey();
            }
        }
        if (oldestKey != null) {
            states.invalidate(oldestKey);
        }
    }

    private static void appendBounded(ArrayDeque<Long> window, long nowMs, int capacity) {
        if (window.size() == capacity) {
            window.removeFirst();
        }
        window.addLast(nowMs);
    }

    private static String joinReasons(boolean delayableReached, boolean protectiveReached) {
        if (delayableReached && protectiveReached) {
            return DELAYABLE_TRANSIENT + "," + PROTECTIVE_ERROR;
        }
        return delayableReached ? DELAYABLE_TRANSIENT : PROTECTIVE_ERROR;
    }

    private enum Mode {
        CANDIDATE,
        DELAYING
    }

    static class BackoffState {
        private Mode mode = Mode.CANDIDATE;
        private final ArrayDeque<Long> delayableTransientWindow = new ArrayDeque<>(DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD);
        private final ArrayDeque<Long> protectiveWindow = new ArrayDeque<>(DEFAULT_PROTECTIVE_THRESHOLD);
        private long lastFailureMs;
        private String delayReason = "";

        private void cleanExpired(long nowMs, long slidingWindowMs) {
            cleanExpired(delayableTransientWindow, nowMs, slidingWindowMs);
            cleanExpired(protectiveWindow, nowMs, slidingWindowMs);
        }

        private void reset() {
            mode = Mode.CANDIDATE;
            delayableTransientWindow.clear();
            protectiveWindow.clear();
            delayReason = "";
        }

        private static void cleanExpired(ArrayDeque<Long> window, long nowMs, long slidingWindowMs) {
            for (Iterator<Long> iterator = window.iterator(); iterator.hasNext(); ) {
                long timestampMs = iterator.next();
                if (nowMs - timestampMs > slidingWindowMs) {
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
    }

    public record BackoffKey(short apiKey, String resourceKey, String clientScope) {
        public BackoffKey {
            Objects.requireNonNull(resourceKey, "resourceKey");
            Objects.requireNonNull(clientScope, "clientScope");
        }
    }

    public record ErrorClassSet(boolean delayableTransient, boolean protective) {
        public static ErrorClassSet delayableTransientError() {
            return new ErrorClassSet(true, true);
        }

        public static ErrorClassSet protectiveOnlyError() {
            return new ErrorClassSet(false, true);
        }

        public String reason() {
            return joinReasons(delayableTransient, protective && !delayableTransient);
        }
    }

    public record StateDecision(boolean delayed, long delayMs, String reason) {
        public static StateDecision immediate() {
            return new StateDecision(false, 0L, "");
        }

        public static StateDecision delayed(long delayMs, String reason) {
            return new StateDecision(delayMs > 0, delayMs, reason);
        }
    }
}
