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

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintains per-resource retry storm backoff state for policy evaluation.
 *
 * <p>Each key tracks independent delayable-transient and protective-error windows,
 * plus a delaying mode that keeps delaying repeated failures until quiet time expires.
 * The store owns state lifecycle while each {@link BackoffState} owns its per-resource
 * state machine and synchronization.</p>
 */
public class RetryStormBackoffStateStore {
    public static final int DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD = 1;
    public static final int DEFAULT_PROTECTIVE_THRESHOLD = 5;
    public static final long DEFAULT_WINDOW_MS = 1000L;
    public static final long DEFAULT_RECOVERY_QUIET_MS = 1000L;
    public static final long DEFAULT_DELAY_MS = 1000L;
    public static final int DEFAULT_MAX_TRACKED_DIMENSIONS = 100000;

    private static final String DELAYABLE_TRANSIENT = "delayable-transient";
    private static final String PROTECTIVE_ERROR = "protective-error";

    private final long slidingWindowMs;
    private final long recoveryQuietMs;
    private final long defaultDelayMs;
    private final int maxTrackedDimensions;
    private final ConcurrentHashMap<BackoffKey, BackoffState> states;
    private final Object lifecycleLock = new Object();

    /**
     * Creates a store with production retry storm thresholds and capacity.
     */
    public RetryStormBackoffStateStore() {
        this(DEFAULT_WINDOW_MS, DEFAULT_RECOVERY_QUIET_MS, DEFAULT_DELAY_MS, DEFAULT_MAX_TRACKED_DIMENSIONS);
    }

    /**
     * Creates a store with testable timing and capacity knobs.
     *
     * @param slidingWindowMs logical window used before a dimension enters delaying mode
     * @param recoveryQuietMs quiet time after the latest expected send time before a dimension returns to candidate mode
     * @param delayMs default delay returned by {@link #recordAndDecide(BackoffKey, ErrorClassSet, long)}
     * @param maxTrackedDimensions maximum number of dimensions retained before oldest-state eviction
     */
    public RetryStormBackoffStateStore(long slidingWindowMs, long recoveryQuietMs, long delayMs, int maxTrackedDimensions) {
        if (maxTrackedDimensions <= 0) {
            throw new IllegalArgumentException("maxTrackedDimensions must be positive");
        }
        this.slidingWindowMs = slidingWindowMs;
        this.recoveryQuietMs = recoveryQuietMs;
        this.defaultDelayMs = delayMs;
        this.maxTrackedDimensions = maxTrackedDimensions;
        this.states = new ConcurrentHashMap<>();
    }

    /**
     * Records an error observation using the default delay and returns the resource-level decision.
     */
    public StateDecision recordAndDecide(BackoffKey key, ErrorClassSet errorClasses, long nowMs) {
        return recordAndDecide(key, errorClasses, nowMs, defaultDelayMs);
    }

    /**
     * Records an error observation, updates candidate/delaying state, and returns whether this resource should delay.
     *
     * <p>In candidate mode the method evaluates thresholds before appending the current timestamp,
     * making the first delayable-transient failure immediate and the second one delayed. In delaying
     * mode it refreshes {@code lastFailureMs} to the expected response send time
     * {@code nowMs + decisionDelayMs}, so quiet timeout starts after the delayed response can leave.</p>
     */
    public StateDecision recordAndDecide(BackoffKey key, ErrorClassSet errorClasses, long nowMs, long decisionDelayMs) {
        while (true) {
            BackoffState state = stateFor(key, nowMs);
            synchronized (state) {
                if (states.get(key) != state) {
                    continue;
                }
                return state.recordAndDecide(errorClasses, nowMs, decisionDelayMs, slidingWindowMs, recoveryQuietMs);
            }
        }
    }

    /**
     * Performs logical-time eviction using the wall clock for external cleanup calls.
     */
    public void evictIfNeeded() {
        evictIfNeeded(System.currentTimeMillis());
    }

    int trackedDimensions() {
        return states.size();
    }

    private BackoffState stateFor(BackoffKey key, long nowMs) {
        BackoffState state = states.get(key);
        if (state != null) {
            return state;
        }
        synchronized (lifecycleLock) {
            state = states.get(key);
            if (state != null) {
                return state;
            }
            evictIfNeededLocked(nowMs);
            while (states.size() >= maxTrackedDimensions && removeOldestStateLocked()) {
                // Keep removing until the new dimension fits or no removable state remains.
            }
            BackoffState newState = new BackoffState();
            BackoffState existing = states.putIfAbsent(key, newState);
            return existing == null ? newState : existing;
        }
    }

    private void evictIfNeeded(long nowMs) {
        synchronized (lifecycleLock) {
            evictIfNeededLocked(nowMs);
        }
    }

    private void evictIfNeededLocked(long nowMs) {
        for (Iterator<Map.Entry<BackoffKey, BackoffState>> iterator = states.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<BackoffKey, BackoffState> entry = iterator.next();
            BackoffState state = entry.getValue();
            synchronized (state) {
                if (states.get(entry.getKey()) == state && state.isExpired(nowMs, recoveryQuietMs)) {
                    states.remove(entry.getKey(), state);
                }
            }
        }
        if (states.size() < maxTrackedDimensions) {
            return;
        }
        removeOldestStateLocked();
    }

    private boolean removeOldestStateLocked() {
        BackoffKey oldestKey = null;
        BackoffState oldestState = null;
        long oldestFailureMs = Long.MAX_VALUE;
        for (Map.Entry<BackoffKey, BackoffState> entry : states.entrySet()) {
            BackoffState state = entry.getValue();
            synchronized (state) {
                if (states.get(entry.getKey()) == state && state.lastFailureMs() < oldestFailureMs) {
                    oldestFailureMs = state.lastFailureMs();
                    oldestKey = entry.getKey();
                    oldestState = state;
                }
            }
        }
        if (oldestKey == null) {
            return false;
        }
        synchronized (oldestState) {
            if (states.get(oldestKey) == oldestState && oldestState.lastFailureMs() == oldestFailureMs) {
                return states.remove(oldestKey, oldestState);
            }
        }
        return true;
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

    /**
     * Owns the state machine for one retry storm dimension.
     *
     * <p>Callers must synchronize on the instance before invoking methods or reading mutable fields.
     * The state contains only per-dimension counters and does not manage map lifecycle.</p>
     */
    static class BackoffState {
        private Mode mode = Mode.CANDIDATE;
        private final ArrayDeque<Long> delayableTransientWindow = new ArrayDeque<>(DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD);
        private final ArrayDeque<Long> protectiveWindow = new ArrayDeque<>(DEFAULT_PROTECTIVE_THRESHOLD);
        private long lastFailureMs;
        private String delayReason = "";

        private StateDecision recordAndDecide(ErrorClassSet errorClasses,
                                              long nowMs,
                                              long decisionDelayMs,
                                              long slidingWindowMs,
                                              long recoveryQuietMs) {
            if (mode == Mode.DELAYING) {
                if (nowMs > lastFailureMs + recoveryQuietMs) {
                    reset();
                } else {
                    lastFailureMs = expectedSendTimeMs(nowMs, decisionDelayMs);
                    delayReason = errorClasses.reason();
                    return StateDecision.delayed(decisionDelayMs, delayReason);
                }
            }

            cleanExpired(nowMs, slidingWindowMs);

            boolean delayableReached = delayableReached(errorClasses);
            boolean protectiveReached = protectiveReached(errorClasses);

            if (delayableReached || protectiveReached) {
                enterDelaying(nowMs, decisionDelayMs, delayableReached, protectiveReached);
                return StateDecision.delayed(decisionDelayMs, delayReason);
            }

            recordCandidate(errorClasses, nowMs);
            return StateDecision.immediate();
        }

        private void cleanExpired(long nowMs, long slidingWindowMs) {
            cleanExpired(delayableTransientWindow, nowMs, slidingWindowMs);
            cleanExpired(protectiveWindow, nowMs, slidingWindowMs);
        }

        private boolean delayableReached(ErrorClassSet errorClasses) {
            return errorClasses.delayableTransient()
                && delayableTransientWindow.size() >= DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD;
        }

        private boolean protectiveReached(ErrorClassSet errorClasses) {
            return errorClasses.protective()
                && protectiveWindow.size() >= DEFAULT_PROTECTIVE_THRESHOLD;
        }

        private void enterDelaying(long nowMs,
                                   long decisionDelayMs,
                                   boolean delayableReached,
                                   boolean protectiveReached) {
            mode = Mode.DELAYING;
            lastFailureMs = expectedSendTimeMs(nowMs, decisionDelayMs);
            delayReason = joinReasons(delayableReached, protectiveReached);
        }

        private void recordCandidate(ErrorClassSet errorClasses, long nowMs) {
            if (errorClasses.delayableTransient()) {
                appendBounded(delayableTransientWindow, nowMs, DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD);
            }
            if (errorClasses.protective()) {
                appendBounded(protectiveWindow, nowMs, DEFAULT_PROTECTIVE_THRESHOLD);
            }
            lastFailureMs = nowMs;
            delayReason = errorClasses.reason();
        }

        private void appendBounded(ArrayDeque<Long> window, long nowMs, int capacity) {
            if (window.size() == capacity) {
                window.removeFirst();
            }
            window.addLast(nowMs);
        }

        private void reset() {
            mode = Mode.CANDIDATE;
            delayableTransientWindow.clear();
            protectiveWindow.clear();
            delayReason = "";
        }

        private long expectedSendTimeMs(long nowMs, long decisionDelayMs) {
            return nowMs + decisionDelayMs;
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

        private boolean isExpired(long nowMs, long recoveryQuietMs) {
            return nowMs > lastFailureMs + recoveryQuietMs;
        }

        private long lastFailureMs() {
            return lastFailureMs;
        }
    }

    /**
     * Identifies the retry storm state dimension: API, resource, and client connection scope.
     */
    public record BackoffKey(short apiKey, String resourceKey, String clientScope) {
        public BackoffKey {
            Objects.requireNonNull(resourceKey, "resourceKey");
            Objects.requireNonNull(clientScope, "clientScope");
        }
    }

    /**
     * Describes which retry storm counters a resource error should update.
     */
    public record ErrorClassSet(boolean delayableTransient, boolean protective) {
        /**
         * Returns the class set for allowlisted transient errors, which also count as protective errors.
         */
        public static ErrorClassSet delayableTransientError() {
            return new ErrorClassSet(true, true);
        }

        /**
         * Returns the class set for non-transient errors that can only reach the protective threshold.
         */
        public static ErrorClassSet protectiveOnlyError() {
            return new ErrorClassSet(false, true);
        }

        /**
         * Returns the comma-separated reason labels represented by this class set.
         */
        public String reason() {
            return joinReasons(delayableTransient, protective && !delayableTransient);
        }
    }

    /**
     * Resource-level policy result returned by the state store.
     */
    public record StateDecision(boolean delayed, long delayMs, String reason) {
        /**
         * Returns an immediate resource decision with no delay reason.
         */
        public static StateDecision immediate() {
            return new StateDecision(false, 0L, "");
        }

        /**
         * Returns a delayed resource decision when the configured delay is positive.
         */
        public static StateDecision delayed(long delayMs, String reason) {
            return new StateDecision(delayMs > 0, delayMs, reason);
        }
    }
}
