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

import kafka.automq.AutoMQConfig;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Maintains per-resource retry storm backoff state for policy evaluation.
 *
 * <p>Each key tracks independent delayable-transient and protective-error windows,
 * plus a delaying mode that keeps delaying repeated failures until quiet time expires.
 * The store owns state lifecycle through a bounded Guava cache while each
 * {@link BackoffState} owns its per-resource state machine and synchronization.</p>
 */
public class RetryStormBackoffStateStore {
    public static final int DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD = 1;
    public static final int DEFAULT_PROTECTIVE_THRESHOLD = 5;
    public static final long DEFAULT_WINDOW_MS = 1000L;
    public static final long DEFAULT_RECOVERY_QUIET_MS = 1000L;
    public static final long DEFAULT_DELAY_MS = 1000L;
    public static final int DEFAULT_MAX_TRACKED_DIMENSIONS = 100000;
    public static final int REASON_DELAYABLE_TRANSIENT = 1;
    public static final int REASON_PROTECTIVE_ERROR = 1 << 1;

    private static final String DELAYABLE_TRANSIENT = "delayable-transient";
    private static final String PROTECTIVE_ERROR = "protective-error";

    private final long slidingWindowMs;
    private final long recoveryQuietMs;
    private final long defaultDelayMs;
    private final LoadingCache<BackoffKey, BackoffState> states;

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
     * @param maxTrackedDimensions maximum number of dimensions retained by the state cache
     */
    public RetryStormBackoffStateStore(long slidingWindowMs, long recoveryQuietMs, long delayMs, int maxTrackedDimensions) {
        this(slidingWindowMs, recoveryQuietMs, delayMs, maxTrackedDimensions, AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_MAX);
    }

    RetryStormBackoffStateStore(long slidingWindowMs,
                                long recoveryQuietMs,
                                long delayMs,
                                int maxTrackedDimensions,
                                long stateRetentionDelayMs) {
        if (maxTrackedDimensions <= 0) {
            throw new IllegalArgumentException("maxTrackedDimensions must be positive");
        }
        if (recoveryQuietMs < 0 || stateRetentionDelayMs < 0) {
            throw new IllegalArgumentException("state retention timing must be non-negative");
        }
        this.slidingWindowMs = slidingWindowMs;
        this.recoveryQuietMs = recoveryQuietMs;
        this.defaultDelayMs = delayMs;
        this.states = CacheBuilder.newBuilder()
            .maximumSize(maxTrackedDimensions)
            .expireAfterAccess(recoveryQuietMs + stateRetentionDelayMs, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<>() {
                @Override
                public BackoffState load(BackoffKey key) {
                    return new BackoffState();
                }
            });
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
        BackoffState state = states.getUnchecked(key);
        synchronized (state) {
            return state.recordAndDecide(errorClasses, nowMs, decisionDelayMs, slidingWindowMs, recoveryQuietMs);
        }
    }

    /**
     * Runs cache maintenance for size and quiet-time based state eviction.
     */
    public void evictIfNeeded() {
        states.cleanUp();
    }

    int trackedDimensions() {
        return (int) states.size();
    }

    /**
     * Returns delaying state snapshots for periodic logging without updating cache access time.
     *
     * <p>The scan reads {@link LoadingCache#asMap()} instead of {@code get} or {@code getUnchecked},
     * so expire-after-access metadata is not refreshed by observability scans.</p>
     */
    public List<DelayedStateSnapshot> delayedSnapshots(long nowMs, long minDelayingAgeMs, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        ArrayList<DelayedStateSnapshot> snapshots = new ArrayList<>(Math.min(limit, 10));
        for (var entry : states.asMap().entrySet()) {
            BackoffState state = entry.getValue();
            synchronized (state) {
                DelayedStateSnapshot snapshot = state.snapshotIfDelayed(entry.getKey(), nowMs, minDelayingAgeMs);
                if (snapshot != null) {
                    snapshots.add(snapshot);
                    if (snapshots.size() >= limit) {
                        break;
                    }
                }
            }
        }
        return snapshots;
    }

    /**
     * Converts internal retry storm reason flags into the stable log-facing reason string.
     */
    public static String reasonString(int reasonMask) {
        boolean delayableReached = (reasonMask & REASON_DELAYABLE_TRANSIENT) != 0;
        boolean protectiveReached = (reasonMask & REASON_PROTECTIVE_ERROR) != 0;
        if (delayableReached && protectiveReached) {
            return DELAYABLE_TRANSIENT + "," + PROTECTIVE_ERROR;
        }
        if (delayableReached) {
            return DELAYABLE_TRANSIENT;
        }
        return protectiveReached ? PROTECTIVE_ERROR : "";
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
        private long delayingSinceMs;
        private int delayReasonMask;

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
                    delayReasonMask = errorClasses.reasonMask();
                    return StateDecision.delayed(decisionDelayMs, delayReasonMask);
                }
            }

            cleanExpired(nowMs, slidingWindowMs);

            boolean delayableReached = delayableReached(errorClasses);
            boolean protectiveReached = protectiveReached(errorClasses);

            if (delayableReached || protectiveReached) {
                enterDelaying(nowMs, decisionDelayMs, delayableReached, protectiveReached);
                return StateDecision.delayed(decisionDelayMs, delayReasonMask);
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
            delayingSinceMs = nowMs;
            lastFailureMs = expectedSendTimeMs(nowMs, decisionDelayMs);
            delayReasonMask = buildReasonMask(delayableReached, protectiveReached);
        }

        private void recordCandidate(ErrorClassSet errorClasses, long nowMs) {
            if (errorClasses.delayableTransient()) {
                appendBounded(delayableTransientWindow, nowMs, DEFAULT_DELAYABLE_TRANSIENT_THRESHOLD);
            }
            if (errorClasses.protective()) {
                appendBounded(protectiveWindow, nowMs, DEFAULT_PROTECTIVE_THRESHOLD);
            }
            lastFailureMs = nowMs;
            delayReasonMask = errorClasses.reasonMask();
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
            delayingSinceMs = 0L;
            delayReasonMask = 0;
        }

        private long expectedSendTimeMs(long nowMs, long decisionDelayMs) {
            return nowMs + decisionDelayMs;
        }

        private DelayedStateSnapshot snapshotIfDelayed(BackoffKey key, long nowMs, long minDelayingAgeMs) {
            if (mode != Mode.DELAYING || nowMs - delayingSinceMs < minDelayingAgeMs) {
                return null;
            }
            return new DelayedStateSnapshot(key, delayingSinceMs, lastFailureMs, delayReasonMask);
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
         * Returns the bit flags for the reason classes represented by this class set.
         */
        public int reasonMask() {
            return buildReasonMask(delayableTransient, protective && !delayableTransient);
        }
    }

    /**
     * Resource-level policy result returned by the state store.
     */
    public record StateDecision(boolean delayed, long delayMs, int reasonMask) {
        private static final StateDecision IMMEDIATE = new StateDecision(false, 0L, 0);

        /**
         * Returns an immediate resource decision with no delay reason.
         */
        public static StateDecision immediate() {
            return IMMEDIATE;
        }

        /**
         * Returns a delayed resource decision when the configured delay is positive.
         */
        public static StateDecision delayed(long delayMs, int reasonMask) {
            return delayMs > 0 ? new StateDecision(true, delayMs, reasonMask) : IMMEDIATE;
        }

    }

    /**
     * Read-only delaying-state snapshot used by periodic retry storm logging.
     */
    public record DelayedStateSnapshot(BackoffKey key, long delayingSinceMs, long lastFailureMs, int reasonMask) {
    }

    private static int buildReasonMask(boolean delayable, boolean protective) {
        int reasonMask = 0;
        if (delayable) {
            reasonMask |= REASON_DELAYABLE_TRANSIENT;
        }
        if (protective) {
            reasonMask |= REASON_PROTECTIVE_ERROR;
        }
        return reasonMask;
    }
}
