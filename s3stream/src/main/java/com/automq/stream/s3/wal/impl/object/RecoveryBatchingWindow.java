/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Shares a bounded recovery batching window across a broker's data and router object WALs.
 * The registry lives for the process lifetime; writers register on start and unregister on close.
 * This policy changes scheduling only, not durability, admission, or callback ordering.
 */
public final class RecoveryBatchingWindow {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryBatchingWindow.class);
    private static final ConcurrentHashMap<Integer, RecoveryBatchingWindow> WINDOWS = new ConcurrentHashMap<>();
    private static final long FAST_BATCH_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final long WINDOW_NANOS = TimeUnit.SECONDS.toNanos(2) / 3;
    private static final long COOLDOWN_NANOS = TimeUnit.SECONDS.toNanos(60);

    private final Time time;
    private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();
    private volatile long lastStartNanos;
    private volatile boolean started;
    // Guarded by this. Three non-overlapping bursts share the two-second budget.
    private long previousStartNanos;
    private boolean previousStarted;
    private long oldestStartNanos;
    private boolean oldestStarted;

    RecoveryBatchingWindow(Time time) {
        this.time = time;
    }

    /** Returns the process-local policy shared by all object WALs for the given broker ID. */
    public static RecoveryBatchingWindow forNode(int nodeId) {
        return WINDOWS.computeIfAbsent(nodeId, ignored -> new RecoveryBatchingWindow(Time.SYSTEM));
    }

    /**
     * Starts a two-thirds-second window, at most three times per rolling minute; active windows cannot be extended.
     * Separate bursts cover broker departure, proxy return and a subsequent handoff within the two-second budget.
     * Notifies registered writers outside the policy lock so an existing batch can also be shortened.
     * Returns whether this handoff opened a new window.
     */
    public boolean startRecovery() {
        synchronized (this) {
            long now = time.nanoseconds();
            if (started && now - lastStartNanos < WINDOW_NANOS) {
                return false;
            }
            if (oldestStarted && now - oldestStartNanos < COOLDOWN_NANOS) {
                return false;
            }
            if (started) {
                oldestStartNanos = previousStartNanos;
                oldestStarted = previousStarted;
                previousStartNanos = lastStartNanos;
                previousStarted = true;
            }
            lastStartNanos = now;
            started = true;
        }
        for (Runnable listener : listeners) {
            try {
                listener.run();
            } catch (RuntimeException ex) {
                LOGGER.warn("Failed to shorten an object WAL recovery batch", ex);
            }
        }
        return true;
    }

    /** Caps the configured interval at 50ms only within the window; faster settings are unchanged. */
    public long batchIntervalNanos(long normalIntervalNanos) {
        return started && time.nanoseconds() - lastStartNanos < WINDOW_NANOS
            ? recoveryBatchIntervalNanos(normalIntervalNanos) : normalIntervalNanos;
    }

    /** Caps one recovery batch at the existing 50ms interval without extending a window or slowing faster settings. */
    public static long recoveryBatchIntervalNanos(long normalIntervalNanos) {
        return Math.min(normalIntervalNanos, FAST_BATCH_NANOS);
    }

    /** Registers a writer-owned callback to shorten an already pending batch on a new window. */
    public void addListener(Runnable listener) {
        listeners.add(listener);
    }

    /** Removes the callback when its owning writer closes; no work or threads are created here. */
    public void removeListener(Runnable listener) {
        listeners.remove(listener);
    }
}
