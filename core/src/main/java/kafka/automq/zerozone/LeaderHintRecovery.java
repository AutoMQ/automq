/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Briefly waits for published leader hints after a completed ZeroZone Produce failure.
 * This broker-owned helper never changes persistence or acknowledgements. It owns no threads:
 * at most 64 responses use a shared scheduler, each for at most one second. Expiry, overload and
 * scheduling failures fall back to sending the original error without a usable hint.
 */
public final class LeaderHintRecovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderHintRecovery.class);
    /** Failure-only routing grace; shared with shutdown so sockets remain available for the admitted replies. */
    public static final long MAX_WAIT_MILLIS = 1000;
    private static final long MAX_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(MAX_WAIT_MILLIS);
    private static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private final ScheduledExecutorService scheduler;
    private final Time time;
    private final Semaphore slots = new Semaphore(64);
    private final AtomicBoolean failureLogged = new AtomicBoolean();
    private final Set<CompletableFuture<Void>> pending = ConcurrentHashMap.newKeySet();

    /** Uses a broker's shared scheduler and monotonic clock; no executor lifecycle is transferred. */
    public LeaderHintRecovery(ScheduledExecutorService scheduler, Time time) {
        this.scheduler = scheduler;
        this.time = time;
    }

    /**
     * Completes once the hint supplier becomes true or the smaller of the request budget and
     * one second expires. An already usable hint and an exhausted budget require no scheduled work.
     * Cancellation releases its admission slot and a pending poll will observe completion.
     */
    public CompletableFuture<Void> await(BooleanSupplier ready, long requestBudgetNanos) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        return await(ready, requestBudgetNanos, result, result);
    }

    /**
     * Also tracks a socket-send completion for graceful shutdown. Admission is retained until
     * the send completes or the same bounded deadline expires; abandoned connections cannot
     * retain slots indefinitely. The supplied completion carries no successful-append semantics.
     */
    public CompletableFuture<Void> await(BooleanSupplier ready, long requestBudgetNanos,
        CompletableFuture<Void> responseSent) {
        return await(ready, requestBudgetNanos, new CompletableFuture<>(), responseSent);
    }

    private CompletableFuture<Void> await(BooleanSupplier ready, long requestBudgetNanos,
        CompletableFuture<Void> result, CompletableFuture<Void> responseSent) {
        long budget = Math.min(requestBudgetNanos, MAX_WAIT_NANOS);
        try {
            if (budget <= 0 || ready.getAsBoolean() || !slots.tryAcquire()) {
                result.complete(null);
                return result;
            }
        } catch (RuntimeException ex) {
            logFallback(ex);
            result.complete(null);
            return result;
        }
        pending.add(responseSent);
        responseSent.whenComplete((ignored, error) -> {
            pending.remove(responseSent);
            slots.release();
        });
        new Poll(ready, result, responseSent, time.nanoseconds(), budget).schedule(POLL_NANOS);
        return result;
    }

    /**
     * Returns a completion snapshot for admitted response waits. Controlled shutdown may briefly
     * await it before closing sockets and metadata publishers. Later requests do not extend this
     * snapshot; callers must enforce their own shutdown timeout and must not wait on a request thread.
     */
    public CompletableFuture<Void> pendingResponses() {
        return CompletableFuture.allOf(pending.toArray(new CompletableFuture<?>[0]));
    }

    private void logFallback(RuntimeException error) {
        if (failureLogged.compareAndSet(false, true)) {
            LOGGER.warn("Recovery leader hint wait unavailable; sending original Produce error", error);
        }
    }

    private final class Poll implements Runnable {
        private final BooleanSupplier ready;
        private final CompletableFuture<Void> result;
        private final CompletableFuture<Void> responseSent;
        private final long start;
        private final long budget;

        Poll(BooleanSupplier ready, CompletableFuture<Void> result, CompletableFuture<Void> responseSent,
            long start, long budget) {
            this.ready = ready;
            this.result = result;
            this.responseSent = responseSent;
            this.start = start;
            this.budget = budget;
        }

        void schedule(long delay) {
            try {
                scheduler.schedule(this, Math.min(delay, budget), TimeUnit.NANOSECONDS);
            } catch (RuntimeException ex) {
                logFallback(ex);
                result.complete(null);
                responseSent.complete(null);
            }
        }

        @Override
        public void run() {
            if (responseSent.isDone()) {
                result.complete(null);
                return;
            }
            try {
                long remaining = budget - (time.nanoseconds() - start);
                if (remaining <= 0) {
                    result.complete(null);
                    responseSent.complete(null);
                } else if (!result.isDone() && ready.getAsBoolean()) {
                    result.complete(null);
                }
                if (!responseSent.isDone()) {
                    schedule(Math.min(POLL_NANOS, remaining));
                }
            } catch (RuntimeException ex) {
                logFallback(ex);
                result.complete(null);
                responseSent.complete(null);
            }
        }
    }
}
