/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Partition-owned barrier between accepted appends and delayed-Produce registration.
 * Admission is stopped under the partition ISR write lock; waiting happens without that
 * lock. This barrier does not persist records or decide acknowledgement eligibility.
 */
public final class AcceptedProduceDrain {
    private int registrations;
    private boolean stopped;

    /** Reserves registration before appending; returns false once draining has stopped admission. */
    public synchronized boolean begin() {
        if (stopped) {
            return false;
        }
        registrations++;
        return true;
    }

    /** Releases a reservation after registration, or after a failed append/registration attempt. */
    public synchronized void finish() {
        if (registrations <= 0) {
            throw new IllegalStateException("Unbalanced Produce registration reservation");
        }
        if (--registrations == 0) {
            notifyAll();
        }
    }

    /** Permanently stops new reservations for this partition incarnation; repeated calls are harmless. */
    public synchronized void stop() {
        stopped = true;
    }

    /**
     * Waits for previously reserved registrations without changing their outcomes. A bounded
     * timeout or interruption lets the caller retain its normal unhost/close failure path.
     */
    public synchronized void await(long timeoutNanos) throws InterruptedException, TimeoutException {
        long start = System.nanoTime();
        while (registrations != 0) {
            long remaining = timeoutNanos - (System.nanoTime() - start);
            if (remaining <= 0) {
                throw new TimeoutException("Accepted Produce registration did not drain");
            }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
    }
}
