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

package com.automq.stream.s3.network;

import com.automq.stream.utils.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Shares a network byte budget between traffic groups, combining fixed-window rate limits with
 * weighted deficit round-robin (DRR) scheduling.
 *
 * <p>Each window provides a fresh baseline allowance. Unused baseline may be saved up to a bounded
 * capacity and spent in later windows, allowing temporary bursts. A separate peak allowance limits
 * the total spent within any one window. Thus the spendable budget is
 * {@code min(peakRemaining, baselineRemaining + savedTokens)}. Spending uses baseline first, then
 * savings, and always reduces the peak allowance. With zero saving capacity, only baseline is available.
 *
 * <p>The scheduler shares this budget by bytes: each group receives a quantum proportional to its
 * weight and serves its requests in FIFO order. When a group's quantum is exhausted, scheduling moves
 * to the next group. When the window budget is exhausted, scheduling pauses and resumes from the same
 * position after refill. A request completes once all its bytes have been allocated, possibly across
 * multiple windows. BYPASS succeeds immediately; its bounded debt is repaid before ordinary requests.
 *
 * <p>This limiter grants permission to transfer bytes; callers must submit small chunks to smooth
 * actual network I/O. Returned futures are owned by the limiter and must not be completed or cancelled
 * by callers. Completion callbacks run on a separate executor. Call {@link #shutdown()} to release
 * queued requests and owned executors.
 */
public final class SmoothNetworkBandwidthLimiter implements NetworkBandwidthLimiter {
    private static final int MAX_BYPASS_DEBT_WINDOWS = 10;

    private final ReentrantLock lock = new ReentrantLock();
    private final Configuration configuration;
    private final Map<ThrottleStrategy, Group> groups = new EnumMap<>(ThrottleStrategy.class);
    private final Group[] schedulingGroups;
    private int nextGroup;
    private final AtomicBoolean workPending = new AtomicBoolean();
    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicLong pendingBypass = new AtomicLong();
    private long bypassDebt;
    private final long maxBypassDebt;
    private final Time time;
    private final Executor completions;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService ownedCompletions;
    private long windowStart;
    // Unspent baseline allowance for the current window; unused bytes may become savings at rollover.
    private long baselineRemaining;
    // Remaining total spending ceiling for the current window, including any spending from savings.
    private long peakRemaining;
    // Baseline bytes saved across windows, bounded by capacity; spending still requires peak allowance.
    private long savedTokens;
    private volatile boolean closed;

    /**
     * Creates a limiter with an empty burst pool and one baseline window available immediately.
     * Ordinary weights are explicit; BYPASS is always available and does not take a DRR weight.
     */
    @SuppressWarnings("this-escape")
    public SmoothNetworkBandwidthLimiter(Configuration configuration) {
        this(configuration, Time.SYSTEM,
            Executors.newSingleThreadExecutor(new DefaultThreadFactory("smooth-bandwidth-completions", true)),
            Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("smooth-bandwidth-refill", true)), true);
        scheduler.scheduleAtFixedRate(this::tick, configuration.windowNanos(), configuration.windowNanos(), TimeUnit.NANOSECONDS);
    }

    // A manual clock and tick keep window and scheduling tests deterministic without wall-clock sleeps.
    SmoothNetworkBandwidthLimiter(Configuration configuration, Time time, Executor completions) {
        this(configuration, time, completions, null, false);
    }

    private SmoothNetworkBandwidthLimiter(Configuration configuration, Time time, Executor completions,
        ScheduledExecutorService scheduler, boolean ownsCompletionExecutor) {
        this.configuration = Objects.requireNonNull(configuration);
        this.maxBypassDebt = saturatedMultiply(configuration.baselineTokens(), MAX_BYPASS_DEBT_WINDOWS);
        this.time = Objects.requireNonNull(time);
        this.completions = Objects.requireNonNull(completions);
        this.scheduler = scheduler;
        this.ownedCompletions = ownsCompletionExecutor ? (ExecutorService) completions : null;
        configuration.weights().forEach((strategy, weight) ->
            groups.put(strategy, new Group(Math.multiplyExact(configuration.baseQuantum(), weight))));
        schedulingGroups = groups.values().toArray(Group[]::new);
        windowStart = time.nanoseconds();
        baselineRemaining = configuration.baselineTokens();
        peakRemaining = configuration.peakTokens();
    }

    /**
     * Enqueues ordinary demand without waiting for the scheduling lock. The future completes when
     * all bytes have been allocated. BYPASS records usage and succeeds immediately, even in debt.
     * Zero bytes and valid requests after shutdown succeed immediately. Negative sizes, null strategies
     * and unconfigured ordinary strategies fail without consuming tokens, including after shutdown.
     */
    @Override
    public CompletableFuture<Void> consume(ThrottleStrategy strategy, long bytes) {
        if (bytes < 0 || strategy == null || (strategy != ThrottleStrategy.BYPASS && !groups.containsKey(strategy))) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid size or unconfigured strategy"));
        }
        if (closed || bytes == 0) {
            return CompletableFuture.completedFuture(null);
        }
        if (strategy == ThrottleStrategy.BYPASS) {
            recordBypass(bytes);
            return CompletableFuture.completedFuture(null);
        }
        Group group = groups.get(strategy);
        Request request = new Request(bytes);
        queueSize.incrementAndGet();
        group.requests.add(request);
        // A producer may pass the first closed check before shutdown drains the queues.
        if (closed) {
            if (group.requests.remove(request)) {
                queueSize.decrementAndGet();
                request.future.complete(null);
            }
        } else {
            tick();
        }
        return request.future;
    }

    // Retain at most ten baseline windows of usage; excess BYPASS traffic is not repaid.
    private void recordBypass(long bytes) {
        pendingBypass.getAndUpdate(pending -> pending + Math.min(maxBypassDebt - pending, bytes));
        workPending.set(true);
        if (closed) {
            pendingBypass.set(0);
        }
    }

    /**
     * Waits for a grant. Interruption stops waiting but does not cancel the limiter-owned request;
     * its allocation continues until completion or shutdown.
     */
    @Override
    public void consumeBlocking(ThrottleStrategy strategy, long bytes) throws InterruptedException, ExecutionException {
        consume(strategy, bytes).get();
    }

    void tick() {
        workPending.set(true);
        if (!lock.tryLock()) {
            return;
        }
        try {
            // Bound lock ownership under continuously arriving work. Refill handles any remaining flag.
            for (int pass = 0; pass < 2 && !closed; pass++) {
                workPending.set(false);
                advanceWindow();
                // Account for BYPASS once per pass; concurrent usage is handled by a later pass or tick.
                repayBypass();
                List<Request> granted = new ArrayList<>();
                scheduleRequests(granted);
                dispatch(granted);
                if (!workPending.get()) {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void repayBypass() {
        if (bypassDebt == 0 && pendingBypass.get() == 0) {
            return;
        }
        // Newly observed usage is charged to the current window, never to expired idle budgets.
        bypassDebt += Math.min(maxBypassDebt - bypassDebt, pendingBypass.getAndSet(0));
        long paid = Math.min(bypassDebt, available());
        allocate(paid);
        bypassDebt -= paid;
    }

    // Advance accounting to the current window without replaying expired sending budgets.
    // Pending BYPASS usage is intentionally left for repayBypass(): only already observed debt
    // can be repaid with past windows; newly observed usage is charged to the current window.
    private void advanceWindow() {
        long elapsed = time.nanoseconds() - windowStart;
        long windows = elapsed / configuration.windowNanos();
        if (windows <= 0) {
            return;
        }
        // Close the previously tracked window: unused baseline becomes burst savings, up to capacity.
        savedTokens += Math.min(configuration.capacity() - savedTokens, baselineRemaining);

        // Exclude the current window, whose budget is initialized below. For example, moving from
        // window 0 to window 3 skips windows 1 and 2, so missing = 2. A normal one-window tick skips none.
        long missing = windows - 1;
        // Each skipped window contributes baseline credit. Together with existing savings, this
        // can repay known debt, but repayment cannot exceed the skipped windows' total peak budget.
        long pool = saturatedAdd(saturatedMultiply(missing, configuration.baselineTokens()), savedTokens);
        long paid = Math.min(bypassDebt, Math.min(pool, saturatedMultiply(missing, configuration.peakTokens())));
        bypassDebt -= paid;
        // Only leftover credit may survive as bounded savings; it is not an accumulated send budget.
        savedTokens = Math.min(pool - paid, configuration.capacity());

        // Keep the original window boundaries (do not reset the start to now). Give the current
        // window exactly one baseline and peak budget, regardless of how many windows elapsed.
        windowStart += windows * configuration.windowNanos();
        baselineRemaining = configuration.baselineTokens();
        peakRemaining = configuration.peakTokens();
    }

    // All operands are nonnegative. Extreme accounting values saturate rather than wrapping negative.
    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long saturatedMultiply(long left, long right) {
        return right != 0 && left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private void scheduleRequests(List<Request> granted) {
        int emptyGroups = 0;
        while (available() > 0 && emptyGroups < schedulingGroups.length) {
            Group group = schedulingGroups[nextGroup];
            Request request = group.requests.peek();
            if (request == null) {
                group.deficit = 0;
                emptyGroups++;
            } else {
                emptyGroups = 0;
                if (group.deficit == 0) {
                    group.deficit = group.quantum;
                }
                long allocated = Math.min(available(), Math.min(group.deficit, request.remainingBytes));
                allocate(allocated);
                group.deficit -= allocated;
                request.remainingBytes -= allocated;
                if (request.remainingBytes == 0) {
                    group.requests.remove();
                    queueSize.decrementAndGet();
                    granted.add(request);
                    if (group.requests.isEmpty()) {
                        group.deficit = 0;
                    }
                }
            }
            if (group.deficit == 0) {
                nextGroup = (nextGroup + 1) % schedulingGroups.length;
            }
        }
    }

    private void allocate(long bytes) {
        long fromBaseline = Math.min(baselineRemaining, bytes);
        baselineRemaining -= fromBaseline;
        savedTokens -= bytes - fromBaseline;
        peakRemaining -= bytes;
    }

    // Submission is serialized with shutdown, so accepted grants cannot lose their completion task.
    private void dispatch(List<Request> granted) {
        if (!granted.isEmpty()) {
            completions.execute(() -> granted.forEach(request -> request.future.complete(null)));
        }
    }

    private long available() {
        return Math.min(peakRemaining, baselineRemaining + savedTokens);
    }

    /** Returns the maximum bytes that can be available in one window, including a full burst pool. */
    @Override
    public long getMaxTokens() {
        return Math.min(configuration.peakTokens(), configuration.baselineTokens() + configuration.capacity());
    }

    /** Returns currently spendable bytes; queued requests may already have consumed the window budget. */
    @Override
    public long getAvailableTokens() {
        lock.lock();
        try {
            if (closed) {
                return 0;
            }
            long debt = bypassDebt + Math.min(maxBypassDebt - bypassDebt, pendingBypass.get());
            return Math.max(0, available() - debt);
        } finally {
            lock.unlock();
        }
    }

    /** Returns the number of requests still waiting for a grant, excluding granted notifications. */
    @Override
    public int getQueueSize() {
        return queueSize.get();
    }

    /**
     * Disables throttling, completes queued requests successfully, and stops owned executors after
     * completion notifications drain. Future valid requests succeed immediately; repeated shutdown
     * calls have no effect.
     */
    @Override
    public void shutdown() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            List<Request> pending = new ArrayList<>();
            for (Group group : groups.values()) {
                Request request;
                while ((request = group.requests.poll()) != null) {
                    queueSize.decrementAndGet();
                    pending.add(request);
                }
            }
            pendingBypass.set(0);
            bypassDebt = 0;
            dispatch(pending);
            if (scheduler != null) {
                scheduler.shutdown();
            }
            if (ownedCompletions != null) {
                ownedCompletions.shutdown();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Fixed-window configuration. Bandwidths are bytes/second, quantum is bytes, and maxBurstWindows
     * counts windows. Rates are rounded down to whole bytes per window; each must yield at least one
     * byte. Capacity uses those effective window budgets. All configuration arithmetic must fit in
     * a signed long. Weights apply only to ordinary groups; BYPASS must not appear in the map.
     * Choose quantum near the transfer chunk size; scheduling visits each group rather than batching rounds.
     */
    public record Configuration(long baselineBandwidth, long burstBandwidth, Duration window,
                                long maxBurstWindows, long baseQuantum, Map<ThrottleStrategy, Integer> weights) {
        /** Validates budgets and defensively copies positive weights; no production weights are implicit. */
        public Configuration {
            Objects.requireNonNull(window);
            weights = Map.copyOf(weights);
            if (baselineBandwidth <= 0 || burstBandwidth < baselineBandwidth || window.isNegative() || window.isZero()) {
                throw new IllegalArgumentException("Invalid bandwidth or window");
            }
            if (maxBurstWindows < 0 || baseQuantum <= 0 || weights.isEmpty() || weights.containsKey(ThrottleStrategy.BYPASS)) {
                throw new IllegalArgumentException("Invalid burst duration, quantum or weights");
            }
            long nanos = window.toNanos();
            long baseline = Math.multiplyExact(baselineBandwidth, nanos) / TimeUnit.SECONDS.toNanos(1);
            long peak = Math.multiplyExact(burstBandwidth, nanos) / TimeUnit.SECONDS.toNanos(1);
            if (baseline == 0) {
                throw new IllegalArgumentException("Baseline must provide at least one byte per window");
            }
            long capacity = Math.multiplyExact(peak - baseline, maxBurstWindows);
            Math.addExact(baseline, capacity);
            for (int weight : weights.values()) {
                if (weight <= 0) {
                    throw new IllegalArgumentException("Weights must be positive");
                }
                Math.multiplyExact(baseQuantum, weight);
            }
        }

        long windowNanos() {
            return window.toNanos();
        }

        long baselineTokens() {
            return baselineBandwidth * windowNanos() / TimeUnit.SECONDS.toNanos(1);
        }

        long peakTokens() {
            return burstBandwidth * windowNanos() / TimeUnit.SECONDS.toNanos(1);
        }

        long capacity() {
            return (peakTokens() - baselineTokens()) * maxBurstWindows;
        }
    }

    private static final class Group {
        private final long quantum;
        private final ConcurrentLinkedQueue<Request> requests = new ConcurrentLinkedQueue<>();
        private long deficit;

        private Group(long quantum) {
            this.quantum = quantum;
        }
    }

    private static final class Request {
        private long remainingBytes;
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        private Request(long bytes) {
            this.remainingBytes = bytes;
        }
    }
}
