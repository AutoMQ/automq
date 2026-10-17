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

package kafka.server;

import kafka.server.Limiter.AcquireContext;

import org.apache.kafka.common.utils.Time;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * Isolates fetch responses that drain slowly to the network from normal fetch traffic.
 *
 * <p>Fetch records retain pooled memory from the local log read until the response is completed or discarded. If a
 * connection drains responses slowly, its ready responses can hold most of the fetch memory permits and prevent
 * unrelated consumers from reading. Callers represent that lifecycle by acquiring a permit before the read, reducing
 * it to the actual response size with {@link Limiter.Permit#releaseTo(long)}, calling
 * {@link Limiter.Permit#markResponseReady()} when the records become response-ready, and closing it when the retained
 * records are released.</p>
 *
 * <p>The limiter maintains one usage counter and two admission thresholds. A soft acquisition is admitted while the
 * current usage is below {@code softThreshold}. A hard acquisition is admitted while the current usage is below
 * {@code hardThreshold}. Normal connections use hard admission; connections already classified as slow-draining use
 * soft admission. The admitted acquisition retains all requested permits, so the acquisition at a threshold may
 * oversell it. Hard waiters are served before soft waiters, and waiters with the same type are served in arrival
 * order.</p>
 *
 * <p>The limiter also owns normal and slow-draining executors. {@link #execute(String, Runnable)} routes each new task
 * using the connection classification at submission time. The fast and slow fetch paths use separate limiter
 * instances, so each path isolates only connections classified from its own retained responses.</p>
 *
 * <p>When a hard acquisition remains blocked for the slow-drain threshold, the limiter examines response-ready
 * permits that have been retained for at least the same duration. It classifies the oldest connections needed to
 * isolate ready memory above the soft allowance. Classification affects only future acquisitions; queued requests
 * retain the admission type selected when they entered the queue. A classified connection returns to normal after a
 * response completes within the threshold, or after one minute without response lifecycle activity. Stale cleanup is
 * driven by subsequent acquisitions.</p>
 *
 * <p>Each limiter instance owns its classification state; separate fast and slow fetch limiters do not share it. All
 * public methods and permit lifecycle operations are thread-safe. The strict priority policy intentionally allows
 * soft waiters to starve while hard demand remains queued.</p>
 */
public final class FetchLimiter implements Limiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchLimiter.class);
    private static final AttributeKey<String> LABEL_FETCH_LIMITER_NAME = AttributeKey.stringKey("limiter_name");
    private static final AttributeKey<String> LABEL_FETCH_EXECUTOR_NAME = AttributeKey.stringKey("executor_name");
    private static final Metrics.LongGaugeBundle FETCH_LIMITER_PERMIT_NUM = Metrics.instance()
        .longGauge("kafka_stream_fetch_limiter_permit_num", "The number of permits in fetch limiters", "");
    private static final Metrics.LongGaugeBundle FETCH_LIMITER_WAITING_TASK_NUM = Metrics.instance()
        .longGauge(
            "kafka_stream_fetch_limiter_waiting_task_num",
            "The number of tasks waiting for permits in fetch limiters",
            ""
        );
    private static final Metrics.LongGaugeBundle FETCH_PENDING_TASK_NUM = Metrics.instance()
        .longGauge("kafka_stream_fetch_pending_task_num", "The number of pending tasks in fetch executors", "");
    private static final Metrics.LongCounterBundle FETCH_LIMITER_TIMEOUT_COUNT = Metrics.instance()
        .longCounter("kafka_stream_fetch_limiter_timeout_count", "The number of acquire permits timeout", "");
    private static final Metrics.HistogramBundle FETCH_LIMITER_TIME = Metrics.instance()
        .histogram("kafka_stream_fetch_limiter_time", "The time cost of acquire permits", "nanoseconds");
    private static final int EXECUTOR_QUEUE_CAPACITY = 10000;
    static final long SLOW_DRAIN_THRESHOLD_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long SLOW_DRAIN_CLEANUP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final long SLOW_DRAIN_EXPIRATION_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final long SLOW_DRAIN_TRANSITION_LOG_DELAY_SECONDS = 1;
    private static final Comparator<Waiter> WAITER_COMPARATOR = Comparator
        .comparingInt((Waiter waiter) -> waiter.type.priority)
        .thenComparingLong(waiter -> waiter.sequence);

    // Package-private for the co-located slow-drain strategy; callers must hold lock while reading limiter state.
    final long softThreshold;
    private final long hardThreshold;
    private final String name;
    private final Time time;
    private final Executor normalExecutor;
    private final Executor slowDrainingExecutor;
    private final FetchLimiterSlowDrainStrategy slowDrainStrategy;
    private final Metrics.LongCounterBundle.LongCounter timeoutCounter;
    private final DeltaHistogram acquireTimeHistogram;
    private final boolean transitionLoggingEnabled;
    private final ReentrantLock lock = new ReentrantLock();
    private final PriorityQueue<Waiter> waiters = new PriorityQueue<>(WAITER_COMPARATOR);
    private final Set<Permit> inflightPermits = new LinkedHashSet<>();
    final Set<Permit> responseReadyPermits = new LinkedHashSet<>();
    final Map<String, Long> slowDrainingConnections = new HashMap<>();
    private final Set<String> normalToSlowDrainingTransitions = new LinkedHashSet<>();
    private final Set<String> slowDrainingToNormalTransitions = new LinkedHashSet<>();

    private long usedPermits;
    private long nextSequence;
    private long lastSlowDrainProbeNanos;
    private long lastSlowDrainCleanupNanos;
    private boolean slowDrainTransitionLogScheduled;

    /**
     * Creates a fetch limiter with soft and hard admission thresholds.
     *
     * @param softThreshold usage at which new soft acquisitions start waiting
     * @param hardThreshold usage at which new hard acquisitions start waiting
     * @param name limiter name used for metrics and executor threads
     * @param normalThreadNum number of normal fetch executor threads
     * @param slowDrainingThreadNum number of slow-draining fetch executor threads
     */
    public FetchLimiter(
        long softThreshold,
        long hardThreshold,
        String name,
        int normalThreadNum,
        int slowDrainingThreadNum
    ) {
        validateThresholds(softThreshold, hardThreshold);
        validateThreadNum(normalThreadNum, "normalThreadNum");
        validateThreadNum(slowDrainingThreadNum, "slowDrainingThreadNum");
        this.softThreshold = softThreshold;
        this.hardThreshold = hardThreshold;
        this.name = name;
        this.time = Time.SYSTEM;
        this.normalExecutor = createExecutor(name, normalThreadNum, false);
        this.slowDrainingExecutor = createExecutor(name, slowDrainingThreadNum, true);
        this.slowDrainStrategy = loadSlowDrainStrategy();
        Attributes limiterAttributes = limiterAttributes(name);
        timeoutCounter = FETCH_LIMITER_TIMEOUT_COUNT.register(MetricsLevel.INFO, limiterAttributes);
        acquireTimeHistogram = FETCH_LIMITER_TIME.histogram(MetricsLevel.INFO, limiterAttributes);
        transitionLoggingEnabled = true;
        registerGauges(limiterAttributes);
    }

    FetchLimiter(
        long softThreshold,
        long hardThreshold,
        String name,
        Time time,
        Executor normalExecutor,
        Executor slowDrainingExecutor,
        FetchLimiterSlowDrainStrategy slowDrainStrategy
    ) {
        validateThresholds(softThreshold, hardThreshold);
        if (time == null) {
            throw new IllegalArgumentException("time must not be null");
        }
        if (normalExecutor == null) {
            throw new IllegalArgumentException("normalExecutor must not be null");
        }
        if (slowDrainingExecutor == null) {
            throw new IllegalArgumentException("slowDrainingExecutor must not be null");
        }
        this.softThreshold = softThreshold;
        this.hardThreshold = hardThreshold;
        this.name = name;
        this.time = time;
        this.normalExecutor = normalExecutor;
        this.slowDrainingExecutor = slowDrainingExecutor;
        this.slowDrainStrategy = slowDrainStrategy;
        this.timeoutCounter = null;
        this.acquireTimeHistogram = null;
        this.transitionLoggingEnabled = false;
    }

    private static void validateThresholds(long softThreshold, long hardThreshold) {
        if (softThreshold <= 0) {
            throw new IllegalArgumentException("softThreshold must be positive");
        }
        if (hardThreshold < softThreshold) {
            throw new IllegalArgumentException("hardThreshold must be greater than or equal to softThreshold");
        }
    }

    private static void validateThreadNum(int threadNum, String name) {
        if (threadNum <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static FetchLimiterSlowDrainStrategy loadSlowDrainStrategy() {
        ServiceLoader<FetchLimiterSlowDrainStrategy> loader = ServiceLoader.load(FetchLimiterSlowDrainStrategy.class);
        Iterator<FetchLimiterSlowDrainStrategy> iterator = loader.iterator();
        if (!iterator.hasNext()) {
            LOGGER.debug("No fetch limiter slow-drain strategy loaded; probing is disabled");
            return null;
        }
        FetchLimiterSlowDrainStrategy strategy = iterator.next();
        if (iterator.hasNext()) {
            throw new IllegalStateException("Only one FetchLimiterSlowDrainStrategy is supported");
        }
        LOGGER.info("Loaded fetch limiter slow-drain strategy: {}", strategy.getClass().getName());
        return strategy;
    }

    private static Executor createExecutor(String name, int threadNum, boolean slowDraining) {
        String executorName = slowDraining ? name + "-slow-draining" : name;
        return Threads.newFixedThreadPool(
            threadNum,
            "kafka-apis-" + executorName + "-fetch-executor",
            true,
            EXECUTOR_QUEUE_CAPACITY,
            LOGGER
        );
    }

    private void registerGauges(Attributes limiterAttributes) {
        FETCH_LIMITER_PERMIT_NUM.register(
            MetricsLevel.INFO,
            limiterAttributes,
            measurement -> measurement.record(availablePermits())
        );
        FETCH_LIMITER_WAITING_TASK_NUM.register(
            MetricsLevel.INFO,
            limiterAttributes,
            measurement -> measurement.record(waitingThreads())
        );
        FETCH_PENDING_TASK_NUM.register(
            MetricsLevel.INFO,
            executorAttributes(name),
            measurement -> measurement.record(executorQueueSize(normalExecutor))
        );
        FETCH_PENDING_TASK_NUM.register(
            MetricsLevel.INFO,
            executorAttributes(name + "_slow_draining"),
            measurement -> measurement.record(executorQueueSize(slowDrainingExecutor))
        );
    }

    private static long executorQueueSize(Executor executor) {
        return executor instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor) executor).getQueue().size() : 0;
    }

    private static Attributes executorAttributes(String executorName) {
        return Attributes.of(LABEL_FETCH_EXECUTOR_NAME, executorName);
    }

    private static Attributes limiterAttributes(String limiterName) {
        return Attributes.of(LABEL_FETCH_LIMITER_NAME, limiterName);
    }

    /**
     * Acquires permits, waiting until this request is admitted.
     *
     * @param permits requested permits
     * @param context acquisition timeout and owning connection
     * @return a permit handle that must be closed after use, or {@code null} when a positive timeout expires
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public Limiter.Permit acquire(long permits, AcquireContext context) throws InterruptedException {
        validateAcquire(permits, context);
        TimerUtil timer = new TimerUtil();

        lock.lockInterruptibly();
        Permit permit;
        try {
            cleanupSlowDrainingConnections(time.nanoseconds());
            Waiter waiter = enqueue(permits, context.connectionId());
            permit = awaitPermit(waiter, context.timeoutMs());
        } finally {
            lock.unlock();
        }
        if (acquireTimeHistogram != null) {
            acquireTimeHistogram.record(timer.elapsedAs(TimeUnit.NANOSECONDS));
        }
        if (permit == null && timeoutCounter != null) {
            timeoutCounter.add(1);
        }
        return permit;
    }

    @Override
    public void execute(String connectionId, Runnable task) {
        Executor executor;
        lock.lock();
        try {
            executor = slowDrainingConnections.containsKey(connectionId) ? slowDrainingExecutor : normalExecutor;
        } finally {
            lock.unlock();
        }
        executor.execute(task);
    }

    private void validateAcquire(long permits, AcquireContext context) {
        if (permits < 0) {
            throw new IllegalArgumentException("permits must not be negative");
        }
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
    }

    private Waiter enqueue(long permits, String connectionId) {
        Type type = slowDrainingConnections.containsKey(connectionId) ? Type.SOFT : Type.HARD;
        Waiter waiter = new Waiter(type, permits, nextSequence++, connectionId, time.nanoseconds(), lock.newCondition());
        waiters.add(waiter);
        grantWaiters();
        return waiter;
    }

    private Permit awaitPermit(Waiter waiter, long timeoutMs) throws InterruptedException {
        try {
            if (!waiter.await(timeoutMs)) {
                timeout(waiter);
                return null;
            }
            return waiter.permit;
        } catch (InterruptedException e) {
            cancel(waiter);
            throw e;
        }
    }

    private void timeout(Waiter waiter) {
        waiters.remove(waiter);
        grantWaiters();
    }

    long usedPermits() {
        lock.lock();
        try {
            return usedPermits;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the capacity available before the hard threshold is reached.
     */
    long availablePermits() {
        lock.lock();
        try {
            return Math.max(0, hardThreshold - usedPermits);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of queued acquisitions.
     */
    int waitingThreads() {
        lock.lock();
        try {
            return waiters.size();
        } finally {
            lock.unlock();
        }
    }

    boolean isSlowDraining(String connectionId) {
        lock.lock();
        try {
            return slowDrainingConnections.containsKey(connectionId);
        } finally {
            lock.unlock();
        }
    }

    private long threshold(Type type) {
        return type == Type.HARD ? hardThreshold : softThreshold;
    }

    private void grantWaiters() {
        while (!waiters.isEmpty()) {
            Waiter waiter = waiters.peek();
            long nowNanos = time.nanoseconds();
            if (waiter.type == Type.HARD
                && nowNanos - waiter.startNanos >= SLOW_DRAIN_THRESHOLD_NANOS) {
                probeSlowDrainingConnections(nowNanos);
            }
            if (waiter.permits != 0 && usedPermits >= threshold(waiter.type)) {
                return;
            }
            waiters.poll();
            usedPermits += waiter.permits;
            waiter.permit = new Permit(this, waiter.permits, waiter.connectionId);
            if (waiter.permits > 0) {
                inflightPermits.add(waiter.permit);
            }
            waiter.granted = true;
            waiter.condition.signal();
        }
    }

    private void cancel(Waiter waiter) {
        if (waiter.granted) {
            releasePermit(waiter.permit, waiter.permits);
            waiter.granted = false;
        } else {
            waiters.remove(waiter);
        }
        grantWaiters();
    }

    private void probeSlowDrainingConnections(long nowNanos) {
        if (slowDrainStrategy == null) {
            return;
        }
        if (lastSlowDrainProbeNanos != 0
            && nowNanos - lastSlowDrainProbeNanos < SLOW_DRAIN_THRESHOLD_NANOS) {
            return;
        }
        lastSlowDrainProbeNanos = nowNanos;

        for (String connectionId : slowDrainStrategy.select(this, nowNanos)) {
            if (connectionId != null && !slowDrainingConnections.containsKey(connectionId)) {
                transitionToSlowDraining(connectionId, nowNanos);
            }
        }
    }

    private void cleanupSlowDrainingConnections(long nowNanos) {
        if (lastSlowDrainCleanupNanos != 0
            && nowNanos - lastSlowDrainCleanupNanos < SLOW_DRAIN_CLEANUP_INTERVAL_NANOS) {
            return;
        }
        lastSlowDrainCleanupNanos = nowNanos;
        Iterator<Map.Entry<String, Long>> iterator = slowDrainingConnections.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (nowNanos - entry.getValue() >= SLOW_DRAIN_EXPIRATION_NANOS) {
                iterator.remove();
                recordSlowDrainingToNormalTransition(entry.getKey());
            }
        }
    }

    private void releasePermit(Permit permit, long permits) {
        if (permits == 0) {
            return;
        }
        usedPermits -= permits;
        permit.permitsHeld -= permits;
        if (permit.permitsHeld == 0) {
            inflightPermits.remove(permit);
            responseReadyPermits.remove(permit);
            responseCompleted(permit, time.nanoseconds());
        }
    }

    private void markResponseReady(Permit permit) {
        if (permit.permitsHeld > 0 && inflightPermits.contains(permit) && responseReadyPermits.add(permit)) {
            long nowNanos = time.nanoseconds();
            permit.responseReadyNanos = nowNanos;
            if (permit.connectionId != null && slowDrainingConnections.containsKey(permit.connectionId)) {
                slowDrainingConnections.put(permit.connectionId, nowNanos);
            }
        }
    }

    private void responseCompleted(Permit permit, long nowNanos) {
        if (permit.responseReadyNanos == 0 || permit.connectionId == null
            || !slowDrainingConnections.containsKey(permit.connectionId)) {
            return;
        }
        if (nowNanos - permit.responseReadyNanos < SLOW_DRAIN_THRESHOLD_NANOS) {
            transitionToNormal(permit.connectionId);
        } else {
            slowDrainingConnections.put(permit.connectionId, nowNanos);
        }
    }

    private void transitionToSlowDraining(String connectionId, long nowNanos) {
        if (slowDrainingConnections.put(connectionId, nowNanos) == null) {
            recordNormalToSlowDrainingTransition(connectionId);
        }
    }

    private void transitionToNormal(String connectionId) {
        if (slowDrainingConnections.remove(connectionId) != null) {
            recordSlowDrainingToNormalTransition(connectionId);
        }
    }

    private void recordNormalToSlowDrainingTransition(String connectionId) {
        if (transitionLoggingEnabled) {
            normalToSlowDrainingTransitions.add(connectionId);
            scheduleSlowDrainTransitionLog();
        }
    }

    private void recordSlowDrainingToNormalTransition(String connectionId) {
        if (transitionLoggingEnabled) {
            slowDrainingToNormalTransitions.add(connectionId);
            scheduleSlowDrainTransitionLog();
        }
    }

    private void scheduleSlowDrainTransitionLog() {
        if (slowDrainTransitionLogScheduled) {
            return;
        }
        slowDrainTransitionLogScheduled = true;
        Threads.COMMON_SCHEDULER.schedule(
            this::flushSlowDrainTransitions,
            SLOW_DRAIN_TRANSITION_LOG_DELAY_SECONDS,
            TimeUnit.SECONDS
        );
    }

    private void flushSlowDrainTransitions() {
        List<String> normalToSlowDraining;
        List<String> slowDrainingToNormal;
        lock.lock();
        try {
            normalToSlowDraining = List.copyOf(normalToSlowDrainingTransitions);
            slowDrainingToNormal = List.copyOf(slowDrainingToNormalTransitions);
            normalToSlowDrainingTransitions.clear();
            slowDrainingToNormalTransitions.clear();
            slowDrainTransitionLogScheduled = false;
        } finally {
            lock.unlock();
        }
        if (!normalToSlowDraining.isEmpty() || !slowDrainingToNormal.isEmpty()) {
            LOGGER.info(
                "Fetch limiter {} connection state transitions: normalToSlowDraining(count={}, connectionIds={}), "
                    + "slowDrainingToNormal(count={}, connectionIds={})",
                name,
                normalToSlowDraining.size(),
                normalToSlowDraining,
                slowDrainingToNormal.size(),
                slowDrainingToNormal
            );
        }
    }

    /**
     * Selects the admission threshold and queue priority of an acquisition.
     */
    private enum Type {
        HARD(0),
        SOFT(1);

        private final int priority;

        Type(int priority) {
            this.priority = priority;
        }
    }

    static final class Permit implements Limiter.Permit {
        private final FetchLimiter limiter;
        long permitsHeld;
        final String connectionId;
        long responseReadyNanos;

        private Permit(FetchLimiter limiter, long permitsHeld, String connectionId) {
            this.limiter = limiter;
            this.permitsHeld = permitsHeld;
            this.connectionId = connectionId;
        }

        /**
         * Returns the permits still held by this handle.
         */
        public long permitsHeld() {
            limiter.lock.lock();
            try {
                return permitsHeld;
            } finally {
                limiter.lock.unlock();
            }
        }

        /**
         * Marks the retained records as response-ready so their drain duration participates in slow-connection
         * classification. Repeated calls have no effect.
         */
        public void markResponseReady() {
            limiter.lock.lock();
            try {
                limiter.markResponseReady(this);
            } finally {
                limiter.lock.unlock();
            }
        }

        /**
         * Releases permits until this handle owns the supplied amount.
         *
         * @param newPermits permits that should remain held
         * @return {@code true} if permits were released, or {@code false} if the value is outside the valid range
         */
        public boolean releaseTo(long newPermits) {
            limiter.lock.lock();
            try {
                if (newPermits < 0 || newPermits > permitsHeld) {
                    return false;
                }
                releaseLocked(permitsHeld - newPermits);
                return true;
            } finally {
                limiter.lock.unlock();
            }
        }

        /**
         * Releases all remaining permits and completes the tracked response lifecycle. Repeated calls have no effect.
         */
        @Override
        public void close() {
            limiter.lock.lock();
            try {
                releaseLocked(permitsHeld);
            } finally {
                limiter.lock.unlock();
            }
        }

        private void releaseLocked(long permits) {
            limiter.releasePermit(this, permits);
            limiter.grantWaiters();
        }
    }

    private static final class Waiter {
        private final Type type;
        private final long permits;
        private final long sequence;
        private final String connectionId;
        private final long startNanos;
        private final Condition condition;
        private boolean granted;
        private Permit permit;

        private Waiter(
            Type type,
            long permits,
            long sequence,
            String connectionId,
            long startNanos,
            Condition condition
        ) {
            this.type = type;
            this.permits = permits;
            this.sequence = sequence;
            this.connectionId = connectionId;
            this.startNanos = startNanos;
            this.condition = condition;
        }

        private boolean await(long timeoutMs) throws InterruptedException {
            long timeoutRemainingNanos = timeoutMs > 0
                ? TimeUnit.MILLISECONDS.toNanos(timeoutMs)
                : Long.MAX_VALUE;

            while (!granted) {
                if (timeoutRemainingNanos <= 0) {
                    return false;
                }
                if (timeoutRemainingNanos == Long.MAX_VALUE) {
                    condition.await();
                } else {
                    timeoutRemainingNanos = Math.max(0, condition.awaitNanos(timeoutRemainingNanos));
                }
            }
            return true;
        }
    }
}
