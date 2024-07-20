/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.network;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.utils.LogContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncNetworkBandwidthLimiter implements NetworkBandwidthLimiter {
    private static final Logger LOGGER = new LogContext().logger(AsyncNetworkBandwidthLimiter.class);
    private static final float DEFAULT_EXTRA_TOKEN_RATIO = 0.1f;
    private static final long MAX_TOKEN_PART_SIZE = 1024 * 1024;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long maxTokens;
    private final ScheduledExecutorService refillThreadPool;
    private final ExecutorService callbackThreadPool;
    private final Queue<BucketItem> queuedCallbacks;
    private final Type type;
    private final long tokenSize;
    private final long extraTokenSize;
    private final long refillIntervalMs;
    private long availableTokens;
    private long availableExtraTokens;
    private boolean tokenExhausted = false;
    private long tokenExhaustedDuration = 0;

    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs) {
        this(type, tokenSize, refillIntervalMs, tokenSize);
    }

    @SuppressWarnings("this-escape")
    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs, long maxTokens) {
        this.type = type;
        long tokenPerSec = tokenSize * 1000 / (long) refillIntervalMs;
        this.extraTokenSize = (long) (tokenPerSec * DEFAULT_EXTRA_TOKEN_RATIO);
        this.tokenSize = (long) (tokenSize * (1 - DEFAULT_EXTRA_TOKEN_RATIO));
        this.availableTokens = this.tokenSize;
        this.maxTokens = maxTokens;
        this.refillIntervalMs = refillIntervalMs;
        this.queuedCallbacks = new PriorityQueue<>();
        this.refillThreadPool = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("refill-bucket-thread"));
        this.callbackThreadPool = Executors.newFixedThreadPool(1, new DefaultThreadFactory("callback-thread"));
        this.callbackThreadPool.execute(this::run);
        this.refillThreadPool.scheduleAtFixedRate(this::refillToken, refillIntervalMs, refillIntervalMs, TimeUnit.MILLISECONDS);
        S3StreamMetricsManager.registerNetworkLimiterSupplier(type, this::getAvailableTokens, this::getQueueSize);
        LOGGER.info("AsyncNetworkBandwidthLimiter initialized, type: {}, tokenSize: {}, maxTokens: {}, refillIntervalMs: {}",
            type.getName(), tokenSize, maxTokens, refillIntervalMs);
    }

    private void run() {
        while (true) {
            lock.lock();
            try {
                while (!ableToConsume()) {
                    condition.await();
                }
                while (ableToConsume()) {
                    BucketItem head = queuedCallbacks.poll();
                    if (head == null) {
                        break;
                    }
                    long size = Math.min(head.size, MAX_TOKEN_PART_SIZE);
                    if (availableExtraTokens > 0) {
                        // consume extra tokens
                        availableExtraTokens = Math.max(-maxTokens, availableExtraTokens - size);
                    } else {
                        reduceToken(size);
                    }
                    head.complete();
                }
            } catch (InterruptedException ignored) {
                break;
            } finally {
                lock.unlock();
            }
        }
    }

    private void refillToken() {
        lock.lock();
        try {
            availableTokens = Math.min(availableTokens + this.tokenSize, this.maxTokens);
            boolean isExhausted = isExhausted();
            if (tokenExhausted && isExhausted) {
                tokenExhaustedDuration += refillIntervalMs;
            } else {
                tokenExhaustedDuration = 0;
            }
            tokenExhausted = isExhausted;
            if (Duration.ofMillis(tokenExhaustedDuration).compareTo(Duration.ofSeconds(1)) >= 0) {
                availableExtraTokens = Math.min(availableExtraTokens + extraTokenSize, extraTokenSize);
                tokenExhaustedDuration = 0;
            }
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean ableToConsume() {
        if (queuedCallbacks.isEmpty()) {
            return false;
        }
        return availableTokens > 0 || availableExtraTokens > 0;
    }

    private boolean isExhausted() {
        if (queuedCallbacks.isEmpty()) {
            return false;
        }
        return availableTokens <= 0;
    }

    public void shutdown() {
        this.callbackThreadPool.shutdown();
        this.refillThreadPool.shutdown();
    }

    public long getMaxTokens() {
        return maxTokens;
    }

    public long getAvailableTokens() {
        lock.lock();
        try {
            return availableTokens;
        } finally {
            lock.unlock();
        }
    }

    public long getAvailableExtraTokens() {
        lock.lock();
        try {
            return availableExtraTokens;
        } finally {
            lock.unlock();
        }
    }

    public int getQueueSize() {
        lock.lock();
        try {
            return queuedCallbacks.size();
        } finally {
            lock.unlock();
        }
    }

    private void forceConsume(long size) {
        lock.lock();
        try {
            reduceToken(size);
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.whenComplete((v, e) -> logMetrics(size, throttleStrategy));
        if (Objects.requireNonNull(throttleStrategy) == ThrottleStrategy.BYPASS) {
            forceConsume(size);
            cf.complete(null);
        } else {
            lock.lock();
            try {
                if (availableTokens < 0 || !queuedCallbacks.isEmpty()) {
                    queuedCallbacks.offer(new BucketItem(throttleStrategy, size, cf));
                    condition.signalAll();
                } else {
                    reduceToken(size);
                    cf.complete(null);
                }
            } finally {
                lock.unlock();
            }
        }
        return cf;
    }

    private void reduceToken(long size) {
        this.availableTokens = Math.max(-maxTokens, availableTokens - size);
    }

    private void logMetrics(long size, ThrottleStrategy strategy) {
        NetworkStats.getInstance().networkUsageTotalStats(type, strategy).add(MetricsLevel.INFO, size);
    }

    public enum Type {
        INBOUND("Inbound"),
        OUTBOUND("Outbound");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private class BucketItem implements Comparable<BucketItem> {
        private final ThrottleStrategy strategy;
        private final CompletableFuture<Void> cf;
        private final long timestamp;
        private long size;

        BucketItem(ThrottleStrategy strategy, long size, CompletableFuture<Void> cf) {
            this.strategy = strategy;
            this.size = size;
            this.cf = cf;
            this.timestamp = System.nanoTime();
        }

        @Override
        public int compareTo(BucketItem o) {
            if (strategy.priority() == o.strategy.priority()) {
                return Long.compare(timestamp, o.timestamp);
            }
            return Long.compare(strategy.priority(), o.strategy.priority());
        }

        public void complete() {
            size -= Math.min(size, MAX_TOKEN_PART_SIZE);
            if (size <= 0) {
                cf.complete(null);
                return;
            }
            queuedCallbacks.offer(this);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (BucketItem) obj;
            return this.strategy == that.strategy &&
                this.size == that.size &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(strategy, size, cf);
        }

        @Override
        public String toString() {
            return "BucketItem[" +
                "throttleStrategy=" + strategy + ", " +
                "size=" + size + ", " +
                "cf=" + cf + ']';
        }

    }
}
