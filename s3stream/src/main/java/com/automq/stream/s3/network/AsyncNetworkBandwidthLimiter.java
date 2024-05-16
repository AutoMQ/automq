/*
 * Copyright 2024, AutoMQ CO.,LTD.
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

public class AsyncNetworkBandwidthLimiter {
    private static final Logger LOGGER = new LogContext().logger(AsyncNetworkBandwidthLimiter.class);
    private static final float DEFAULT_EXTRA_TOKEN_RATIO = 0.1f;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long maxTokens;
    private final ScheduledExecutorService refillThreadPool;
    private final ExecutorService callbackThreadPool;
    private final Queue<BucketItem> queuedCallbacks;
    private final Type type;
    private final long tokenSize;
    private final long extraTokenSize;
    private long availableTokens;
    private long availableExtraTokens;
    private boolean tokenExhausted = false;
    private long tokenExhaustedDuration = 0;

    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs) {
        this(type, tokenSize, refillIntervalMs, tokenSize);
    }

    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs, long maxTokens) {
        this.type = type;
        this.extraTokenSize = (long) (maxTokens * DEFAULT_EXTRA_TOKEN_RATIO);
        this.tokenSize = (long) (tokenSize * (1 - DEFAULT_EXTRA_TOKEN_RATIO));
        this.availableTokens = this.tokenSize;
        this.maxTokens = (long) (maxTokens * (1 - DEFAULT_EXTRA_TOKEN_RATIO));
        this.queuedCallbacks = new PriorityQueue<>();
        this.refillThreadPool = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("refill-bucket-thread"));
        this.callbackThreadPool = Executors.newFixedThreadPool(1, new DefaultThreadFactory("callback-thread"));
        this.callbackThreadPool.execute(() -> {
            while (true) {
                lock.lock();
                try {
                    while (queuedCallbacks.isEmpty() || (availableTokens <= 0 && availableExtraTokens <= 0)) {
                        condition.await();
                    }
                    while (!queuedCallbacks.isEmpty() && (availableTokens > 0 || availableExtraTokens > 0)) {
                        BucketItem head = queuedCallbacks.poll();
                        if (availableExtraTokens > 0) {
                            // consume extra tokens
                            availableExtraTokens = Math.max(0, availableExtraTokens - head.size);
                        } else {
                            reduceToken(head.size);
                        }

                        logMetrics(head.size, head.strategy);
                        head.cf.complete(null);
                    }
                } catch (InterruptedException ignored) {
                    break;
                } finally {
                    lock.unlock();
                }
            }
        });
        this.refillThreadPool.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                availableTokens = Math.min(availableTokens + this.tokenSize, this.maxTokens);
                boolean isExhausted = availableTokens <= 0;
                if (tokenExhausted && isExhausted) {
                    tokenExhaustedDuration += refillIntervalMs;
                } else {
                    tokenExhaustedDuration = 0;
                }
                tokenExhausted = isExhausted;
                if (Duration.ofMillis(tokenExhaustedDuration).compareTo(Duration.ofSeconds(1)) >= 0) {
                    availableExtraTokens = extraTokenSize;
                    tokenExhaustedDuration = 0;
                }

                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }, refillIntervalMs, refillIntervalMs, TimeUnit.MILLISECONDS);
        S3StreamMetricsManager.registerNetworkLimiterSupplier(type, this::getAvailableTokens, this::getQueueSize);
        LOGGER.info("AsyncNetworkBandwidthLimiter initialized, type: {}, tokenSize: {}, maxTokens: {}, refillIntervalMs: {}",
            type.getName(), tokenSize, maxTokens, refillIntervalMs);
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

    public void forceConsume(long size) {
        lock.lock();
        try {
            reduceToken(size);
            logMetrics(size, ThrottleStrategy.BYPASS);
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        if (Objects.requireNonNull(throttleStrategy) == ThrottleStrategy.BYPASS) {
            forceConsume(size);
            cf.complete(null);
        } else {
            lock.lock();
            try {
                if (availableTokens < 0 || !queuedCallbacks.isEmpty()) {
                    queuedCallbacks.add(new BucketItem(throttleStrategy, size, cf));
                    condition.signalAll();
                } else {
                    reduceToken(size);
                    cf.complete(null);
                    logMetrics(size, throttleStrategy);
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

    static final class BucketItem implements Comparable<BucketItem> {
        private final ThrottleStrategy strategy;
        private final long size;
        private final CompletableFuture<Void> cf;

        BucketItem(ThrottleStrategy strategy, long size, CompletableFuture<Void> cf) {
            this.strategy = strategy;
            this.size = size;
            this.cf = cf;
        }

        @Override
        public int compareTo(BucketItem o) {
            return Long.compare(strategy.priority(), o.strategy.priority());
        }

        public CompletableFuture<Void> cf() {
            return cf;
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
