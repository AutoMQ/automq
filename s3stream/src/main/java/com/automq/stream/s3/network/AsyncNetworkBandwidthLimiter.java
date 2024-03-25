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
import io.netty.util.concurrent.DefaultThreadFactory;
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
    private static final float DEFAULT_EXTRA_TOKEN_RATIO = 0.1f;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long maxTokens;
    private final ScheduledExecutorService refillThreadPool;
    private final ExecutorService callbackThreadPool;
    private final Queue<BucketItem> queuedCallbacks;
    private final Type type;
    private long availableTokens;
    private long extraTokens;

    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs, long maxTokenSize) {
        this.type = type;
        this.availableTokens = tokenSize;
        this.maxTokens = maxTokenSize;
        this.queuedCallbacks = new PriorityQueue<>();
        this.refillThreadPool = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("refill-bucket-thread"));
        this.callbackThreadPool = Executors.newFixedThreadPool(1, new DefaultThreadFactory("callback-thread"));
        this.callbackThreadPool.execute(() -> {
            while (true) {
                lock.lock();
                try {
                    while (queuedCallbacks.isEmpty() || (availableTokens <= 0 && extraTokens <= 0)) {
                        condition.await();
                    }
                    while (!queuedCallbacks.isEmpty() && (availableTokens > 0 || extraTokens > 0)) {
                        BucketItem head = queuedCallbacks.poll();
                        reduceToken(head.size);
                        extraTokens = Math.max(0, extraTokens - head.size);
                        logMetrics(head.size);
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
                availableTokens = Math.min(availableTokens + tokenSize, maxTokenSize);
                if (availableTokens <= 0) {
                    // provide extra tokens to prevent starvation when available tokens are exhausted
                    extraTokens = (long) (tokenSize * DEFAULT_EXTRA_TOKEN_RATIO);
                } else {
                    // disable extra tokens when available tokens are sufficient
                    extraTokens = 0;
                }
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }, refillIntervalMs, refillIntervalMs, TimeUnit.MILLISECONDS);
        S3StreamMetricsManager.registerNetworkLimiterSupplier(type, this::getAvailableTokens, this::getQueueSize);
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

    public long getExtraTokens() {
        lock.lock();
        try {
            return extraTokens;
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
            logMetrics(size);
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
            cf = consume(throttleStrategy.priority(), size);
        }
        return cf;
    }

    private CompletableFuture<Void> consume(int priority, long size) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        lock.lock();
        try {
            if (availableTokens < 0 || !queuedCallbacks.isEmpty()) {
                queuedCallbacks.add(new BucketItem(priority, size, cf));
                condition.signalAll();
            } else {
                reduceToken(size);
                cf.complete(null);
                logMetrics(size);
            }
        } finally {
            lock.unlock();
        }
        return cf;
    }

    private void reduceToken(long size) {
        this.availableTokens = Math.max(-maxTokens, availableTokens - size);
    }

    private void logMetrics(long size) {
        NetworkStats.getInstance().networkUsageStats(type).add(MetricsLevel.INFO, size);
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
        private final int priority;
        private final long size;
        private final CompletableFuture<Void> cf;

        BucketItem(int priority, long size, CompletableFuture<Void> cf) {
            this.priority = priority;
            this.size = size;
            this.cf = cf;
        }

        @Override
        public int compareTo(BucketItem o) {
            return Long.compare(priority, o.priority);
        }

        public int priority() {
            return priority;
        }

        public long size() {
            return size;
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
            return this.priority == that.priority &&
                this.size == that.size &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(priority, size, cf);
        }

        @Override
        public String toString() {
            return "BucketItem[" +
                "priority=" + priority + ", " +
                "size=" + size + ", " +
                "cf=" + cf + ']';
        }

    }
}
