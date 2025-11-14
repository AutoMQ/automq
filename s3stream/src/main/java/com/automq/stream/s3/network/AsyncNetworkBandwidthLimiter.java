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

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.util.concurrent.DefaultThreadFactory;

public class AsyncNetworkBandwidthLimiter implements NetworkBandwidthLimiter {
    private static final Logger LOGGER = new LogContext().logger(AsyncNetworkBandwidthLimiter.class);
    private static final long MAX_TOKEN_PART_SIZE = 1024 * 1024;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long maxTokens;
    private final ScheduledExecutorService refillThreadPool;
    private final ExecutorService callbackThreadPool;
    private final Queue<BucketItem> queuedCallbacks;
    private final Type type;
    private final long tokenSize;
    private final AtomicLong availableTokens;

    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs) {
        this(type, tokenSize, refillIntervalMs, tokenSize);
    }

    @SuppressWarnings("this-escape")
    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs, long maxTokens) {
        this.type = type;
        this.tokenSize = tokenSize;
        this.availableTokens = new AtomicLong(this.tokenSize);
        this.maxTokens = maxTokens;
        this.queuedCallbacks = new PriorityQueue<>();
        this.refillThreadPool =
            Threads.newSingleThreadScheduledExecutor(new DefaultThreadFactory("refill-bucket-thread"), LOGGER);
        // The threads number must be larger than 1 because the #run will occupy one thread.
        this.callbackThreadPool = Threads.newFixedFastThreadLocalThreadPoolWithMonitor(2, "callback-thread", true, LOGGER);
        this.callbackThreadPool.execute(this::run);
        this.refillThreadPool.scheduleAtFixedRate(this::refillToken, refillIntervalMs, refillIntervalMs, TimeUnit.MILLISECONDS);
        S3StreamMetricsManager.registerNetworkLimiterQueueSizeSupplier(type, this::getQueueSize);
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
                    BucketItem head = queuedCallbacks.peek();
                    if (head == null) {
                        break;
                    }
                    long size = Math.min(head.size, MAX_TOKEN_PART_SIZE);
                    reduceToken(size);
                    if (head.complete(size, callbackThreadPool)) {
                        queuedCallbacks.poll();
                    }
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
            this.availableTokens.getAndUpdate(old -> Math.min(old + this.tokenSize, this.maxTokens));
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean ableToConsume() {
        if (queuedCallbacks.isEmpty()) {
            return false;
        }
        return availableTokens.get() > 0;
    }

    public void shutdown() {
        this.callbackThreadPool.shutdown();
        this.refillThreadPool.shutdown();
    }

    public long getMaxTokens() {
        return maxTokens;
    }

    public long getAvailableTokens() {
        return availableTokens.get();
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
        reduceToken(size);
    }

    public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.whenComplete((v, e) -> NetworkStats.getInstance().networkUsageTotalStats(type, throttleStrategy).add(MetricsLevel.INFO, size));
        if (Objects.requireNonNull(throttleStrategy) == ThrottleStrategy.BYPASS) {
            forceConsume(size);
            cf.complete(null);
        } else {
            lock.lock();
            try {
                if (availableTokens.get() <= 0 || !queuedCallbacks.isEmpty()) {
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
        this.availableTokens.getAndUpdate(old -> Math.max(-maxTokens, old - size));
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

        public boolean complete(long completeSize, ExecutorService executor) {
            size -= completeSize;
            if (size <= 0) {
                executor.submit(() -> cf.complete(null));
                NetworkStats.getInstance().networkLimiterQueueTimeStats(type, strategy).record(System.nanoTime() - timestamp);
                return true;
            }
            return false;
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
