/*
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

import com.automq.stream.s3.metrics.stats.NetworkMetricsStats;
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
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long maxTokens;
    private long availableTokens;
    private final ScheduledExecutorService refillThreadPool;
    private final ExecutorService callbackThreadPool;
    private final Queue<BucketItem> queuedCallbacks;
    private final Type type;

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
                    while (queuedCallbacks.isEmpty() || availableTokens <= 0) {
                        condition.await();
                    }
                    while (!queuedCallbacks.isEmpty() && availableTokens > 0) {
                        BucketItem head = queuedCallbacks.poll();
                        availableTokens -= head.size;
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
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }, refillIntervalMs, refillIntervalMs, TimeUnit.MILLISECONDS);
        NetworkMetricsStats.registerNetworkInboundAvailableBandwidth(type, () -> {
            lock.lock();
            try {
                return availableTokens;
            } finally {
                lock.unlock();
            }
        });
        NetworkMetricsStats.registerNetworkLimiterQueueSize(type, () -> {
            lock.lock();
            try {
                return queuedCallbacks.size();
            } finally {
                lock.unlock();
            }
        });
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

    public void forceConsume(long size) {
        lock.lock();
        try {
            availableTokens -= size;
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
                availableTokens -= size;
                cf.complete(null);
                logMetrics(size);
            }
        } finally {
            lock.unlock();
        }
        return cf;
    }

    private void logMetrics(long size) {
        if (type == Type.INBOUND) {
            NetworkMetricsStats.getOrCreateNetworkInboundUsageCounter().inc(size);
        } else {
            NetworkMetricsStats.getOrCreateNetworkOutboundUsageCounter().inc(size);
        }
    }

    record BucketItem(int priority, long size, CompletableFuture<Void> cf) implements Comparable<BucketItem> {
        @Override
        public int compareTo(BucketItem o) {
            return Long.compare(priority, o.priority);
        }
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
}
