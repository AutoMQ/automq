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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Utils;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InflightReadThrottle implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InflightReadThrottle.class);
    private static final Integer MAX_INFLIGHT_READ_SIZE = 256 * 1024 * 1024; //256MB
    private final int maxInflightReadBytes;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final Map<UUID, Integer> inflightQuotaMap = new HashMap<>();
    private final Queue<InflightReadItem> inflightReadQueue = new LinkedList<>();
    private final ExecutorService executorService = Threads.newFixedThreadPool(1,
        ThreadUtils.createThreadFactory("inflight-read-throttle-%d", false), LOGGER);

    private int remainingInflightReadBytes;

    public InflightReadThrottle() {
        this((int) (MAX_INFLIGHT_READ_SIZE * (1 - Utils.getMaxMergeReadSparsityRate())));
    }

    public InflightReadThrottle(int maxInflightReadBytes) {
        this.maxInflightReadBytes = maxInflightReadBytes;
        this.remainingInflightReadBytes = maxInflightReadBytes;
        executorService.execute(this);
        S3StreamMetricsManager.registerInflightReadSizeLimiterSupplier(this::getRemainingInflightReadBytes);
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public int getInflightQueueSize() {
        lock.lock();
        try {
            return inflightReadQueue.size();
        } finally {
            lock.unlock();
        }
    }

    public int getRemainingInflightReadBytes() {
        lock.lock();
        try {
            return remainingInflightReadBytes;
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<Void> acquire(UUID uuid, int readSize) {
        return acquire(TraceContext.DEFAULT, uuid, readSize);
    }

    @WithSpan
    public CompletableFuture<Void> acquire(TraceContext context, UUID uuid, int readSize) {
        context.currentContext();
        lock.lock();
        try {
            if (readSize > maxInflightReadBytes) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(String.format(
                    "read size %d exceeds max inflight read size %d", readSize, maxInflightReadBytes)));
            }
            if (readSize <= 0) {
                return CompletableFuture.completedFuture(null);
            }
            inflightQuotaMap.put(uuid, readSize);
            if (readSize <= remainingInflightReadBytes) {
                remainingInflightReadBytes -= readSize;
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> cf = new CompletableFuture<>();
            inflightReadQueue.offer(new InflightReadItem(readSize, cf));
            condition.signalAll();
            return cf;
        } finally {
            lock.unlock();
        }
    }

    public void release(UUID uuid) {
        lock.lock();
        try {
            Integer inflightReadSize = inflightQuotaMap.remove(uuid);
            if (inflightReadSize != null) {
                remainingInflightReadBytes += inflightReadSize;
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        while (true) {
            lock.lock();
            try {
                while (inflightReadQueue.isEmpty() || inflightReadQueue.peek().readSize > remainingInflightReadBytes) {
                    condition.await();
                }
                InflightReadItem inflightReadItem = inflightReadQueue.poll();
                if (inflightReadItem == null) {
                    continue;
                }
                remainingInflightReadBytes -= inflightReadItem.readSize;
                inflightReadItem.cf.complete(null);
            } catch (Exception e) {
                break;
            } finally {
                lock.unlock();
            }
        }
    }

    static final class InflightReadItem {
        private final int readSize;
        private final CompletableFuture<Void> cf;

        InflightReadItem(int readSize, CompletableFuture<Void> cf) {
            this.readSize = readSize;
            this.cf = cf;
        }

        public int readSize() {
            return readSize;
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
            var that = (InflightReadItem) obj;
            return this.readSize == that.readSize &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(readSize, cf);
        }

        @Override
        public String toString() {
            return "InflightReadItem[" +
                "readSize=" + readSize + ", " +
                "cf=" + cf + ']';
        }

    }
}
