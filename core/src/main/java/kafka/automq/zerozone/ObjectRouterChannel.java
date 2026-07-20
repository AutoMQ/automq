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

package kafka.automq.zerozone;

import com.automq.stream.RecyclingByteBufSeqAlloc;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Threads;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.netty.buffer.ByteBuf;

public class ObjectRouterChannel implements RouterChannel {
    private static final ExecutorService ASYNC_EXECUTOR = Executors.newCachedThreadPool();
    private static final long OVER_CAPACITY_RETRY_DELAY_MS = 1000L;
    private static final long CACHE_WEIGHT_UNIT = 100L << 20;
    private static final long HEAP_PER_CACHE_WEIGHT_UNIT = 6L << 30;
    private static final long CACHE_MAX_WEIGHT = cacheMaxWeight(Systems.HEAP_MEMORY_SIZE);
    private static final long CACHE_EXPIRE_SECONDS = 10;
    // Owned by this class for the process lifetime so all router channels reuse the same slabs.
    private static final RecyclingByteBufSeqAlloc ALLOC =
        new RecyclingByteBufSeqAlloc(ByteBufAlloc.ROUTER_CHANNEL);
    private final Logger logger;
    private final AtomicLong mockOffset = new AtomicLong(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    // The data written by the local router channel is cached to avoid reading it back from object storage.
    private final Cache<ByteBuf /* channel offset */, CachedData> cache = CacheBuilder.newBuilder()
        .expireAfterWrite(CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS)
        .maximumWeight(CACHE_MAX_WEIGHT)
        .weigher((ByteBuf k, CachedData v) -> v.readableBytes())
        .removalListener(notification -> {
            CachedData data = notification.getValue();
            if (data != null) {
                data.release();
            }
        })
        .build();

    private final ObjectWALService wal;
    private final int nodeId;
    private final short channelId;

    private long channelEpoch = 0L;
    private final Queue<Long> channelEpochQueue = new LinkedList<>();
    private final Map<Long, RecordOffset> channelEpoch2LastRecordOffset = new HashMap<>();

    private final CompletableFuture<Void> startCf;
    private boolean closed;
    private CompletableFuture<Void> closeCf;

    public ObjectRouterChannel(int nodeId, short channelId, ObjectWALService wal) {
        this.logger = new LogContext(String.format("[OBJECT_ROUTER_CHANNEL-%s-%s] ", channelId, nodeId)).logger(ObjectRouterChannel.class);
        this.nodeId = nodeId;
        this.channelId = channelId;
        this.wal = wal;
        this.startCf = CompletableFuture.runAsync(() -> {
            try {
                wal.start();
            } catch (Throwable e) {
                logger.error("start object router channel failed.", e);
                throw new RuntimeException(e);
            }
        }, ASYNC_EXECUTOR);
    }

    @Override
    public CompletableFuture<AppendResult> append(int targetNodeId, short orderHint, ByteBuf data) {
        return startCf.thenCompose(nil -> append0(targetNodeId, orderHint, data));
    }

    CompletableFuture<AppendResult> append0(int targetNodeId, short orderHint, ByteBuf data) {
        // The record will be released after appending is done. Use the ALLOC to decrease the memory fragmentation.
        StreamRecordBatch record = StreamRecordBatch.of(targetNodeId, 0, mockOffset.incrementAndGet(), 1, data, ALLOC);
        for (; ; ) {
            readLock.lock();
            try {
                if (closed) {
                    record.release();
                    return CompletableFuture.failedFuture(new IllegalStateException("ObjectRouterChannel is closed"));
                }
                record.retain();
                return wal.append(TraceContext.DEFAULT, record).thenApply(walRst -> {
                    readLock.lock();
                    try {
                        if (closed) {
                            throw new IllegalStateException("ObjectRouterChannel is closed");
                        }
                        long epoch = this.channelEpoch;
                        ChannelOffset channelOffset = ChannelOffset.of(channelId, orderHint, nodeId, targetNodeId, walRst.recordOffset().buffer());
                        channelEpoch2LastRecordOffset.put(epoch, walRst.recordOffset());
                        return new AppendResult(epoch, channelOffset.byteBuf());
                    } finally {
                        readLock.unlock();
                    }
                }).whenComplete((rst, ex) -> {
                    readLock.lock();
                    try {
                        if (rst != null && !closed && targetNodeId != this.nodeId) {
                            // The channel offset is an immutable unpooled buffer, so the cache key does not own a
                            // reference. The cache owns the payload reference until a reader takes it or the entry is
                            // removed.
                            cache.put(rst.channelOffset().slice(), new CachedData(record.getPayload()));
                        } else {
                            record.release();
                        }
                    } finally {
                        readLock.unlock();
                    }
                });
            } catch (OverCapacityException e) {
                logger.warn("OverCapacityException occurred while appending, err={}", e.getMessage());
                // Use block-based delayed retries for network backpressure.
                Threads.sleep(OVER_CAPACITY_RETRY_DELAY_MS);
            } catch (Throwable e) {
                logger.error("[UNEXPECTED], append wal fail", e);
                record.release();
                return CompletableFuture.failedFuture(e);
            } finally {
                readLock.unlock();
            }
        }
    }

    @Override
    public CompletableFuture<ByteBuf> get(ByteBuf channelOffset) {
        return startCf.thenCompose(nil -> get0(channelOffset));
    }

    CompletableFuture<ByteBuf> get0(ByteBuf channelOffset) {
        CachedData cachedData = cache.getIfPresent(channelOffset);
        if (cachedData != null) {
            ByteBuf buf = cachedData.take();
            if (buf != null) {
                // Router channel data is read once, so the cache entry is no longer useful after a hit.
                cache.invalidate(channelOffset);
                return CompletableFuture.completedFuture(buf);
            }
        }
        return wal.get(DefaultRecordOffset.of(ChannelOffset.of(channelOffset).walRecordOffset())).thenApply(streamRecordBatch -> {
            ByteBuf payload = streamRecordBatch.getPayload().retainedSlice();
            streamRecordBatch.release();
            return payload;
        });
    }

    @Override
    public void nextEpoch(long epoch) {
        writeLock.lock();
        try {
            if (epoch > this.channelEpoch) {
                this.channelEpochQueue.add(epoch);
                this.channelEpoch = epoch;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void trim(long epoch) {
        writeLock.lock();
        try {
            RecordOffset recordOffset = null;
            for (; ; ) {
                Long channelEpoch = channelEpochQueue.peek();
                if (channelEpoch == null || channelEpoch > epoch) {
                    break;
                }
                channelEpochQueue.poll();
                RecordOffset removed = channelEpoch2LastRecordOffset.remove(channelEpoch);
                if (removed != null) {
                    recordOffset = removed;
                }
            }
            if (recordOffset != null) {
                wal.trim(recordOffset);
                logger.info("trim to epoch={} offset={}", epoch, recordOffset);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        if (closeCf != null) {
            return closeCf;
        }
        closeCf = startCf.thenRunAsync(() -> {
            writeLock.lock();
            try {
                closed = true;
                cache.invalidateAll();
                cache.cleanUp();
            } finally {
                writeLock.unlock();
            }
            FutureUtil.suppress(wal::shutdownGracefully, logger);
        }, ASYNC_EXECUTOR);
        return closeCf;
    }

    static final class CachedData {
        private final ByteBuf data;
        private final int readableBytes;
        private boolean ownedByCache = true;

        CachedData(ByteBuf data) {
            this.data = data;
            this.readableBytes = data.readableBytes();
        }

        synchronized ByteBuf take() {
            if (!ownedByCache) {
                return null;
            }
            ownedByCache = false;
            return data;
        }

        synchronized void release() {
            if (ownedByCache) {
                ownedByCache = false;
                data.release();
            }
        }

        int readableBytes() {
            return readableBytes;
        }
    }

    static long cacheMaxWeight(long heapMemorySize) {
        long units = heapMemorySize / HEAP_PER_CACHE_WEIGHT_UNIT;
        if (heapMemorySize % HEAP_PER_CACHE_WEIGHT_UNIT != 0) {
            units++;
        }
        return Math.max(units, 1) * CACHE_WEIGHT_UNIT;
    }
}
