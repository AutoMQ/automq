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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.LRUCache;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.AsyncSemaphore;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;
import com.automq.stream.utils.threads.EventLoopSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The DataBlockCache, akin to Linux's Page Cache, has the following responsibilities:
 * - Caching data blocks, releasing the least active data block when the cache size surpasses the maximum size.
 * - Responsible for fetching data from S3 in cases of cache misses.
 */
@EventLoopSafe
public class DataBlockCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataBlockCache.class);
    static final long DATA_TTL = TimeUnit.MINUTES.toMillis(1);
    static final long CHECK_EXPIRED_DATA_INTERVAL = TimeUnit.MINUTES.toMillis(1);
    final Cache[] caches;
    /**
     * Limit the cache size, it real size may be slightly larger than the max size.
     */
    final AsyncSemaphore sizeLimiter;
    private final long maxSize;
    private final Time time;

    public DataBlockCache(long maxSize, EventLoop[] eventLoops) {
        this(maxSize, eventLoops, Time.SYSTEM);
    }

    public DataBlockCache(long maxSize, EventLoop[] eventLoops, Time time) {
        this.maxSize = maxSize;
        this.sizeLimiter = new AsyncSemaphore(maxSize);
        this.time = time;
        this.caches = new Cache[eventLoops.length];
        for (int i = 0; i < eventLoops.length; i++) {
            caches[i] = new Cache(eventLoops[i]);
        }
        S3StreamMetricsManager.registerBlockCacheSizeSupplier(() -> maxSize - sizeLimiter.permits());
    }

    /**
     * Get the data block from cache.
     * <p>
     * Note: the data block should invoke the {@link DataBlock#release()} after using.
     *
     * @param options        the get options
     * @param objectReader   the object reader could be used read the data block when cache misses
     * @param dataBlockIndex the required data block's index
     * @return the future of {@link DataBlock}, the future will be completed in the same stream's eventLoop.
     */
    public CompletableFuture<DataBlock> getBlock(GetOptions options, ObjectReader objectReader,
        DataBlockIndex dataBlockIndex) {
        Cache cache = cache(dataBlockIndex.streamId());
        return cache.getBlock(options, objectReader, dataBlockIndex);
    }

    public CompletableFuture<DataBlock> getBlock(ObjectReader objectReader, DataBlockIndex dataBlockIndex) {
        return getBlock(GetOptions.DEFAULT, objectReader, dataBlockIndex);
    }

    public long available() {
        return sizeLimiter.permits();
    }

    @Override
    public String toString() {
        return "DataBlockCache{" +
            ", capacity=" + maxSize +
            ", remaining=" + sizeLimiter.permits() +
            '}';
    }

    private Cache cache(long streamId) {
        return caches[(int) Math.abs(streamId % caches.length)];
    }

    private void evict() {
        for (Cache cache : caches) {
            cache.evict();
        }
    }

    static class DataBlockGroupKey {
        final long objectId;
        final DataBlockIndex dataBlockIndex;

        public DataBlockGroupKey(long objectId, DataBlockIndex dataBlockIndex) {
            this.objectId = objectId;
            this.dataBlockIndex = dataBlockIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            DataBlockGroupKey key = (DataBlockGroupKey) o;
            return objectId == key.objectId && Objects.equals(dataBlockIndex, key.dataBlockIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objectId, dataBlockIndex);
        }
    }

    class Cache implements ReadStatusChangeListener {
        final Map<DataBlockGroupKey, DataBlock> blocks = new HashMap<>();
        final LRUCache<DataBlockGroupKey, DataBlock> lru = new LRUCache<>();
        private final EventLoop eventLoop;
        private long lastEvictExpiredDataTimestamp = time.milliseconds();

        public Cache(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
        }

        public CompletableFuture<DataBlock> getBlock(GetOptions options, ObjectReader objectReader,
            DataBlockIndex dataBlockIndex) {
            return FutureUtil.exec(() -> getBlock0(options, objectReader, dataBlockIndex), LOGGER, "getBlock");
        }

        private CompletableFuture<DataBlock> getBlock0(GetOptions options, ObjectReader objectReader,
            DataBlockIndex dataBlockIndex) {
            long objectId = objectReader.metadata().objectId();
            DataBlockGroupKey key = new DataBlockGroupKey(objectId, dataBlockIndex);
            DataBlock dataBlock = blocks.get(key);
            if (dataBlock == null) {
                DataBlock newDataBlock = new DataBlock(objectId, dataBlockIndex, this, time);
                dataBlock = newDataBlock;
                blocks.put(key, newDataBlock);
                read(options, objectReader, newDataBlock, eventLoop);
            }
            lru.touchIfExist(key);
            CompletableFuture<DataBlock> cf = new CompletableFuture<>();
            // if the data is already loaded, the listener will be invoked right now,
            // else the listener will be invoked immediately after data loaded in the same eventLoop.
            if (!dataBlock.dataFuture().isDone()) {
                if (options.isReadahead()) {
                    StorageOperationStats.getInstance().blockCacheReadaheadThroughput.add(MetricsLevel.INFO, dataBlock.dataBlockIndex().size());
                } else {
                    StorageOperationStats.getInstance().blockCacheBlockMissThroughput.add(MetricsLevel.INFO, dataBlock.dataBlockIndex().size());
                }
            }
            // DataBlock#retain should will before the complete the future to avoid the other read use #markRead to really free the data block.
            dataBlock.retain();
            dataBlock.dataFuture().whenComplete((db, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                    return;
                }
                cf.complete(db);
            });
            tryEvictExpired();
            return cf;
        }

        private void read(GetOptions getOptions, ObjectReader reader, DataBlock dataBlock, EventLoop eventLoop) {
            ThrottleStrategy throttleStrategy = getOptions.readahead ? ThrottleStrategy.CATCH_UP : ThrottleStrategy.BYPASS;
            reader.retain();
            boolean acquired = sizeLimiter.acquire(dataBlock.dataBlockIndex().size(), () -> {
                reader.read(new ObjectReader.ReadOptions().throttleStrategy(throttleStrategy), dataBlock.dataBlockIndex()).whenCompleteAsync((rst, ex) -> {
                    StorageOperationStats.getInstance().blockCacheReadS3Throughput.add(MetricsLevel.INFO, dataBlock.dataBlockIndex().size());
                    reader.release();
                    DataBlockGroupKey key = new DataBlockGroupKey(dataBlock.objectId(), dataBlock.dataBlockIndex());
                    if (ex != null) {
                        dataBlock.completeExceptionally(ex);
                        blocks.remove(key, dataBlock);
                    } else {
                        lru.put(key, dataBlock);
                        dataBlock.complete(rst);
                    }
                    if (sizeLimiter.requiredRelease()) {
                        // In the described scenario, with maxSize set to 1, upon the sequential arrival of requests #getBlock(size=2) and #getBlock(3),
                        // #getBlock(3) will wait in the queue until permits are available.
                        // If, after #getBlock(size=2) completes, permits are still lacking in the sizeLimiter, implying queued tasks,
                        // invoke #evict is necessary to release permits for scheduling #getBlock(3).
                        DataBlockCache.this.evict();
                    }
                }, eventLoop);
                // after the data block is freed, the sizeLimiter will be released and try re-schedule the waiting tasks
                return dataBlock.freeFuture();
            }, eventLoop);
            if (!acquired) {
                DataBlockCache.this.evict();
            }
        }

        void evict() {
            eventLoop.execute(this::evict0);
        }

        void tryEvictExpired() {
            long now = time.milliseconds();
            if (now - lastEvictExpiredDataTimestamp > CHECK_EXPIRED_DATA_INTERVAL) {
                lastEvictExpiredDataTimestamp = now;
                this.evict0();
            }
        }

        private void evict0() {
            // TODO: avoid awake more tasks than necessary
            long expiredTimestamp = time.milliseconds() - DATA_TTL;
            while (true) {
                Map.Entry<DataBlockGroupKey, DataBlock> entry;
                entry = lru.peek();
                if (entry == null) {
                    break;
                }
                DataBlockGroupKey key = entry.getKey();
                DataBlock dataBlock = entry.getValue();
                if (!dataBlock.isExpired(expiredTimestamp) && !sizeLimiter.requiredRelease()) {
                    break;
                }
                lru.pop();
                if (blocks.remove(key, dataBlock)) {
                    dataBlock.free();
                    StorageOperationStats.getInstance().blockCacheBlockEvictThroughput.add(MetricsLevel.INFO, dataBlock.dataBlockIndex().size());
                } else {
                    LOGGER.error("[BUG] duplicated free data block {}", dataBlock);
                }
            }
        }

        public void markUnread(DataBlock dataBlock) {
        }

        public void markRead(DataBlock dataBlock) {
            DataBlockGroupKey key = new DataBlockGroupKey(dataBlock.objectId(), dataBlock.dataBlockIndex());
            if (blocks.remove(key, dataBlock)) {
                lru.remove(key);
                dataBlock.free();
            }
        }
    }

    public static class GetOptions {
        public static final GetOptions DEFAULT = new GetOptions(false);

        final boolean readahead;

        public GetOptions(boolean readahead) {
            this.readahead = readahead;
        }

        public boolean isReadahead() {
            return readahead;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private boolean readahead;

            public Builder readahead(boolean readahead) {
                this.readahead = readahead;
                return this;
            }

            public GetOptions build() {
                return new GetOptions(readahead);
            }
        }

    }

}
