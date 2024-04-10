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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.LRUCache;
import io.netty.channel.DefaultEventLoop;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The DataBlockCache, akin to Linux's Page Cache, has the following responsibilities:
 * - Caching data blocks, releasing the least active data block when the cache size surpasses the maximum size.
 * - Responsible for fetching data from S3 in cases of cache misses.
 */
@EventLoopSafe
public class DataBlockCache {
    final Cache[] caches;
    /**
     * Limit the cache size, it real size may be slightly larger than the max size.
     */
    final AsyncSemaphore sizeLimiter;
    private final long maxSize;

    public DataBlockCache(long maxSize, DefaultEventLoop[] eventLoops) {
        this.maxSize = maxSize;
        this.sizeLimiter = new AsyncSemaphore(maxSize);
        this.caches = new Cache[eventLoops.length];
        for (int i = 0; i < eventLoops.length; i++) {
            caches[i] = new Cache(eventLoops[i]);
        }
    }

    /**
     * Get the data block from cache.
     * <p>
     * Note: the data block should invoke the {@link DataBlock#release()} after using.
     *
     * @param objectReader   the object reader could be used read the data block when cache misses
     * @param dataBlockIndex the required data block's index
     * @return the future of {@link DataBlock}, the future will be completed in the same stream's eventLoop.
     */
    public CompletableFuture<DataBlock> getBlock(ObjectReader objectReader, DataBlockIndex dataBlockIndex) {
        Cache cache = cache(dataBlockIndex.streamId());
        return cache.getBlock(objectReader, dataBlockIndex);
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
        final Map<DataBlockGroupKey, DataBlock> blocks = new ConcurrentHashMap<>();
        final LRUCache<DataBlockGroupKey, DataBlock> lru = new LRUCache<>();
        final Map<DataBlockGroupKey, DataBlock> inactive = new ConcurrentHashMap<>();
        private final DefaultEventLoop eventLoop;

        public Cache(DefaultEventLoop eventLoop) {
            this.eventLoop = eventLoop;
        }

        public CompletableFuture<DataBlock> getBlock(ObjectReader objectReader, DataBlockIndex dataBlockIndex) {
            if (Thread.currentThread().getId() != eventLoop.threadProperties().id()) {
                throw new IllegalStateException("getBlock must be invoked in the same eventLoop");
            }
            long objectId = objectReader.metadata().objectId();
            DataBlockGroupKey key = new DataBlockGroupKey(objectId, dataBlockIndex);
            DataBlock dataBlock = blocks.get(key);
            if (dataBlock == null) {
                DataBlock newDataBlock = new DataBlock(objectId, dataBlockIndex, this);
                read(objectReader, newDataBlock, eventLoop);
                put(key, newDataBlock);
                dataBlock = newDataBlock;
            }
            lru.touchIfExist(key);
            CompletableFuture<DataBlock> cf = new CompletableFuture<>();
            // if the data is already loaded, the listener will be invoked right now,
            // else the listener will be invoked immediately after data loaded in the same eventLoop.
            dataBlock.dataFuture().whenComplete((db, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                    return;
                }
                db.retain();
                cf.complete(db);
            });
            return cf;
        }

        private void read(ObjectReader reader, DataBlock dataBlock, DefaultEventLoop eventLoop) {
            reader.retain();
            boolean acquired = sizeLimiter.acquire(dataBlock.dataBlockIndex().size(), () -> {
                reader.read(dataBlock.dataBlockIndex()).whenCompleteAsync((rst, ex) -> {
                    reader.release();
                    if (ex != null) {
                        dataBlock.completeExceptionally(ex);
                    } else {
                        dataBlock.complete(rst);
                    }
                    lru.put(new DataBlockGroupKey(dataBlock.objectId(), dataBlock.dataBlockIndex()), dataBlock);
                    if (!sizeLimiter.hasPermits()) {
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

        private void put(DataBlockGroupKey key, DataBlock dataBlock) {
            blocks.put(key, dataBlock);
        }

        void evict() {
            eventLoop.execute(this::evict0);
        }

        private void evict0() {
            while (!sizeLimiter.hasPermits()) {
                Map.Entry<DataBlockGroupKey, DataBlock> entry = null;
                if (!inactive.isEmpty()) {
                    Iterator<Map.Entry<DataBlockGroupKey, DataBlock>> it = inactive.entrySet().iterator();
                    if (it.hasNext()) {
                        entry = it.next();
                        it.remove();
                        lru.remove(entry.getKey());
                    }
                }
                if (entry == null) {
                    entry = lru.pop();
                }
                if (entry == null) {
                    break;
                }
                DataBlockGroupKey key = entry.getKey();
                DataBlock dataBlock = entry.getValue();
                dataBlock.free();
                blocks.remove(key);
            }
        }

        public void markUnread(DataBlock dataBlock) {
            inactive.remove(new DataBlockGroupKey(dataBlock.objectId(), dataBlock.dataBlockIndex()));
        }

        public void markRead(DataBlock dataBlock) {
            DataBlockGroupKey key = new DataBlockGroupKey(dataBlock.objectId(), dataBlock.dataBlockIndex());
            if (blocks.containsKey(key)) {
                inactive.put(new DataBlockGroupKey(dataBlock.objectId(), dataBlock.dataBlockIndex()), dataBlock);
            }
        }

    }

}
