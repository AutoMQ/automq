/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.operator.ObjectStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * Broker-owned cache of immutable RouterWAL bytes, shared across bucket clients and channel readers.
 * Fixed-size range fills bound read-ahead and reserve their maximum size before starting. Completed,
 * unused blocks are evicted in LRU order; busy blocks stay pinned until callers have copied their data.
 * Each entry keeps a full-block reservation, including short EOF blocks. At 8 MiB per block a 1 GiB
 * budget holds at most 128 entries. Ranges crossing a block boundary bypass the cache and may exceed 8 MiB.
 * The budget excludes caller copies and storage/allocator overhead.
 * Keys must never be overwritten with different data. Closing the provider closes this cache; late fills
 * are released after serving their existing callers. The cache does not own its storage clients.
 */
public final class RouterWALReadCache implements AutoCloseable {
    public static final int BLOCK_SIZE = 8 * 1024 * 1024;
    private static final Metrics.LongCounterBundle ACCESS = Metrics.instance()
        .longCounter("kafka_stream_router_wal_read_cache_access", "RouterWAL cache accesses", "count");
    private static final Metrics.LongCounterBundle.LongCounter HIT = accessCounter("hit");
    private static final Metrics.LongCounterBundle.LongCounter MISS = accessCounter("miss");
    private static final Metrics.LongCounterBundle.LongCounter INFLIGHT = accessCounter("inflight");
    private static final Metrics.LongCounterBundle.LongCounter BYPASS = accessCounter("bypass");
    private static final Metrics.LongCounterBundle.LongCounter EARLY_READS = Metrics.instance()
        .longCounter("kafka_stream_router_wal_read_cache_early_reads", "Validated ranges served before fill completion", "count")
        .register(MetricsLevel.INFO, Attributes.empty());
    private static final Metrics.LongGaugeBundle MEMORY = Metrics.instance()
        .longGauge("kafka_stream_router_wal_read_cache_memory", "RouterWAL cache bytes including fill reservations", "bytes");

    private final long maxBytes;
    private final int blockSize;
    private final boolean progressive;
    private final Map<Key, Entry> entries = new HashMap<>();
    private final LRUCache<Key, Entry> idle = new LRUCache<>();
    private final Metrics.LongGaugeBundle.LongGauge usedGauge;
    private final Metrics.LongGaugeBundle.LongGauge capacityGauge;
    private long usedBytes;
    private boolean closed;

    /** Creates a shared cache with a byte budget of at least one 8 MiB block. */
    public RouterWALReadCache(long maxBytes) {
        this(maxBytes, BLOCK_SIZE, false);
    }

    /** Enables independently validated early range delivery while fills retain their reservations. */
    public RouterWALReadCache(long maxBytes, boolean progressive) {
        this(maxBytes, BLOCK_SIZE, progressive);
    }

    RouterWALReadCache(long maxBytes, int blockSize) {
        this(maxBytes, blockSize, false);
    }

    RouterWALReadCache(long maxBytes, int blockSize, boolean progressive) {
        if (blockSize <= 0 || maxBytes < blockSize) {
            throw new IllegalArgumentException("RouterWAL read cache must hold at least one block");
        }
        this.maxBytes = maxBytes;
        this.blockSize = blockSize;
        this.progressive = progressive;
        usedGauge = MEMORY.register(MetricsLevel.INFO, memoryAttributes("used"));
        capacityGauge = MEMORY.register(MetricsLevel.INFO, memoryAttributes("capacity"));
        usedGauge.record(this::usedBytes);
        capacityGauge.record(maxBytes);
    }

    /**
     * Returns an owned range that callers must release. Cached bytes are copied for each caller.
     * Only finite RouterWAL ranges contained in one block are cached. Other requests, or misses with
     * no available budget, retain the original storage path. Cancelling a caller does not cancel a fill.
     * Hits and joins reuse the existing fill without issuing another GET.
     */
    public CompletableFuture<ByteBuf> read(ObjectStorage storage, ObjectStorage.ReadOptions options,
        String path, long start, long end) {
        return read(storage, options, path, start, end, null);
    }

    /**
     * Allows a complete range to return before the fill ends only if earlyValidator accepts its private
     * copy. Rejected ranges wait for normal download completion. The validator must check independent
     * record integrity and identity; transport checksums that require EOF disable progress at the source.
     */
    public CompletableFuture<ByteBuf> read(ObjectStorage storage, ObjectStorage.ReadOptions options,
        String path, long start, long end, Predicate<ByteBuf> earlyValidator) {
        ObjectStorage.ReadOptions requestOptions = options.copy();
        Entry entry = null;
        boolean load = false;
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("RouterWAL read cache is closed"));
            }
            if (cacheable(requestOptions, start, end)) {
                Key key = new Key(storage, path, start / blockSize * blockSize);
                entry = entries.get(key);
                if (entry != null) {
                    (entry.data.isDone() ? HIT : INFLIGHT).add(1);
                    idle.remove(key);
                    entry.readers++;
                } else if (makeRoom()) {
                    MISS.add(1);
                    entry = new Entry(key, progressive ? new RouterWALReadProgress(blockSize, () -> EARLY_READS.add(1)) : null);
                    entries.put(key, entry);
                    usedBytes += blockSize;
                    load = true;
                }
            }
        }
        if (entry == null) {
            BYPASS.add(1);
            return storage.rangeRead(requestOptions, path, start, end);
        }
        CompletableFuture<ByteBuf> result = copyWhenReady(entry, requestOptions, start, end);
        if (entry.progress != null && earlyValidator != null) {
            entry.progress.add((int) (start - entry.key.start()), (int) (end - entry.key.start()), result, earlyValidator);
        }
        Entry acquired = entry;
        if (load) {
            try {
                ObjectStorage.ReadOptions fillOptions = requestOptions.copy();
                if (acquired.progress != null) {
                    fillOptions.progressListener(acquired.progress::newAttempt);
                }
                storage.rangeRead(fillOptions, path, acquired.key.start(), acquired.key.start() + blockSize)
                    .whenComplete((buf, error) -> completeFill(acquired, buf, error));
            } catch (Throwable ex) {
                completeFill(acquired, null, ex);
            }
        }
        return result;
    }

    /**
     * Copies a range from a successfully completed block, without starting a fill or evicting data.
     * An empty result means the range is not ready in this cache, including ranges starting beyond EOF.
     * This lets dual-layout readers reuse a known object before probing another path. Hits update LRU
     * order and the hit counter; misses have no effect. A closed cache returns a failed future.
     * Callers own and must release the returned copy, as with {@link #read}.
     */
    public Optional<CompletableFuture<ByteBuf>> readIfPresent(ObjectStorage storage, ObjectStorage.ReadOptions options,
        String path, long start, long end) {
        ObjectStorage.ReadOptions requestOptions = options.copy();
        Entry entry;
        synchronized (this) {
            if (closed) {
                return Optional.of(CompletableFuture.failedFuture(new IllegalStateException("RouterWAL read cache is closed")));
            }
            if (!cacheable(requestOptions, start, end)) {
                return Optional.empty();
            }
            Key key = new Key(storage, path, start / blockSize * blockSize);
            entry = entries.get(key);
            if (entry == null || !entry.data.isDone() || entry.buffer == null
                || start - key.start() >= entry.buffer.readableBytes()) {
                return Optional.empty();
            }
            idle.remove(key);
            entry.readers++;
            HIT.add(1);
        }
        return Optional.of(copyWhenReady(entry, requestOptions, start, end));
    }

    private boolean cacheable(ObjectStorage.ReadOptions options, long start, long end) {
        return options.source() == S3RequestSource.ROUTER_WAL && start >= 0 && end > start
            && start / blockSize == (end - 1) / blockSize && start <= Long.MAX_VALUE - blockSize;
    }

    private CompletableFuture<ByteBuf> copyWhenReady(Entry entry, ObjectStorage.ReadOptions requestOptions,
        long start, long end) {
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        entry.data.whenComplete((buf, error) -> {
            try {
                if (result.isDone()) {
                    return;
                }
                if (error != null) {
                    result.completeExceptionally(error);
                    return;
                }
                int relativeStart = (int) (start - entry.key.start());
                if (relativeStart >= buf.readableBytes()) {
                    // Preserve the storage error for a range starting beyond EOF.
                    entry.key.storage().rangeRead(requestOptions, entry.key.path(), start, end)
                        .whenComplete((data, ex) -> complete(result, data, ex));
                    return;
                }
                int length = (int) Math.min(end - start, buf.readableBytes() - relativeStart);
                // Link decoding mutates Kafka metadata. A retained slice would corrupt other readers.
                complete(result, buf.copy(buf.readerIndex() + relativeStart, length), null);
            } catch (Throwable ex) {
                result.completeExceptionally(ex);
            } finally {
                releaseReader(entry);
            }
        });
        return result;
    }

    /** Returns full-block reservations for cached data and outstanding fills, including pinned blocks. */
    public synchronized long usedBytes() {
        return usedBytes;
    }

    private boolean makeRoom() {
        while (maxBytes - usedBytes < blockSize) {
            Map.Entry<Key, Entry> oldest = idle.pop();
            if (oldest == null) {
                return false;
            }
            remove(oldest.getValue());
        }
        return true;
    }

    private void completeFill(Entry entry, ByteBuf buf, Throwable error) {
        if (entry.progress != null) {
            entry.progress.close();
        }
        if (error == null && (buf == null || buf.capacity() > blockSize)) {
            error = new IllegalStateException("RouterWAL range fill exceeded its reserved block size");
        }
        synchronized (this) {
            if (error == null) {
                entry.buffer = buf;
            } else {
                remove(entry);
            }
        }
        // Complete outside the cache lock: callbacks copy bytes and may submit further reads.
        if (error == null) {
            entry.data.complete(buf);
        } else {
            if (buf != null) {
                buf.release();
            }
            entry.data.completeExceptionally(error);
        }
    }

    private synchronized void releaseReader(Entry entry) {
        entry.readers--;
        if (entry.readers == 0 && entries.get(entry.key) == entry) {
            if (closed) {
                remove(entry);
            } else {
                idle.put(entry.key, entry);
            }
        }
    }

    private void remove(Entry entry) {
        entries.remove(entry.key);
        usedBytes -= blockSize;
        if (entry.buffer != null) {
            entry.buffer.release();
        }
    }

    private static void complete(CompletableFuture<ByteBuf> result, ByteBuf data, Throwable error) {
        if (error != null) {
            result.completeExceptionally(error);
        } else if (!result.complete(data)) {
            data.release();
        }
    }

    /** Releases idle blocks immediately; pending fills and pinned blocks drain with their callers. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        Map.Entry<Key, Entry> oldest;
        while ((oldest = idle.pop()) != null) {
            remove(oldest.getValue());
        }
        usedGauge.close();
        capacityGauge.close();
    }

    private static Metrics.LongCounterBundle.LongCounter accessCounter(String result) {
        return ACCESS.register(MetricsLevel.INFO, Attributes.of(AttributeKey.stringKey("result"), result));
    }

    private static Attributes memoryAttributes(String state) {
        return Attributes.of(AttributeKey.stringKey("state"), state);
    }

    private record Key(ObjectStorage storage, String path, long start) {
    }

    private static final class Entry {
        final Key key;
        final CompletableFuture<ByteBuf> data = new CompletableFuture<>();
        final RouterWALReadProgress progress;
        int readers = 1;
        ByteBuf buffer;

        Entry(Key key, RouterWALReadProgress progress) {
            this.key = key;
            this.progress = progress;
        }
    }
}
