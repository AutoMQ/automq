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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;

/**
 * Shares consumable RouterWAL objects across readers. Each cached byte range is handed out at most
 * once because callers may mutate it. The provider owns this cache; returned slices own references
 * independent of eviction. Cache hits synchronize only on their object and loading hits await a
 * shared completion signal. The instance monitor coordinates miss registration, admission, and close.
 */
public class ObjectWALReadCache implements AutoCloseable {
    private static final long HEAP_UNIT = 6L << 30;
    private static final long CACHE_UNIT = 100L << 20;
    private final Cache<Key, CachedObject> cache;
    private final Map<Key, Load> loading = new ConcurrentHashMap<>();
    private final int maxInflight;
    private volatile boolean closed;

    /** Creates a shared cache with 100 MiB and two concurrent loads per rounded-up 6 GiB of heap. */
    public ObjectWALReadCache(long heapMemorySize) {
        this(cacheMaxWeight(heapMemorySize), maxInflight(heapMemorySize), Ticker.systemTicker());
    }

    ObjectWALReadCache(long maxWeight, int maxInflight, Ticker ticker) {
        this.maxInflight = maxInflight;
        this.cache = CacheBuilder.newBuilder()
            .concurrencyLevel(1)
            .maximumWeight(maxWeight)
            .weigher((Key key, CachedObject value) -> value.data.readableBytes())
            .expireAfterAccess(2, TimeUnit.SECONDS)
            .ticker(ticker)
            .removalListener(notification -> notification.getValue().release())
            .build();
    }

    /**
     * Reads a finite record range, returning an owned slice on its first cached access. Concurrent
     * misses share a full-object GET. Saturated loads and consumed ranges use isolated range
     * responses so a merged overlapping GET cannot share mutable bytes. Returned futures are
     * internal completion signals; callers must not complete or cancel them.
     */
    public CompletableFuture<ByteBuf> read(ObjectStorage storage, ReadOptions options, String path, long start, long end) {
        return read(new Key(storage, path), options, start, end, false);
    }

    private CompletableFuture<ByteBuf> read(Key key, ReadOptions options, long start, long end, boolean retryReadLoad) {
        if (closed) {
            return rangeRead(key.storage, options, key.path, start, end);
        }
        CachedObject object = cache.getIfPresent(key);
        if (object != null) {
            return readCached(key, object, options, start, end);
        }
        if (retryReadLoad) {
            return rangeRead(key.storage, options, key.path, start, end);
        }
        Load load = loading.get(key);
        boolean startLoad = false;
        if (load == null) {
            synchronized (this) {
                if (!closed) {
                    object = cache.getIfPresent(key);
                    if (object == null) {
                        load = loading.get(key);
                        if (load == null && loading.size() < maxInflight) {
                            load = new Load(key, options);
                            loading.put(key, load);
                            startLoad = true;
                        }
                    }
                }
            }
        }
        if (object != null) {
            return readCached(key, object, options, start, end);
        }
        if (load == null) {
            return rangeRead(key.storage, options, key.path, start, end);
        }
        CompletableFuture<ByteBuf> result = retryAfterLoad(load, options, start, end);
        if (startLoad) {
            load.start();
        }
        return result;
    }

    private CompletableFuture<ByteBuf> retryAfterLoad(Load load, ReadOptions options, long start, long end) {
        return load.future.handle((nil, error) -> null)
            .thenCompose(nil -> read(load.key, options, start, end, true));
    }

    private CompletableFuture<ByteBuf> readCached(Key key, CachedObject object, ReadOptions options, long start, long end) {
        try {
            ByteBuf slice = object.take(start, end);
            if (object.consumed) {
                cache.asMap().remove(key, object);
            }
            if (slice != null) {
                return CompletableFuture.completedFuture(slice);
            }
        } catch (Throwable ex) {
            return CompletableFuture.failedFuture(ex);
        }
        return rangeRead(key.storage, options, key.path, start, end);
    }

    private static CompletableFuture<ByteBuf> rangeRead(ObjectStorage storage, ReadOptions options,
        String path, long start, long end) {
        try {
            return storage.rangeRead(options, path, start, end).thenApply(buf -> {
                try {
                    return buf.copy();
                } finally {
                    buf.release();
                }
            });
        } catch (Throwable ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    /**
     * Releases cached objects and bypasses caching for subsequent reads. Existing GETs finish their
     * readers normally, releasing object ownership without admitting their results to the cache.
     */
    @Override
    public synchronized void close() {
        closed = true;
        cache.invalidateAll();
        cache.cleanUp();
    }

    static long cacheMaxWeight(long heapMemorySize) {
        return heapUnits(heapMemorySize) * CACHE_UNIT;
    }

    static int maxInflight(long heapMemorySize) {
        return Math.toIntExact(heapUnits(heapMemorySize) * 2);
    }

    private static long heapUnits(long heapMemorySize) {
        return Math.max(1, heapMemorySize / HEAP_UNIT + (heapMemorySize % HEAP_UNIT == 0 ? 0 : 1));
    }

    private static class Key {
        final ObjectStorage storage;
        final String path;

        Key(ObjectStorage storage, String path) {
            this.storage = storage;
            this.path = path;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Key)) {
                return false;
            }
            Key key = (Key) other;
            return storage == key.storage && path.equals(key.path);
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(storage) + path.hashCode();
        }
    }

    private class Load {
        private final Key key;
        private final ReadOptions options;
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        Load(Key key, ReadOptions options) {
            this.key = key;
            this.options = options;
        }

        void start() {
            try {
                key.storage.rangeRead(options, key.path, 0, ObjectStorage.RANGE_READ_TO_END)
                    .whenComplete(this::finish);
            } catch (Throwable ex) {
                finish(null, ex);
            }
        }

        private void finish(ByteBuf buf, Throwable failure) {
            CachedObject object = null;
            if (failure == null) {
                try {
                    object = new CachedObject(buf);
                } catch (Throwable ex) {
                    buf.release();
                    failure = ex;
                }
            }
            synchronized (ObjectWALReadCache.this) {
                if (object != null) {
                    if (closed || object.consumed) {
                        object.release();
                    } else {
                        cache.put(key, object);
                    }
                }
                loading.remove(key, this);
            }
            // Notify only after publication and slot release; every waiter retries through read.
            if (failure == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(failure);
            }
        }
    }

    private static class CachedObject {
        final ByteBuf data;
        final RangeSet<Long> unread = TreeRangeSet.create();
        boolean released;
        volatile boolean consumed;

        CachedObject(ByteBuf buf) {
            data = buf.slice();
            WALObjectHeader header = WALObjectHeader.unmarshal(data);
            long end = header.size() + header.length();
            if (header.length() < 0 || end != data.readableBytes()) {
                throw new IllegalArgumentException("Invalid WAL object length");
            }
            int start = skipTrimRecords(header.size(), (int) end);
            if (start < end) {
                unread.add(Range.closedOpen((long) start, end));
            }
            consumed = unread.isEmpty();
        }

        private int skipTrimRecords(int start, int end) {
            while (start < end) {
                int recordHeaderSize = RecordHeader.RECORD_HEADER_SIZE;
                if (end - start < recordHeaderSize + StreamRecordBatchCodec.HEADER_SIZE
                    || data.getInt(start) != RecordHeader.RECORD_HEADER_DATA_MAGIC_CODE) {
                    throw new IllegalArgumentException("Invalid WAL record header");
                }
                int length = data.getInt(start + Integer.BYTES);
                if (length < StreamRecordBatchCodec.HEADER_SIZE || length > end - start - recordHeaderSize) {
                    throw new IllegalArgumentException("Invalid WAL record length");
                }
                int batch = start + recordHeaderSize;
                if (data.getLong(batch + StreamRecordBatchCodec.STREAM_ID_POS) != -1L
                    || data.getLong(batch + StreamRecordBatchCodec.EPOCH_POS) != -1L
                    || data.getInt(batch + StreamRecordBatchCodec.LAST_OFFSET_DELTA_POS) != 0) {
                    break;
                }
                if (data.getByte(batch) != StreamRecordBatchCodec.MAGIC_V0
                    || data.getLong(batch + StreamRecordBatchCodec.BASE_OFFSET_POS) != 0
                    || length != StreamRecordBatchCodec.HEADER_SIZE
                    || data.getInt(batch + StreamRecordBatchCodec.PAYLOAD_LENGTH_POS) != 0) {
                    throw new IllegalArgumentException("Invalid WAL trim record");
                }
                start += recordHeaderSize + length;
            }
            return start;
        }

        synchronized ByteBuf take(long start, long end) {
            if (released) {
                return null;
            }
            if (start < 0 || end <= start || end > data.readableBytes()) {
                throw new IllegalArgumentException("Invalid WAL read range");
            }
            Range<Long> range = Range.closedOpen(start, end);
            if (!unread.encloses(range)) {
                return null;
            }
            ByteBuf slice = data.retainedSlice((int) start, (int) (end - start));
            unread.remove(range);
            consumed = unread.isEmpty();
            return slice;
        }

        synchronized void release() {
            if (!released) {
                released = true;
                data.release();
            }
        }
    }
}
