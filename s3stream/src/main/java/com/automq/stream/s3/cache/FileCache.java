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

package com.automq.stream.s3.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Disk-backed byte-range cache with in-memory block allocation and configurable eviction.
 * Contiguous data is indexed by cache ID and position, independently of its location in the backing file.
 * Reads and writes copy data synchronously under the cache lock; mapped-file access is delegated to
 * {@link MappedFileStorage}. The backing file is recreated on construction and is not durable cache state.
 * Capacity and offsets use long byte counts; allocation block indexes remain int-sized.
 */
public class FileCache {
    /** Eviction order for cached byte-range entries. */
    public enum EvictionPolicy {
        LRU,
        FIFO
    }

    private static final int BLOCK_SIZE = 4 * 1024;
    public static final FileCache NOOP;

    static {
        try {
            NOOP = new FileCache("", 0, BLOCK_SIZE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final long maxSize;
    private final int blockSize;
    /**
     * The index of free blocks in the cache.
     */
    private final BitSet freeBlocks;
    /**
     * The ordered cache entries used for eviction.
     */
    private final LRUCache<Key, Blocks> evictionCache;
    /**
     * The map of cacheId to cache blocks.
     * Its value is a {@link NavigableMap} which is used to store the cache blocks in the order of the position.
     *
     * @see Key#cacheId
     * @see Key#position
     */
    final Map<Long /* segment-unique id */, NavigableMap<Long /* position /*/, Blocks>> cacheMap = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    /**
     * The count of free blocks in the cache.
     * Used for eviction.
     */
    int freeBlockCount;
    /**
     * The index of the free block which will be checked next.
     * Used for acquiring free blocks.
     */
    private int freeCheckPoint = 0;
    private final MappedFileStorage storage;
    private final AtomicLong cacheIdAlloc = new AtomicLong(0);

    /**
     * Creates a cache, rounding its capacity up to an allocation block boundary.
     *
     * @param path backing file, recreated for a positive capacity
     * @param size requested capacity in bytes; zero disables storage
     * @param blockSize allocation block size in bytes
     * @throws IOException if the backing file cannot be created or mapped
     */
    public FileCache(String path, long size, int blockSize) throws IOException {
        this(path, size, blockSize, MappedFileStorage.DEFAULT_MAPPING_REGION_SIZE_BYTES);
    }

    FileCache(String path, long size, int blockSize, int mappingSize) throws IOException {
        this(path, size, blockSize, mappingSize, EvictionPolicy.LRU);
    }

    /**
     * Creates a cache with the specified eviction order. FIFO preserves initial admission order
     * across reads and updates; LRU refreshes entries when they are read or updated.
     */
    public FileCache(String path, long size, int blockSize, EvictionPolicy policy) throws IOException {
        this(path, size, blockSize, MappedFileStorage.DEFAULT_MAPPING_REGION_SIZE_BYTES, policy);
    }

    FileCache(String path, long size, int blockSize, int mappingSize, EvictionPolicy policy) throws IOException {
        this.evictionCache = new LRUCache<>(Objects.requireNonNull(policy) == EvictionPolicy.LRU);
        this.blockSize = blockSize;
        size = align(size);
        this.maxSize = size;
        int blockCount = Math.toIntExact(size / blockSize);
        this.freeBlocks = new BitSet(blockCount);
        this.freeBlocks.set(0, blockCount, true);
        this.freeBlockCount = blockCount;
        this.storage = new MappedFileStorage(path, size, mappingSize);
    }

    /**
     * Creates a cache using 4 KiB allocation blocks.
     *
     * @param path backing file, recreated for a positive capacity
     * @param size requested capacity in bytes; zero disables storage
     * @throws IOException if the backing file cannot be created or mapped
     */
    public FileCache(String path, long size) throws IOException {
        this(path, size, BLOCK_SIZE);
    }

    /** Creates a cache using 4 KiB blocks and the specified eviction order. */
    public FileCache(String path, long size, EvictionPolicy policy) throws IOException {
        this(path, size, BLOCK_SIZE, policy);
    }

    /**
     * Allocates an identifier for an independent byte-range namespace within this cache.
     */
    public long newCacheId() {
        return cacheIdAlloc.incrementAndGet();
    }

    /**
     * Copies readable bytes into the namespace at the given logical position without consuming the source.
     * May evict entries according to the configured policy; admission is skipped if allocation cannot succeed.
     *
     * @param cacheId identifier of the independent byte-range namespace
     * @param position starting logical byte position within that namespace
     * @param data source buffer; its contents and reader/writer indexes are unchanged
     */
    public void put(long cacheId, long position, ByteBuf data) {
        tryPut(cacheId, position, data);
    }

    // Report skipped admission to the asynchronous batch-cache owner.
    boolean tryPut(long cacheId, long position, ByteBuf data) {
        writeLock.lock();
        try {
            int dataLength = data.readableBytes();
            NavigableMap<Long, Blocks> cache = cacheMap.computeIfAbsent(cacheId, k -> new TreeMap<>());
            Map.Entry<Long, Blocks> pos2block = cache.floorEntry(position);
            long cacheStartPosition;
            long cacheEndPosition;
            Blocks blocks;
            boolean coverPosition = pos2block != null && position <= pos2block.getKey() + pos2block.getValue().dataLength;
            boolean tooManyBlocks = pos2block != null && pos2block.getValue().indexes.length >= Blocks.MAX_BLOCK_COUNT;
            if (!coverPosition || tooManyBlocks) {
                // no existing cache covers the position, or the existing cache has too many blocks, create a new one
                cacheStartPosition = position;
                cacheEndPosition = position + dataLength;
                blocks = Blocks.EMPTY;
            } else {
                // found an existing cache which covers the position, use it and maybe extend its capacity
                cacheStartPosition = pos2block.getKey();
                blocks = pos2block.getValue();
                cacheEndPosition = Math.max(pos2block.getKey() + blocks.dataLength, position + dataLength);
            }
            // ensure the capacity, if the capacity change then update the cache index
            long moreCapacity = cacheEndPosition - (cacheStartPosition + blocks.indexes.length * (long) blockSize);
            long newDataLength = cacheEndPosition - cacheStartPosition;
            if (moreCapacity > 0) {
                // need to extend the capacity, acquire free blocks
                int[] indexes = ensureCapacity(cacheStartPosition, moreCapacity);
                if (indexes == null) {
                    return false;
                }
                int[] newIndexes = new int[blocks.indexes.length + indexes.length];
                System.arraycopy(blocks.indexes, 0, newIndexes, 0, blocks.indexes.length);
                System.arraycopy(indexes, 0, newIndexes, blocks.indexes.length, indexes.length);
                blocks = new Blocks(newIndexes, newDataLength);
            } else {
                blocks = new Blocks(blocks.indexes, newDataLength);
            }
            cache.put(cacheStartPosition, blocks);
            evictionCache.put(new Key(cacheId, cacheStartPosition), blocks);

            // write data to cache
            long positionDelta = position - cacheStartPosition;
            int written = 0;
            ByteBuffer[] nioBuffers = data.nioBuffers();
            int[] indexes = blocks.indexes;
            for (ByteBuffer nioBuffer : nioBuffers) {
                ByteBuf buf = Unpooled.wrappedBuffer(nioBuffer);
                while (buf.readableBytes() > 0) {
                    long writePosition = positionDelta + written;
                    int index = indexes[Math.toIntExact(writePosition / blockSize)];
                    int blockOffset = Math.toIntExact(writePosition % blockSize);
                    int length = Math.min(buf.readableBytes(), blockSize - blockOffset);
                    storage.write((long) index * blockSize + blockOffset,
                        buf.slice(buf.readerIndex(), length).nioBuffer());
                    buf.skipBytes(length);
                    written += length;
                }
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Copies a fully cached range into a new buffer, or returns empty on a miss.
     * The caller owns and must release the returned buffer.
     *
     * @param cacheId identifier of the independent byte-range namespace
     * @param position starting logical byte position within that namespace
     * @param length number of bytes to read, nonnegative
     * @return a caller-owned copy of the requested range, or empty if the range is not fully cached
     */
    public Optional<ByteBuf> get(long cacheId, long position, int length) {
        return get(new ByteRange(cacheId, position, length));
    }

    /**
     * Copies a fully cached range into a new buffer, or returns empty on a miss.
     * The caller owns and must release the returned buffer.
     *
     * @param range logical byte range to read
     * @return a caller-owned copy of the range, or empty if the range is not fully cached
     */
    public Optional<ByteBuf> get(ReadRange range) {
        readLock.lock();
        try {
            Map.Entry<Long, Blocks> entry = findRange(range);
            if (entry == null) {
                return Optional.empty();
            }
            return Optional.of(read(range, entry));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Checks every range and copies them in list order only when all are cached.
     * If any are missing, reports the original missing range instances without reading mapped bytes or refreshing LRU order.
     * Holds one read lock through lookup and copying, preventing eviction between them.
     * The caller owns and must release every returned buffer.
     *
     * @param ranges logical byte ranges to read in list order
     * @return copies of all ranges on a full hit, or all missing ranges with no buffers on a miss;
     *     empty input returns empty buffers and missing ranges
     */
    public BulkReadResult get(List<ReadRange> ranges) {
        readLock.lock();
        try {
            List<Map.Entry<Long, Blocks>> entries = new ArrayList<>(ranges.size());
            List<ReadRange> missingRanges = new ArrayList<>();
            for (ReadRange range : ranges) {
                Map.Entry<Long, Blocks> entry = findRange(range);
                if (entry == null) {
                    missingRanges.add(range);
                } else {
                    entries.add(entry);
                }
            }
            if (!missingRanges.isEmpty()) {
                return new BulkReadResult(List.of(), missingRanges);
            }
            List<ByteBuf> buffers = new ArrayList<>(ranges.size());
            try {
                Iterator<Map.Entry<Long, Blocks>> entryIterator = entries.iterator();
                for (ReadRange range : ranges) {
                    buffers.add(read(range, entryIterator.next()));
                }
                return new BulkReadResult(buffers, List.of());
            } catch (RuntimeException | Error e) {
                buffers.forEach(ByteBuf::release);
                throw e;
            }
        } finally {
            readLock.unlock();
        }
    }

    private ByteBuf read(ReadRange range, Map.Entry<Long, Blocks> entry) {
        long cacheStartPosition = entry.getKey();
        Blocks blocks = entry.getValue();
        ByteBuf buf = Unpooled.buffer(range.length());
        try {
            evictionCache.touchIfExist(new Key(range.cacheId(), cacheStartPosition));
            long nextPosition = range.position();
            int remaining = range.length();
            for (int i = 0; i < blocks.indexes.length; i++) {
                long cacheBlockEndPosition = cacheStartPosition + (long) (i + 1) * blockSize;
                if (cacheBlockEndPosition < nextPosition) {
                    continue;
                }
                long cacheBlockStartPosition = cacheBlockEndPosition - blockSize;
                int readSize = (int) Math.min(remaining, cacheBlockEndPosition - nextPosition);
                int writerIndex = buf.writerIndex();
                storage.read((long) blocks.indexes[i] * blockSize + nextPosition - cacheBlockStartPosition,
                    buf.internalNioBuffer(writerIndex, readSize));
                buf.writerIndex(writerIndex + readSize);
                remaining -= readSize;
                nextPosition += readSize;
                if (remaining <= 0) {
                    break;
                }
            }
            return buf;
        } catch (RuntimeException | Error e) {
            buf.release();
            throw e;
        }
    }

    private Map.Entry<Long, Blocks> findRange(ReadRange range) {
        NavigableMap<Long, Blocks> cache = cacheMap.get(range.cacheId());
        if (cache == null) {
            return null;
        }
        Map.Entry<Long, Blocks> entry = cache.floorEntry(range.position());
        if (entry == null || entry.getKey() + entry.getValue().dataLength < range.position() + range.length()) {
            return null;
        }
        return entry;
    }

    /**
     * An immutable logical byte range to read. Does not retain or reserve cached bytes.
     * Implementations may carry caller metadata; bulk misses return the original instances.
     */
    public interface ReadRange {
        /**
         * @return byte-range namespace
         */
        long cacheId();

        /**
         * @return logical starting byte position
         */
        long position();

        /**
         * @return requested byte count, nonnegative
         */
        int length();
    }

    /**
     * A logical byte range without caller metadata.
     *
     * @param cacheId byte-range namespace
     * @param position logical starting byte position
     * @param length requested byte count, nonnegative
     */
    public record ByteRange(long cacheId, long position, int length) implements ReadRange {
    }

    /**
     * An all-or-nothing bulk read result. Empty missing ranges indicate a full hit, including empty input.
     *
     * @param buffers caller-owned copies in input order on a hit; empty on a miss
     * @param missingRanges original instances of every missing input range in input order; empty on a hit
     */
    public record BulkReadResult(List<ByteBuf> buffers, List<ReadRange> missingRanges) {
    }

    /**
     * Ensure the capacity of cache
     *
     * @param cacheStartPosition if the eviction entries contain the current cache, then ensure capacity will return null.
     * @param size               size of data
     * @return the indexes of cache blocks
     */
    private int[] ensureCapacity(long cacheStartPosition, long size) {
        if (size > this.maxSize) {
            return null;
        }
        int requiredBlockCount = Math.toIntExact(align(size) / blockSize);
        int[] indexes = new int[requiredBlockCount];
        int acquiringBlockIndex = 0;
        // evict cache entries in policy order until the free blocks are enough
        while (freeBlockCount + acquiringBlockIndex < requiredBlockCount) {
            Map.Entry<Key, Blocks> entry = evictionCache.pop();
            if (entry == null) {
                break;
            }
            Key key = entry.getKey();
            Blocks blocks = entry.getValue();
            cacheMap.get(key.cacheId).remove(key.position);
            if (key.position == cacheStartPosition) {
                // eviction is conflict to current cache
                for (int i = 0; i < acquiringBlockIndex; i++) {
                    freeBlockCount++;
                    freeBlocks.set(indexes[i], true);
                }
                return null;
            }
            for (int blockIndex : blocks.indexes) {
                if (acquiringBlockIndex < indexes.length) {
                    indexes[acquiringBlockIndex++] = blockIndex;
                    freeBlocks.set(blockIndex, false);
                } else {
                    freeBlockCount++;
                    freeBlocks.set(blockIndex, true);
                }
            }
        }
        // acquire free blocks
        while (acquiringBlockIndex < indexes.length) {
            int next = freeBlocks.nextSetBit(freeCheckPoint);
            if (next >= 0) {
                indexes[acquiringBlockIndex++] = next;
                freeBlockCount--;
                freeBlocks.set(next, false);
                freeCheckPoint = next;
            } else if (freeCheckPoint != 0) {
                freeCheckPoint = 0;
            } else {
                // BUG
                return null;
            }
        }
        return indexes;
    }

    private long align(long size) {
        return size % blockSize == 0 ? size : Math.addExact(size, blockSize - size % blockSize);
    }

    static class Key implements Comparable<Key> {
        long cacheId;
        long position;

        public Key(Long cacheId, long position) {
            this.cacheId = cacheId;
            this.position = position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Key key = (Key) o;
            return position == key.position && Objects.equals(cacheId, key.cacheId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cacheId, position);
        }

        @Override
        public int compareTo(Key o) {
            int compareCacheId = Long.compare(cacheId, o.cacheId);
            return compareCacheId == 0 ? Long.compare(position, o.position) : compareCacheId;
        }
    }

    /**
     * Cache blocks
     * It contains backing-file block indexes in the order of the data.
     */
    static class Blocks {
        static final Blocks EMPTY = new Blocks(new int[0], 0);
        /**
         * The max count of blocks in a {@link Blocks}.
         * Used to ensure not too many blocks in a {@link Blocks}, as they will be evicted together once the cache is full.
         */
        static final int MAX_BLOCK_COUNT = 16;

        /**
         * The indexes of cache blocks
         */
        int[] indexes;

        /**
         * The length of data
         */
        long dataLength;

        public Blocks(int[] indexes, long dataLength) {
            this.indexes = indexes;
            this.dataLength = dataLength;
        }
    }

}
