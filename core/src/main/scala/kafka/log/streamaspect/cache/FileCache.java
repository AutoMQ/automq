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

package kafka.log.streamaspect.cache;

import kafka.log.streamaspect.ElasticTimeIndex;
import kafka.log.streamaspect.ElasticTransactionIndex;

import com.automq.stream.s3.cache.LRUCache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.BitSet;
import java.util.HashMap;
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
 * File cache which used for cache {@link ElasticTimeIndex} data and {@link ElasticTransactionIndex} data.
 * It uses a {@link MappedByteBuffer} as the cache, and the cache is divided into blocks.
 * Contiguous data will be stored in a {@link Blocks} object, and the {@link Blocks} object contains the indexes of the cache blocks.
 * A {@link LRUCache} is used to manage the cache, each block in the least recently used {@link Blocks} will be evicted once the cache is full.
 */
public class FileCache {
    private static final int BLOCK_SIZE = 4 * 1024;
    public static final FileCache NOOP;

    static {
        try {
            NOOP = new FileCache("", 0, BLOCK_SIZE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final int maxSize;
    private final int blockSize;
    /**
     * The index of free blocks in the cache.
     */
    private final BitSet freeBlocks;
    /**
     * The LRU cache which contains the cache blocks. Used for eviction.
     */
    private final LRUCache<Key, Blocks> lru = new LRUCache<>();
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
    private final MappedByteBuffer cacheByteBuffer;
    private final AtomicLong cacheIdAlloc = new AtomicLong(0);

    public FileCache(String path, int size, int blockSize) throws IOException {
        this.blockSize = blockSize;
        size = align(size);
        this.maxSize = size;
        int blockCount = size / blockSize;
        this.freeBlocks = new BitSet(blockCount);
        this.freeBlocks.set(0, blockCount, true);
        this.freeBlockCount = blockCount;
        if (size > 0) {
            File file = new File(path);
            do {
                Files.deleteIfExists(file.toPath());
            }
            while (!file.createNewFile());
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(size);
                this.cacheByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
            }
        } else {
            this.cacheByteBuffer = null;
        }
    }

    public FileCache(String path, int size) throws IOException {
        this(path, size, BLOCK_SIZE);
    }

    public long newCacheId() {
        return cacheIdAlloc.incrementAndGet();
    }

    public void put(long cacheId, long position, ByteBuf data) {
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
            int moreCapacity = (int) (cacheEndPosition - (cacheStartPosition + blocks.indexes.length * (long) blockSize));
            int newDataLength = (int) (cacheEndPosition - cacheStartPosition);
            if (moreCapacity > 0) {
                // need to extend the capacity, acquire free blocks
                int[] indexes = ensureCapacity(cacheStartPosition, moreCapacity);
                if (indexes == null) {
                    return;
                }
                int[] newIndexes = new int[blocks.indexes.length + indexes.length];
                System.arraycopy(blocks.indexes, 0, newIndexes, 0, blocks.indexes.length);
                System.arraycopy(indexes, 0, newIndexes, blocks.indexes.length, indexes.length);
                blocks = new Blocks(newIndexes, newDataLength);
            } else {
                blocks = new Blocks(blocks.indexes, newDataLength);
            }
            cache.put(cacheStartPosition, blocks);
            lru.put(new Key(cacheId, cacheStartPosition), blocks);

            // write data to cache
            ByteBuffer cacheByteBuffer = this.cacheByteBuffer.duplicate();
            int positionDelta = (int) (position - cacheStartPosition);
            int written = 0;
            ByteBuffer[] nioBuffers = data.nioBuffers();
            int[] indexes = blocks.indexes;
            for (ByteBuffer nioBuffer : nioBuffers) {
                ByteBuf buf = Unpooled.wrappedBuffer(nioBuffer);
                while (buf.readableBytes() > 0) {
                    int writePosition = positionDelta + written;
                    int index = indexes[writePosition / blockSize];
                    cacheByteBuffer.position(index * blockSize + writePosition % blockSize);
                    int length = Math.min(buf.readableBytes(), blockSize - writePosition % blockSize);
                    cacheByteBuffer.put(buf.slice(buf.readerIndex(), length).nioBuffer());
                    buf.skipBytes(length);
                    written += length;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Optional<ByteBuf> get(long cacheId, long position, int length) {
        ByteBuf buf = Unpooled.buffer(length);
        readLock.lock();
        try {
            NavigableMap<Long, Blocks> cache = cacheMap.get(cacheId);
            if (cache == null) {
                return Optional.empty();
            }
            Map.Entry<Long, Blocks> entry = cache.floorEntry(position);
            if (entry == null) {
                return Optional.empty();
            }
            long cacheStartPosition = entry.getKey();
            Blocks blocks = entry.getValue();
            if (entry.getKey() + entry.getValue().dataLength < position + length) {
                return Optional.empty();
            }
            lru.touchIfExist(new Key(cacheId, cacheStartPosition));
            MappedByteBuffer cacheByteBuffer = this.cacheByteBuffer.duplicate();
            long nextPosition = position;
            int remaining = length;
            for (int i = 0; i < blocks.indexes.length; i++) {
                long cacheBlockEndPosition = cacheStartPosition + (long) (i + 1) * blockSize;
                if (cacheBlockEndPosition < nextPosition) {
                    continue;
                }
                long cacheBlockStartPosition = cacheBlockEndPosition - blockSize;
                int readSize = (int) Math.min(remaining, cacheBlockEndPosition - nextPosition);
                // Use Java 11 compatible approach instead of Java 13+ slice(int, int) method
                int slicePosition = blocks.indexes[i] * blockSize + (int) (nextPosition - cacheBlockStartPosition);
                MappedByteBuffer slicedBuffer = cacheByteBuffer.duplicate();
                slicedBuffer.position(slicePosition).limit(slicePosition + readSize);
                buf.writeBytes(slicedBuffer.slice());
                remaining -= readSize;
                nextPosition += readSize;
                if (remaining <= 0) {
                    break;
                }
            }
            return Optional.of(buf);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Ensure the capacity of cache
     *
     * @param cacheStartPosition if the eviction entries contain the current cache, then ensure capacity will return null.
     * @param size               size of data
     * @return the indexes of cache blocks
     */
    private int[] ensureCapacity(long cacheStartPosition, int size) {
        if (size > this.maxSize) {
            return null;
        }
        int requiredBlockCount = align(size) / blockSize;
        int[] indexes = new int[requiredBlockCount];
        int acquiringBlockIndex = 0;
        // evict the least recently used cache blocks until the free blocks are enough
        while (freeBlockCount + acquiringBlockIndex < requiredBlockCount) {
            Map.Entry<Key, Blocks> entry = lru.pop();
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

    private int align(int size) {
        return size % blockSize == 0 ? size : size + blockSize - size % blockSize;
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
     * It contains indexes of blocks in {@link #cacheByteBuffer} in the order of the data.
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
        int dataLength;

        public Blocks(int[] indexes, int dataLength) {
            this.indexes = indexes;
            this.dataLength = dataLength;
        }
    }

}
