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

package kafka.log.streamaspect.cache;

import com.automq.stream.s3.cache.LRUCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * File cache which used for cache time index data.
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
    private final BitSet freeBlocks;
    private final LRUCache<Key, Value> lru = new LRUCache<>();
    final Map<Long, NavigableMap<Long, Value>> stream2cache = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    int freeBlockCount;
    private int freeCheckPoint = 0;
    private final MappedByteBuffer cacheByteBuffer;

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

    public void put(long streamId, long position, ByteBuf data) {
        writeLock.lock();
        try {
            int dataLength = data.readableBytes();
            NavigableMap<Long, Value> cache = stream2cache.computeIfAbsent(streamId, k -> new TreeMap<>());
            Map.Entry<Long, Value> pos2value = cache.floorEntry(position);
            long cacheStartPosition;
            long cacheEndPosition;
            Value value;
            if (pos2value == null || pos2value.getKey() + pos2value.getValue().dataLength < position) {
                cacheStartPosition = position;
                cacheEndPosition = position + dataLength;
                value = Value.EMPTY;
            } else {
                cacheStartPosition = pos2value.getKey();
                value = pos2value.getValue();
                cacheEndPosition = Math.max(pos2value.getKey() + value.dataLength, position + dataLength);
            }
            // ensure the capacity, if the capacity change then update the cache index
            int moreCapacity = (int) (cacheEndPosition - (cacheStartPosition + value.blocks.length * (long) blockSize));
            int newDataLength = (int) (cacheEndPosition - cacheStartPosition);
            if (moreCapacity > 0) {
                int[] blocks = ensureCapacity(cacheStartPosition, moreCapacity);
                if (blocks == null) {
                    return;
                }
                int[] newBlocks = new int[value.blocks.length + blocks.length];
                System.arraycopy(value.blocks, 0, newBlocks, 0, value.blocks.length);
                System.arraycopy(blocks, 0, newBlocks, value.blocks.length, blocks.length);
                value = new Value(newBlocks, newDataLength);
            } else {
                value = new Value(value.blocks, newDataLength);
            }
            cache.put(cacheStartPosition, value);
            lru.put(new Key(streamId, cacheStartPosition), value);

            // write data to cache
            ByteBuffer cacheByteBuffer = this.cacheByteBuffer.duplicate();
            int positionDelta = (int) (position - cacheStartPosition);
            int written = 0;
            ByteBuffer[] nioBuffers = data.nioBuffers();
            int[] blocks = value.blocks;
            for (ByteBuffer nioBuffer : nioBuffers) {
                ByteBuf buf = Unpooled.wrappedBuffer(nioBuffer);
                while (buf.readableBytes() > 0) {
                    int writePosition = positionDelta + written;
                    int block = blocks[writePosition / blockSize];
                    cacheByteBuffer.position(block * blockSize + writePosition % blockSize);
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

    public Optional<ByteBuf> get(long streamId, long position, int length) {
        ByteBuf buf = Unpooled.buffer(length);
        readLock.lock();
        try {
            NavigableMap<Long, Value> cache = stream2cache.get(streamId);
            if (cache == null) {
                return Optional.empty();
            }
            Map.Entry<Long, Value> entry = cache.floorEntry(position);
            if (entry == null) {
                return Optional.empty();
            }
            long cacheStartPosition = entry.getKey();
            Value value = entry.getValue();
            if (entry.getKey() + entry.getValue().dataLength < position + length) {
                return Optional.empty();
            }
            lru.touchIfExist(new Key(streamId, cacheStartPosition));
            MappedByteBuffer cacheByteBuffer = this.cacheByteBuffer.duplicate();
            long nextPosition = position;
            int remaining = length;
            for (int i = 0; i < value.blocks.length; i++) {
                long cacheBlockEndPosition = cacheStartPosition + (long) (i + 1) * blockSize;
                if (cacheBlockEndPosition < nextPosition) {
                    continue;
                }
                long cacheBlockStartPosition = cacheBlockEndPosition - blockSize;
                int readSize = (int) Math.min(remaining, cacheBlockEndPosition - nextPosition);
                buf.writeBytes(cacheByteBuffer.slice(value.blocks[i] * blockSize + (int) (nextPosition - cacheBlockStartPosition), readSize));
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
     * @return the cache blocks
     */
    private int[] ensureCapacity(long cacheStartPosition, int size) {
        if (size > this.maxSize) {
            return null;
        }
        int requiredBlockCount = align(size) / blockSize;
        int[] blocks = new int[requiredBlockCount];
        int acquiringBlockIndex = 0;
        while (freeBlockCount + acquiringBlockIndex < requiredBlockCount) {
            Map.Entry<Key, Value> entry = lru.pop();
            if (entry == null) {
                break;
            }
            Key key = entry.getKey();
            Value value = entry.getValue();
            stream2cache.get(key.path).remove(key.position);
            if (key.position == cacheStartPosition) {
                // eviction is conflict to current cache
                for (int i = 0; i < acquiringBlockIndex; i++) {
                    freeBlockCount++;
                    freeBlocks.set(blocks[i], true);
                }
                return null;
            }
            for (int blockIndex : value.blocks) {
                if (acquiringBlockIndex < blocks.length) {
                    blocks[acquiringBlockIndex++] = blockIndex;
                    freeBlocks.set(blockIndex, false);
                } else {
                    freeBlockCount++;
                    freeBlocks.set(blockIndex, true);
                }
            }
        }
        while (acquiringBlockIndex < blocks.length) {
            int next = freeBlocks.nextSetBit(freeCheckPoint);
            if (next >= 0) {
                blocks[acquiringBlockIndex++] = next;
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
        return blocks;
    }

    private int align(int size) {
        return size % blockSize == 0 ? size : size + blockSize - size % blockSize;
    }

    static class Key implements Comparable<Key> {
        Long path;
        long position;

        public Key(Long path, long position) {
            this.path = path;
            this.position = position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Key key = (Key) o;
            return position == key.position && Objects.equals(path, key.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, position);
        }

        @Override
        public int compareTo(Key o) {
            if (this.path.compareTo(o.path) != 0) {
                return this.path.compareTo(o.path);
            }
            return Long.compare(this.position, o.position);
        }
    }

    static class Value {
        static final Value EMPTY = new Value(new int[0], 0);

        int[] blocks;
        int dataLength;

        public Value(int[] blocks, int dataLength) {
            this.blocks = blocks;
            this.dataLength = dataLength;
        }
    }

}
