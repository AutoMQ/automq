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
    private final int blockSize;
    private final BitSet freeBlocks;
    private final LRUCache<Key, Value> lru = new LRUCache<>();
    final Map<String, NavigableMap<Long, Value>> path2cache = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    int freeBlockCount;
    private int freeCheckPoint = 0;
    private final MappedByteBuffer cacheByteBuffer;

    public FileCache(String path, int size, int blockSize) throws IOException {
        this.blockSize = blockSize;
        size = align(size);
        int blockCount = size / blockSize;
        this.freeBlocks = new BitSet(blockCount);
        this.freeBlocks.set(0, blockCount, true);
        this.freeBlockCount = blockCount;
        File file = new File(path);
        do {
            Files.deleteIfExists(file.toPath());
        } while (!file.createNewFile());
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(size);
            this.cacheByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
        }
    }

    public FileCache(String path, int size) throws IOException {
        this(path, size, BLOCK_SIZE);
    }

    public void put(String path, long position, ByteBuf data) {
        writeLock.lock();
        try {
            int dataLength = data.readableBytes();
            int[] blocks = ensureCapacity(dataLength);
            if (blocks == null) {
                return;
            }
            Key key = new Key(path, position);
            Value value = new Value(blocks, dataLength);
            lru.put(key, value);
            NavigableMap<Long, Value> cache = path2cache.computeIfAbsent(path, k -> new TreeMap<>());
            cache.put(position, value);

            ByteBuffer cacheByteBuffer = this.cacheByteBuffer.duplicate();
            int written = 0;
            ByteBuffer[] nioBuffers = data.nioBuffers();
            for (ByteBuffer nioBuffer : nioBuffers) {
                ByteBuf buf = Unpooled.wrappedBuffer(nioBuffer);
                while (buf.readableBytes() > 0) {
                    int block = blocks[written / blockSize];
                    cacheByteBuffer.position(block * blockSize + written % blockSize);
                    int length = Math.min(buf.readableBytes(), blockSize - written % blockSize);
                    cacheByteBuffer.put(buf.slice(buf.readerIndex(), length).nioBuffer());
                    buf.skipBytes(length);
                    written += length;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Optional<ByteBuf> get(String filePath, long position, int length) {
        ByteBuf buf = Unpooled.buffer(length);
        readLock.lock();
        try {
            NavigableMap<Long, Value> cache = path2cache.get(filePath);
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
            lru.touch(new Key(filePath, cacheStartPosition));
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

    private int[] ensureCapacity(int size) {
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
            path2cache.get(key.path).remove(key.position);
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
        String path;
        long position;

        public Key(String path, long position) {
            this.path = path;
            this.position = position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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
        int[] blocks;
        int dataLength;

        public Value(int[] blocks, int dataLength) {
            this.blocks = blocks;
            this.dataLength = dataLength;
        }
    }

}
