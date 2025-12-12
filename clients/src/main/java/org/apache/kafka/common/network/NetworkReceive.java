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
package org.apache.kafka.common.network;

import org.apache.kafka.common.memory.MemoryPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public static final String UNKNOWN_SOURCE = "";
    public static final int UNLIMITED = -1;
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // Thread-safe pool for reusing 4-byte size buffers
    private static final Queue<ByteBuffer> SIZE_BUFFER_POOL = new ConcurrentLinkedQueue<>();
    private static final int SIZE_BUFFER_POOL_CAPACITY = 512;

    // Tiered memory pool buckets for common payload sizes
    private static final int[] BUCKET_SIZES = {1024, 4096, 16384, 65536, 262144, 1048576};
    private static final Queue<?>[] BUCKET_POOLS = new Queue[BUCKET_SIZES.length];

    static {
        // Initialize bucket pools
        for (int i = 0; i < BUCKET_SIZES.length; i++) {
            BUCKET_POOLS[i] = new ConcurrentLinkedQueue<>();
        }
    }

    private final String source;
    private ByteBuffer size;
    private final int maxSize;
    private final MemoryPool memoryPool;
    private int requestedBufferSize = -1;
    private ByteBuffer buffer;
    private int bufferBucketIndex = -1;

    public NetworkReceive(String source, ByteBuffer buffer) {
        this(UNLIMITED, source);
        this.buffer = buffer;
    }

    public NetworkReceive(String source) {
        this(UNLIMITED, source);
    }

    public NetworkReceive(int maxSize, String source) {
        this(maxSize, source, MemoryPool.NONE);
    }

    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.size = acquireSizeBuffer();
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    /**
     * Acquires a 4-byte buffer from the pool or creates a new one
     */
    private static ByteBuffer acquireSizeBuffer() {
        ByteBuffer pooled = SIZE_BUFFER_POOL.poll();
        if (pooled != null) {
            pooled.clear();
            return pooled;
        }
        return ByteBuffer.allocate(4);
    }

    /**
     * Returns a 4-byte buffer to the pool
     */
    private static void releaseSizeBuffer(ByteBuffer buffer) {
        if (SIZE_BUFFER_POOL.size() < SIZE_BUFFER_POOL_CAPACITY) {
            buffer.clear();
            SIZE_BUFFER_POOL.offer(buffer);
        }
    }

    /**
     * Finds the appropriate bucket index for a given size
     */
    private static int findBucketIndex(int size) {
        for (int i = 0; i < BUCKET_SIZES.length; i++) {
            if (size <= BUCKET_SIZES[i]) {
                return i;
            }
        }

        // Size exceeds all buckets, use memoryPool directly
        return -1; 
    }

    /**
     * Acquires a buffer from the tiered pool or memoryPool
     */
    @SuppressWarnings("unchecked")
    private ByteBuffer acquirePayloadBuffer(int size) {
        int bucketIndex = findBucketIndex(size);

        if (bucketIndex >= 0) {
            Queue<ByteBuffer> bucketPool = (Queue<ByteBuffer>) BUCKET_POOLS[bucketIndex];
            ByteBuffer pooled = bucketPool.poll();
            if (pooled != null) {
                pooled.clear();
                this.bufferBucketIndex = bucketIndex;
                return pooled;
            }
        }

        // Fall back to memoryPool for large sizes or if bucket pool is empty
        ByteBuffer allocated = memoryPool.tryAllocate(size);
        if (allocated != null) {
            this.bufferBucketIndex = bucketIndex;
        }
        return allocated;
    }

    /**
     * Releases a buffer back to the tiered pool
     */
    @SuppressWarnings("unchecked")
    private void releasePayloadBuffer(ByteBuffer buffer) {
        if (buffer == null || buffer == EMPTY_BUFFER) {
            return;
        }

        if (bufferBucketIndex >= 0 && bufferBucketIndex < BUCKET_POOLS.length) {
            Queue<ByteBuffer> bucketPool = (Queue<ByteBuffer>) BUCKET_POOLS[bufferBucketIndex];
            int maxPoolSize = 128; // Limit pool size to prevent memory buildup
            if (bucketPool.size() < maxPoolSize) {
                buffer.clear();
                bucketPool.offer(buffer);
                return;
            }
        }

        // Fall back to memoryPool release
        memoryPool.release(buffer);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        return !size.hasRemaining() && buffer != null && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        int read = 0;
        if (size.hasRemaining()) {
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            if (!size.hasRemaining()) {
                size.rewind();
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                requestedBufferSize = receiveSize; // may be 0 for some payloads (SASL)
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER;
                }
            }
        }
        if (buffer == null && requestedBufferSize != -1) { // we know the size we want but haven't been able to allocate it yet
            buffer = acquirePayloadBuffer(requestedBufferSize);
            if (buffer == null)
                log.trace("Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize, source);
        }
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    @Override
    public boolean requiredMemoryAmountKnown() {
        return requestedBufferSize != -1;
    }

    @Override
    public boolean memoryAllocated() {
        return buffer != null;
    }

    @Override
    public void close() throws IOException {
        // Release payload buffer to tiered pool
        if (buffer != null) {
            releasePayloadBuffer(buffer);
            buffer = null;
            bufferBucketIndex = -1;
        }

        // Release size buffer to pool
        if (size != null) {
            releaseSizeBuffer(size);
            size = null;
        }
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

    public int bytesRead() {
        if (buffer == null)
            return size.position();
        return buffer.position() + size.position();
    }

    /**
     * Returns the total size of the receive including payload and size buffer
     * for use in metrics. This is consistent with {@link NetworkSend#size()}
     */
    public int size() {
        return payload().limit() + size.limit();
    }
}