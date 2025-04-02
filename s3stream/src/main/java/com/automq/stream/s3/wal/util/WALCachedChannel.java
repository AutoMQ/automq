/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.util;

import com.automq.stream.s3.ByteBufAlloc;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.Constants.CAPACITY_NOT_SET;

/**
 * A wrapper of {@link WALChannel} that caches for read to reduce I/O.
 */
public class WALCachedChannel implements WALChannel {

    private static final int DEFAULT_CACHE_SIZE = 1 << 20;

    private final WALChannel channel;
    private final int cacheSize;

    private ByteBuf cache;
    private long cachePosition = -1;

    private WALCachedChannel(WALChannel channel, int cacheSize) {
        this.channel = channel;
        this.cacheSize = cacheSize;
    }

    public static WALCachedChannel of(WALChannel channel) {
        return new WALCachedChannel(channel, DEFAULT_CACHE_SIZE);
    }

    public static WALCachedChannel of(WALChannel channel, int cacheSize) {
        return new WALCachedChannel(channel, cacheSize);
    }

    @Override
    public void markFailed() {
        channel.markFailed();
    }

    @Override
    public int read(ByteBuf dst, long position, int length) throws IOException {
        return read(channel::read, dst, position, length);
    }

    @Override
    public int retryRead(ByteBuf dst, long position, int length, long retryIntervalMillis,
        long retryTimeoutMillis) throws IOException {
        Reader reader = (buf, pos, len) -> channel.retryRead(buf, pos, len, retryIntervalMillis, retryTimeoutMillis);
        return read(reader, dst, position, length);
    }

    /**
     * As we use a common cache for all threads, we need to synchronize the read.
     */
    private synchronized int read(Reader reader, ByteBuf dst, long position, int length) throws IOException {
        if (CAPACITY_NOT_SET == channel.capacity()) {
            // If we don't know the capacity now, we can't cache.
            return reader.read(dst, position, length);
        }

        long start = position;
        length = Math.min(length, dst.writableBytes());
        long end = position + length;

        ByteBuf cache = getCache();
        if (length > cache.capacity()) {
            // If the length is larger than the cache capacity, we can't cache.
            return reader.read(dst, position, length);
        }

        boolean fallWithinCache = cachePosition >= 0 && cachePosition <= start && end <= cachePosition + cache.readableBytes();
        if (!fallWithinCache) {
            cache.clear();
            cachePosition = start;
            // Make sure the cache is not larger than the channel capacity.
            int cacheLength = (int) Math.min(cache.writableBytes(), channel.capacity() - cachePosition);
            reader.read(cache, cachePosition, cacheLength);
        }

        // Now the cache is ready.
        int relativePosition = (int) (start - cachePosition);
        dst.writeBytes(cache, relativePosition, length);
        return length;
    }

    @Override
    public void close() {
        releaseCache();
        this.channel.close();
    }

    /**
     * Release the cache if it is not null.
     * This method should be called when no more {@link #read}s will be called to release the allocated memory.
     */
    public synchronized void releaseCache() {
        if (this.cache != null) {
            this.cache.release();
            this.cache = null;
        }
        this.cachePosition = -1;
    }

    /**
     * Get the cache. If the cache is not initialized, initialize it.
     * Should be called under synchronized.
     */
    private ByteBuf getCache() {
        if (this.cache == null) {
            this.cache = ByteBufAlloc.byteBuffer(cacheSize);
        }
        return this.cache;
    }

    private interface Reader {
        int read(ByteBuf dst, long position, int length) throws IOException;
    }

    @Override
    public void open(CapacityReader reader) throws IOException {
        this.channel.open(reader);
    }

    @Override
    public long capacity() {
        return this.channel.capacity();
    }

    @Override
    public String path() {
        return this.channel.path();
    }

    @Override
    public void write(ByteBuf src, long position) throws IOException {
        this.channel.write(src, position);
    }

    @Override
    public void retryWrite(ByteBuf src, long position, long retryIntervalMillis,
        long retryTimeoutMillis) throws IOException {
        channel.retryWrite(src, position, retryIntervalMillis, retryTimeoutMillis);
    }

    @Override
    public void flush() throws IOException {
        this.channel.flush();
    }

    @Override
    public void retryFlush(long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        channel.retryFlush(retryIntervalMillis, retryTimeoutMillis);
    }

    @Override
    public boolean useDirectIO() {
        return channel.useDirectIO();
    }
}
