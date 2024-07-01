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

package com.automq.stream.s3.wal.util;

import com.automq.stream.s3.wal.exception.WALCapacityMismatchException;
import com.automq.stream.s3.wal.exception.WALNotInitializedException;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.Constants.CAPACITY_NOT_SET;
import static com.automq.stream.s3.wal.util.WALUtil.isBlockDevice;

/**
 * There are two implementations of WALChannel:
 * 1. WALFileChannel based on file system, which calls fsync after each write to ensure data is flushed to disk.
 * 2. WALBlockDeviceChannel based on block device, which uses O_DIRECT to bypass page cache.
 */
public interface WALChannel {

    Logger LOGGER = LoggerFactory.getLogger(WALChannel.class);

    long DEFAULT_RETRY_INTERVAL = 100L;

    static WALChannelBuilder builder(String path) {
        return new WALChannelBuilder(path);
    }

    /**
     * Open the channel for read and write.
     * If {@code reader} is null, checks will be skipped.
     *
     * @param reader the reader to get the capacity of the channel
     * @throws WALCapacityMismatchException if the capacity of the channel does not match the expected capacity
     * @throws WALNotInitializedException   if try to open an un-initialized channel in recovery mode
     * @throws IOException                  if any I/O error happens
     */
    void open(CapacityReader reader) throws IOException;

    default void open() throws IOException {
        open(null);
    }

    void close();

    long capacity();

    String path();

    /**
     * Write bytes from the given buffer to the given position of the channel from the current reader index
     * to the end of the buffer. It only returns when all bytes are written successfully.
     * {@link #flush()} should be called after this method to ensure data is flushed to disk.
     * This method will change the reader index of the given buffer to the end of the written bytes.
     * This method will not change the writer index of the given buffer.
     */
    void write(ByteBuf src, long position) throws IOException;

    default void retryWrite(ByteBuf src, long position) {
        retryWrite(src, position, DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Retry {@link #write(ByteBuf, long)} with the given interval until success.
     */
    default void retryWrite(ByteBuf src, long position, long retryIntervalMillis) {
        while (true) {
            try {
                write(src, position);
                break;
            } catch (IOException e) {
                LOGGER.error("Failed to write, retrying in {}ms", retryIntervalMillis, e);
                Threads.sleep(retryIntervalMillis);
            }
        }
    }

    /**
     * Flush to disk.
     */
    void flush() throws IOException;

    default void retryFlush() {
        retryFlush(DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Retry {@link #flush()} with the given interval until success.
     */
    default void retryFlush(long retryIntervalMillis) {
        while (true) {
            try {
                flush();
                break;
            } catch (IOException e) {
                LOGGER.error("Failed to flush, retrying in {}ms", retryIntervalMillis, e);
                Threads.sleep(retryIntervalMillis);
            }
        }
    }

    /**
     * Call {@link #write(ByteBuf, long)} and {@link #flush()}.
     */
    default void writeAndFlush(ByteBuf src, long position) throws IOException {
        write(src, position);
        flush();
    }

    default void retryWriteAndFlush(ByteBuf src, long position) {
        retryWriteAndFlush(src, position, DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Retry {@link #writeAndFlush(ByteBuf, long)} with the given interval until success.
     */
    default void retryWriteAndFlush(ByteBuf src, long position, long retryIntervalMillis) {
        while (true) {
            try {
                writeAndFlush(src, position);
                break;
            } catch (IOException e) {
                LOGGER.error("Failed to write and flush, retrying in {}ms", retryIntervalMillis, e);
                Threads.sleep(retryIntervalMillis);
            }
        }
    }

    /**
     * Read bytes from the given position of the channel to the given buffer from the current writer index
     * until reaching the capacity of the buffer or the end of the channel.
     * This method will change the writer index of the given buffer to the end of the read bytes.
     * This method will not change the reader index of the given buffer.
     */
    default int read(ByteBuf dst, long position) throws IOException {
        return read(dst, position, dst.writableBytes());
    }

    default int retryRead(ByteBuf dst, long position) {
        return retryRead(dst, position, DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Retry {@link #read(ByteBuf, long)} with the given interval until success.
     */
    default int retryRead(ByteBuf dst, long position, long retryIntervalMillis) {
        while (true) {
            try {
                return read(dst, position);
            } catch (IOException e) {
                LOGGER.error("Failed to read, retrying in {}ms", retryIntervalMillis, e);
                Threads.sleep(retryIntervalMillis);
            }
        }
    }

    /**
     * Read bytes from the given position of the channel to the given buffer from the current writer index
     * until reaching the given length or the end of the channel.
     * This method will change the writer index of the given buffer to the end of the read bytes.
     * This method will not change the reader index of the given buffer.
     * If the given length is larger than the writable bytes of the given buffer, only the first
     * {@code dst.writableBytes()} bytes will be read.
     */
    int read(ByteBuf dst, long position, int length) throws IOException;

    default int retryRead(ByteBuf dst, long position, int length) {
        return retryRead(dst, position, length, DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Retry {@link #read(ByteBuf, long, int)} with the given interval until success.
     */
    default int retryRead(ByteBuf dst, long position, int length, long retryIntervalMillis) {
        while (true) {
            try {
                return read(dst, position, length);
            } catch (IOException e) {
                LOGGER.error("Failed to read, retrying in {}ms", retryIntervalMillis, e);
                Threads.sleep(retryIntervalMillis);
            }
        }
    }

    boolean useDirectIO();

    interface CapacityReader {
        /**
         * Get the capacity of the given channel.
         * It returns null if the channel has not been initialized before.
         */
        Long capacity(WALChannel channel);
    }

    class WALChannelBuilder {
        private static final Logger LOGGER = LoggerFactory.getLogger(WALChannelBuilder.class);
        private final String path;
        private Boolean direct;
        private long capacity;
        private int initBufferSize;
        private int maxBufferSize;
        private boolean recoveryMode;

        private WALChannelBuilder(String path) {
            this.path = path;
        }

        public WALChannelBuilder direct(boolean direct) {
            this.direct = direct;
            return this;
        }

        public WALChannelBuilder capacity(long capacity) {
            assert capacity == CAPACITY_NOT_SET || WALUtil.isAligned(capacity);
            this.capacity = capacity;
            return this;
        }

        public WALChannelBuilder initBufferSize(int initBufferSize) {
            this.initBufferSize = initBufferSize;
            return this;
        }

        public WALChannelBuilder maxBufferSize(int maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public WALChannelBuilder recoveryMode(boolean recoveryMode) {
            this.recoveryMode = recoveryMode;
            return this;
        }

        public WALChannel build() {
            String directNotAvailableMsg = WALBlockDeviceChannel.checkAvailable(path);
            boolean isBlockDevice = isBlockDevice(path);
            boolean useDirect = false;
            if (direct != null) {
                // Set by user.
                useDirect = direct;
            } else if (isBlockDevice) {
                // We can only use direct IO for block devices.
                useDirect = true;
            } else if (directNotAvailableMsg == null) {
                // If direct IO is available, we use it by default.
                useDirect = true;
            }

            if (useDirect && directNotAvailableMsg != null) {
                throw new IllegalArgumentException(directNotAvailableMsg);
            }

            if (!isBlockDevice) {
                LOGGER.warn("WAL in a file system, which may cause performance degradation. path: {}", new File(path).getAbsolutePath());
            }

            if (useDirect) {
                return new WALBlockDeviceChannel(path, capacity, initBufferSize, maxBufferSize, recoveryMode);
            } else {
                LOGGER.warn("Direct IO not used for WAL, which may cause performance degradation. path: {}, isBlockDevice: {}, reason: {}",
                    new File(path).getAbsolutePath(), isBlockDevice, directNotAvailableMsg);
                return new WALFileChannel(path, capacity, recoveryMode);
            }
        }
    }
}
