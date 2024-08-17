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

import com.automq.stream.s3.wal.exception.WALCapacityMismatchException;
import com.automq.stream.s3.wal.exception.WALNotInitializedException;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
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

    long DEFAULT_RETRY_INTERVAL = TimeUnit.MILLISECONDS.toMillis(100);
    long DEFAULT_RETRY_TIMEOUT = TimeUnit.MINUTES.toMillis(1);

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

    default void retryWrite(ByteBuf src, long position) throws IOException {
        retryWrite(src, position, DEFAULT_RETRY_INTERVAL, DEFAULT_RETRY_TIMEOUT);
    }

    /**
     * Retry {@link #write(ByteBuf, long)} with the given interval until success or timeout.
     */
    default void retryWrite(ByteBuf src, long position, long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        retry(() -> write(src, position), retryIntervalMillis, retryTimeoutMillis);
    }

    /**
     * Flush to disk.
     */
    void flush() throws IOException;

    default void retryFlush() throws IOException {
        retryFlush(DEFAULT_RETRY_INTERVAL, DEFAULT_RETRY_TIMEOUT);
    }

    /**
     * Retry {@link #flush()} with the given interval until success or timeout.
     */
    default void retryFlush(long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        retry(this::flush, retryIntervalMillis, retryTimeoutMillis);
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

    default int read(ByteBuf dst, long position) throws IOException {
        return read(dst, position, dst.writableBytes());
    }

    default int retryRead(ByteBuf dst, long position) throws IOException {
        return retryRead(dst, position, DEFAULT_RETRY_INTERVAL, DEFAULT_RETRY_TIMEOUT);
    }

    /**
     * Retry {@link #read(ByteBuf, long)} with the given interval until success or timeout.
     */
    default int retryRead(ByteBuf dst, long position, long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        return retry(() -> read(dst, position), retryIntervalMillis, retryTimeoutMillis);
    }

    default void retry(IORunnable runnable, long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        retry(IOSupplier.from(runnable), retryIntervalMillis, retryTimeoutMillis);
    }

    default <T> T retry(IOSupplier<T> supplier, long retryIntervalMillis, long retryTimeoutMillis) throws IOException {
        long start = System.nanoTime();
        long retryTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(retryTimeoutMillis);
        while (true) {
            try {
                return supplier.get();
            } catch (IOException e) {
                if (System.nanoTime() - start > retryTimeoutNanos) {
                    LOGGER.error("Failed to execute IO operation, retry timeout", e);
                    throw e;
                } else {
                    LOGGER.warn("Failed to execute IO operation, retrying in {}ms, error: {}", retryIntervalMillis, e.getMessage());
                    Threads.sleep(retryIntervalMillis);
                }
            }
        }
    }

    boolean useDirectIO();

    interface CapacityReader {
        /**
         * Get the capacity of the given channel.
         * It returns null if the channel has not been initialized before.
         */
        Long capacity(WALChannel channel) throws IOException;
    }

    interface IOSupplier<T> {
        T get() throws IOException;

        static IOSupplier<Void> from(IORunnable runnable) {
            return () -> {
                runnable.run();
                return null;
            };
        }
    }

    interface IORunnable {
        void run() throws IOException;
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
