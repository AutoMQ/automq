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

package com.automq.stream.s3.wal.util;

import com.automq.stream.s3.wal.exception.WALCapacityMismatchException;
import com.automq.stream.s3.wal.exception.WALNotInitializedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.Constants.CAPACITY_NOT_SET;
import static com.automq.stream.s3.wal.util.WALUtil.isBlockDevice;

/**
 * There are two implementations of WALChannel:
 * 1. WALFileChannel based on file system, which calls fsync after each write to ensure data is flushed to disk.
 * 2. WALBlockDeviceChannel based on block device, which uses O_DIRECT to bypass page cache.
 */
public interface WALChannel {

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

    /**
     * Mark the channel as failed.
     * Note: Once this method is called, the channel cannot be used anymore.
     */
    void markFailed();

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
    void retryWrite(ByteBuf src, long position, long retryIntervalMillis, long retryTimeoutMillis) throws IOException;

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
    void retryFlush(long retryIntervalMillis, long retryTimeoutMillis) throws IOException;

    default int read(ByteBuf dst, long position) throws IOException {
        return read(dst, position, dst.writableBytes());
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

    default int retryRead(ByteBuf dst, long position) throws IOException {
        return retryRead(dst, position, dst.writableBytes(), DEFAULT_RETRY_INTERVAL, DEFAULT_RETRY_TIMEOUT);
    }

    /**
     * Retry {@link #read(ByteBuf, long, int)} with the given interval until success or timeout.
     */
    int retryRead(ByteBuf dst, long position, int length, long retryIntervalMillis, long retryTimeoutMillis) throws IOException;

    boolean useDirectIO();

    interface CapacityReader {
        /**
         * Get the capacity of the given channel.
         * It returns null if the channel has not been initialized before.
         */
        Long capacity(WALChannel channel) throws IOException;
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
