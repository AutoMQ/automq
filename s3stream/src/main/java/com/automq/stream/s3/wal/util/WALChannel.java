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

package com.automq.stream.s3.wal.util;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * There are two implementations of WALChannel:
 * 1. WALFileChannel based on file system, which calls fsync after each write to ensure data is flushed to disk.
 * 2. WALBlockDeviceChannel based on block device, which uses O_DIRECT to bypass page cache.
 */
public interface WALChannel {
    static WALChannelBuilder builder(String path, long capacity) {
        return new WALChannelBuilder(path, capacity);
    }

    void open() throws IOException;

    void close();

    long capacity();

    /**
     * Write bytes from the given buffer to the given position of the channel from the current reader index
     * to the end of the buffer. It only returns when all bytes are written successfully.
     * This method will change the reader index of the given buffer to the end of the written bytes.
     * This method will not change the writer index of the given buffer.
     */
    void write(ByteBuf src, long position) throws IOException;

    /**
     * Read bytes from the given position of the channel to the given buffer from the current writer index
     * until reaching the capacity of the buffer or the end of the channel.
     * This method will change the writer index of the given buffer to the end of the read bytes.
     * This method will not change the reader index of the given buffer.
     */
    int read(ByteBuf dst, long position) throws IOException;

    class WALChannelBuilder {
        private final String path;
        private final long capacity;

        private WALChannelBuilder(String path, long capacity) {
            this.path = path;
            this.capacity = capacity;
        }

        public WALChannel build() {
            if (path.startsWith("/dev/")) {
                return new WALBlockDeviceChannel(path, capacity);
            } else {
                return new WALFileChannel(path, capacity);
            }
        }
    }
}
