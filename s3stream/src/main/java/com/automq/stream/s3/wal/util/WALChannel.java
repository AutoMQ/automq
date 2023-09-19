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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * There are two implementations of WALChannel:
 * 1. WALFileChannel based on file system, which calls fsync after each write to ensure data is flushed to disk.
 * 2. WALBlockDeviceChannel based on block device, which uses O_DIRECT to bypass page cache.
 */
public interface WALChannel {
    void open() throws IOException;

    void close();

    long capacity();

    /**
     * Write the remaining bytes in the given buffer to the given position.
     * It only returns when all bytes are written successfully.
     */
    void write(ByteBuffer src, long position) throws IOException;

    int read(ByteBuffer dst, long position) throws IOException;

    class WALChannelBuilder {
        public static WALChannel build(String path, long maxCapacity) {
            if (path.startsWith("/dev/")) {
                return new WALBlockDeviceChannel(path, maxCapacity);
            } else {
                return new WALFileChannel(path, maxCapacity);
            }
        }
    }
}
