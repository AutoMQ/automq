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

package com.automq.stream.s3.cache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

/**
 * Owns a disposable cache file and its single mapping. Addresses are byte offsets within the file,
 * not native memory addresses. Allocation, eviction and synchronization belong to {@link FileCache}.
 * Borrowed read slices are consumed while the cache lock is held; no mapping escapes to cache callers.
 * The file channel is closed after mapping, and the mapping is reclaimed by the JVM when unreachable.
 */
final class MappedFileStorage {
    private final MappedByteBuffer buffer;

    MappedFileStorage(String path, int size) throws IOException {
        if (size > 0) {
            File file = new File(path);
            do {
                Files.deleteIfExists(file.toPath());
            } while (!file.createNewFile());
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(size);
                this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
            }
        } else {
            this.buffer = null;
        }
    }

    void write(long fileOffset, ByteBuffer source) {
        ByteBuffer target = buffer.duplicate();
        target.position(Math.toIntExact(fileOffset));
        target.put(source);
    }

    ByteBuffer read(long fileOffset, int length) {
        return buffer.slice(Math.toIntExact(fileOffset), length);
    }
}
