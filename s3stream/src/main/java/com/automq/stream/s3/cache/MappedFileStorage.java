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
import java.util.Objects;

/**
 * Stores cache bytes in one disposable file using multiple mapped regions.
 * Reads and writes use byte offsets within the file.
 * Concurrent reads require separate destination buffers. The owner must
 * prevent writes from overlapping other reads or writes.
 */
final class MappedFileStorage {
    static final int DEFAULT_MAPPING_REGION_SIZE_BYTES = 1024 * 1024 * 1024;

    private final MappedByteBuffer[] buffers;
    private final long capacityBytes;
    private final int mappingRegionSizeBytes;

    /**
     * Creates mapped storage using regions of at most 1 GiB.
     *
     * @param path backing file path; existing contents are replaced for positive capacity
     * @param capacityBytes total file capacity in bytes, nonnegative; zero creates no file or mappings
     * @throws IOException if deleting, creating, opening, resizing or mapping the backing file fails
     */
    MappedFileStorage(String path, long capacityBytes) throws IOException {
        this(path, capacityBytes, DEFAULT_MAPPING_REGION_SIZE_BYTES);
    }

    /**
     * Creates mapped storage with the specified region size.
     * The file handle is closed after mapping; the mapped regions remain usable
     * and are reclaimed by the JVM when no longer referenced.
     *
     * @param path backing file path; existing contents are replaced for positive capacity
     * @param capacityBytes total file capacity in bytes, nonnegative; zero creates no file or mappings
     * @param mappingRegionSizeBytes maximum bytes per mapped region, positive; the final region may be smaller
     * @throws IOException if deleting, creating, opening, resizing or mapping the backing file fails
     */
    MappedFileStorage(String path, long capacityBytes, int mappingRegionSizeBytes) throws IOException {
        this.capacityBytes = capacityBytes;
        this.mappingRegionSizeBytes = mappingRegionSizeBytes;
        int mappingCount = 0;
        if (capacityBytes > 0) {
            long completeRegionCount = capacityBytes / mappingRegionSizeBytes;
            // Include a final smaller region when bytes remain after the complete regions.
            mappingCount = Math.toIntExact(completeRegionCount + (capacityBytes % mappingRegionSizeBytes == 0 ? 0 : 1));
        }
        this.buffers = new MappedByteBuffer[mappingCount];
        if (capacityBytes > 0) {
            File file = new File(path);
            do {
                Files.deleteIfExists(file.toPath());
            } while (!file.createNewFile());
            try (RandomAccessFile backingFile = new RandomAccessFile(file, "rw")) {
                backingFile.setLength(capacityBytes);
                for (int i = 0; i < mappingCount; i++) {
                    long offset = (long) i * mappingRegionSizeBytes;
                    buffers[i] = backingFile.getChannel().map(FileChannel.MapMode.READ_WRITE, offset,
                        Math.min(mappingRegionSizeBytes, capacityBytes - offset));
                }
            }
        }
    }

    /**
     * Copies all remaining source bytes into the file and advances the source position to its limit.
     * The source limit is unchanged.
     *
     * @param fileOffset starting byte offset within the backing file
     * @param source buffer whose remaining bytes are copied; its position advances
     * @throws IndexOutOfBoundsException before copying if the requested range is outside the file
     */
    void write(long fileOffset, ByteBuffer source) {
        Objects.checkFromIndexSize(fileOffset, source.remaining(), capacityBytes);
        while (source.hasRemaining()) {
            int mappingIndex = Math.toIntExact(fileOffset / mappingRegionSizeBytes);
            int mappingOffset = Math.toIntExact(fileOffset % mappingRegionSizeBytes);
            MappedByteBuffer buffer = buffers[mappingIndex];
            int length = Math.min(source.remaining(), buffer.capacity() - mappingOffset);
            buffer.put(mappingOffset, source, source.position(), length);
            source.position(source.position() + length);
            fileOffset += length;
        }
    }

    /**
     * Fills the remaining target space from the file and advances the target position to its limit.
     * The target limit is unchanged.
     *
     * @param fileOffset starting byte offset within the backing file
     * @param target writable destination buffer whose remaining space is filled; its position advances
     * @throws IndexOutOfBoundsException before copying if the requested range is outside the file
     */
    void read(long fileOffset, ByteBuffer target) {
        Objects.checkFromIndexSize(fileOffset, target.remaining(), capacityBytes);
        while (target.hasRemaining()) {
            int mappingIndex = Math.toIntExact(fileOffset / mappingRegionSizeBytes);
            int mappingOffset = Math.toIntExact(fileOffset % mappingRegionSizeBytes);
            MappedByteBuffer buffer = buffers[mappingIndex];
            int length = Math.min(target.remaining(), buffer.capacity() - mappingOffset);
            target.put(target.position(), buffer, mappingOffset, length);
            target.position(target.position() + length);
            fileOffset += length;
        }
    }
}
